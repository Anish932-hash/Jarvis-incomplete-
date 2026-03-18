from __future__ import annotations

import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict

import pytest

from backend.python.desktop_api import JarvisAPIHandler, JarvisHTTPServer
from tests.helpers.http_client import request_json


class FakeDesktopService:
    def __init__(self) -> None:
        self.goals: Dict[str, Dict[str, Any]] = {}
        self.approvals: Dict[str, Dict[str, Any]] = {}
        self.schedules: Dict[str, Dict[str, Any]] = {}
        self.triggers: Dict[str, Dict[str, Any]] = {}
        self.macros: Dict[str, Dict[str, Any]] = {}
        self.oauth_tokens: Dict[str, Dict[str, Any]] = {}
        self.oauth_flows: Dict[str, Dict[str, Any]] = {}
        self.browser_sessions: Dict[str, Dict[str, Any]] = {}
        self.missions: Dict[str, Dict[str, Any]] = {}
        self.rollbacks: Dict[str, Dict[str, Any]] = {}
        self.goal_to_mission: Dict[str, str] = {}
        self.provider_update_calls: list[Dict[str, Any]] = []
        self.provider_verify_calls: list[Dict[str, Any]] = []
        self.provider_recovery_calls: list[Dict[str, Any]] = []
        self.model_setup_scope_calls: list[Dict[str, Any]] = []
        self.oauth_maintenance: Dict[str, Any] = {
            "status": "idle",
            "last_run_at": "",
            "candidate_count": 0,
            "refreshed_count": 0,
            "error_count": 0,
        }
        self.recovery_default_profile = "balanced"
        self.desktop_governance_state: Dict[str, Any] = {
            "status": "success",
            "policy_profile": "balanced",
            "allow_high_risk": True,
            "allow_critical_risk": False,
            "allow_admin_clearance": False,
            "allow_destructive": False,
            "allow_desktop_approval_reuse": True,
            "allow_action_confirmation_reuse": True,
            "desktop_approval_reuse_window_s": 90,
            "action_confirmation_reuse_window_s": 45,
            "updated_at": "2026-03-15T10:00:00+00:00",
            "source": "defaults",
            "profiles": {},
        }
        self.recovery_profiles = [
            {
                "name": "safe",
                "retry_adjust": -1,
                "base_delay_factor": 1.25,
                "max_delay_factor": 1.35,
                "multiplier_factor": 1.05,
                "jitter_factor": 0.75,
                "retry_unknown_failures": False,
            },
            {
                "name": "balanced",
                "retry_adjust": 0,
                "base_delay_factor": 1.0,
                "max_delay_factor": 1.0,
                "multiplier_factor": 1.0,
                "jitter_factor": 1.0,
                "retry_unknown_failures": True,
            },
            {
                "name": "aggressive",
                "retry_adjust": 2,
                "base_delay_factor": 0.75,
                "max_delay_factor": 1.25,
                "multiplier_factor": 0.92,
                "jitter_factor": 1.2,
                "retry_unknown_failures": True,
            },
        ]
        self.voice_state: Dict[str, Any] = {
            "available": True,
            "running": False,
            "session_started_at": "",
            "last_trigger_type": "",
            "wakeword_status": "gated:local_launch_template_blacklisted",
            "wakeword_supervision_status": "hybrid_polling",
            "wakeword_supervision_reason": "mission_reliability_hybrid_polling",
            "wakeword_supervision_last_changed_at": "2026-03-08T10:00:00+00:00",
            "wakeword_supervision_restart_delay_s": 12.0,
            "wakeword_supervision_restart_not_before": "2026-03-08T10:03:30+00:00",
            "wakeword_supervision_pause_count": 2,
            "wakeword_supervision_resume_count": 1,
            "route_policy_status": "recovery",
            "route_policy_reason": "Wakeword route cooling down after local launcher instability.",
            "route_policy_last_changed_at": "2026-03-08T10:00:00+00:00",
            "route_policy_next_retry_at": "2026-03-08T10:03:00+00:00",
            "route_policy_block_count": 2,
            "route_policy_reroute_count": 4,
            "route_policy_recovery_count": 3,
        }
        self.voice_route_policy_state: Dict[str, Any] = {
            "status": "success",
            "generated_at": time.time(),
            "stt": {
                "task": "stt",
                "status": "rerouted",
                "selected_provider": "local",
                "recommended_provider": "groq",
                "route_adjusted": True,
                "route_blocked": False,
                "local_route_viable": False,
                "blacklisted": True,
                "recovery_pending": True,
                "cooldown_hint_s": 90,
                "next_retry_at": "2026-03-08T10:03:00+00:00",
                "reason_code": "local_launch_template_blacklisted",
                "reason": "Local STT launcher temporarily blacklisted, using Groq fallback.",
            },
            "wakeword": {
                "task": "wakeword",
                "status": "recovery",
                "selected_provider": "local",
                "recommended_provider": "local",
                "route_adjusted": False,
                "route_blocked": False,
                "local_route_viable": False,
                "blacklisted": True,
                "recovery_pending": True,
                "cooldown_hint_s": 180,
                "next_retry_at": "2026-03-08T10:03:00+00:00",
                "reason_code": "local_launch_template_blacklisted",
                "reason": "Wakeword path held in recovery until local launcher stabilizes.",
            },
            "tts": {
                "task": "tts",
                "status": "stable",
                "selected_provider": "elevenlabs",
                "recommended_provider": "elevenlabs",
                "route_adjusted": False,
                "route_blocked": False,
                "local_route_viable": True,
                "blacklisted": False,
                "recovery_pending": False,
                "cooldown_hint_s": 0,
                "next_retry_at": "",
                "reason_code": "",
                "reason": "",
            },
            "summary": {
                "status": "recovery",
                "reason_code": "local_launch_template_blacklisted",
                "reason": "Wakeword route cooling down after local launcher instability.",
                "next_retry_at": "2026-03-08T10:03:00+00:00",
            },
        }
        self.voice_route_policy_timeline_state: Dict[str, Any] = {
            "status": "success",
            "available": True,
            "count": 3,
            "limit": 48,
            "items": [
                {
                    "event_id": "voice-route-1",
                    "occurred_at": "2026-03-08T09:58:00+00:00",
                    "source": "provider",
                    "task": "stt",
                    "status": "rerouted",
                    "previous_status": "stable",
                    "selected_provider": "local",
                    "recommended_provider": "groq",
                    "route_adjusted": True,
                    "route_blocked": False,
                    "recovery_pending": True,
                    "cooldown_hint_s": 90,
                    "next_retry_at": "2026-03-08T10:03:00+00:00",
                    "reason_code": "local_launch_template_blacklisted",
                    "reason": "Local STT launcher temporarily blacklisted, using Groq fallback.",
                },
                {
                    "event_id": "voice-route-2",
                    "occurred_at": "2026-03-08T09:59:30+00:00",
                    "source": "provider",
                    "task": "wakeword",
                    "status": "recovery",
                    "previous_status": "stable",
                    "selected_provider": "local",
                    "recommended_provider": "local",
                    "route_adjusted": False,
                    "route_blocked": False,
                    "recovery_pending": True,
                    "cooldown_hint_s": 180,
                    "next_retry_at": "2026-03-08T10:03:00+00:00",
                    "reason_code": "local_launch_template_blacklisted",
                    "reason": "Wakeword path held in recovery until local launcher stabilizes.",
                },
                {
                    "event_id": "voice-route-3",
                    "occurred_at": "2026-03-08T10:00:40+00:00",
                    "source": "provider",
                    "task": "tts",
                    "status": "stable",
                    "previous_status": "stable",
                    "selected_provider": "elevenlabs",
                    "recommended_provider": "elevenlabs",
                    "route_adjusted": False,
                    "route_blocked": False,
                    "recovery_pending": False,
                    "cooldown_hint_s": 0,
                    "next_retry_at": "",
                    "reason_code": "",
                    "reason": "",
                },
            ],
            "current": {},
            "diagnostics": {
                "status_counts": {"rerouted": 1, "recovery": 1, "stable": 1},
                "task_counts": {"stt": 1, "wakeword": 1, "tts": 1},
                "next_retry_at": "2026-03-08T10:03:00+00:00",
            },
        }
        self.tts_state: Dict[str, Any] = {
            "local": {
                "status": "success",
                "provider": "local",
                "active_provider": "",
                "active_session_id": "",
                "providers": {
                    "neural_runtime": {
                        "ready": True,
                        "configured": True,
                        "enabled": True,
                        "backend": "llama_cpp",
                        "execution_backend": "openai_http",
                        "bridge_ready": True,
                    },
                    "pyttsx3": {"ready": True, "retry_after_s": 0.0, "attempts": 3, "successes": 3, "failures": 0, "failure_ema": 0.02},
                    "win32_sapi": {"ready": True, "retry_after_s": 0.0, "attempts": 0, "successes": 0, "failures": 0, "failure_ema": 0.0},
                },
                "neural_runtime": {
                    "configured": True,
                    "enabled": True,
                    "ready": True,
                    "backend": "llama_cpp",
                    "execution_backend": "openai_http",
                    "bridge_ready": True,
                },
                "history_tail": [],
            },
            "elevenlabs": {
                "status": "success",
                "provider": "elevenlabs",
                "ready": True,
                "retry_after_s": 0.0,
                "failure_ema": 0.04,
                "configured": True,
                "has_api_key": True,
                "has_voice_id": True,
                "history_tail": [],
            },
            "recommended_provider": "elevenlabs",
        }
        self.tts_bridge_state: Dict[str, Any] = {
            "status": "success",
            "enabled": True,
            "configured": True,
            "managed": True,
            "autostart": True,
            "endpoint": "http://127.0.0.1:5055/v1/audio/speech",
            "endpoint_configured": True,
            "healthcheck_url": "http://127.0.0.1:5055/health",
            "probe_candidates": [
                "http://127.0.0.1:5055/health",
                "http://127.0.0.1:5055",
                "http://127.0.0.1:5055/v1/audio/speech",
            ],
            "running": True,
            "pid": 44210,
            "ready": True,
            "message": "bridge healthy",
            "last_error": "",
            "last_probe_at": "",
            "last_probe_url": "http://127.0.0.1:5055/health",
            "last_start_at": "",
            "last_stop_at": "",
            "last_exit_code": None,
            "last_pid": 44210,
            "start_attempts": 1,
            "probe_attempts": 2,
            "restart_count": 0,
            "cooldown_until": 0.0,
            "cooldown_remaining_s": 0.0,
            "active_profile_id": "",
            "active_template_id": "",
            "runtime_overrides": {},
        }
        self.local_reasoning_bridge_state: Dict[str, Any] = {
            "status": "success",
            "enabled": True,
            "configured": True,
            "managed": True,
            "autostart": True,
            "endpoint": "http://127.0.0.1:8080",
            "request_url": "http://127.0.0.1:8080/v1/chat/completions",
            "endpoint_configured": True,
            "healthcheck_url": "http://127.0.0.1:8080/health",
            "probe_candidates": [
                "http://127.0.0.1:8080/health",
                "http://127.0.0.1:8080",
                "http://127.0.0.1:8080/v1/chat/completions",
            ],
            "api_mode": "openai_chat",
            "model_hint": "local-auto-reasoning-qwen3-14b",
            "active_profile_id": "",
            "active_template_id": "",
            "runtime_overrides": {},
            "running": True,
            "pid": 45110,
            "ready": True,
            "message": "reasoning bridge healthy",
            "last_error": "",
            "last_probe_at": "",
            "last_probe_url": "http://127.0.0.1:8080/health",
            "last_start_at": "",
            "last_stop_at": "",
            "last_exit_code": None,
            "last_pid": 45110,
            "start_attempts": 1,
            "probe_attempts": 1,
            "inference_attempts": 2,
            "restart_count": 0,
            "cooldown_until": 0.0,
            "cooldown_remaining_s": 0.0,
            "last_request_url": "http://127.0.0.1:8080/v1/chat/completions",
            "last_inference_at": "",
            "last_inference_ok": True,
            "last_inference_model": "local-auto-reasoning-qwen3-14b",
            "last_inference_latency_s": 0.41,
        }
        self.tts_policy_state: Dict[str, Any] = {
            "enabled": True,
            "learning_enabled": True,
            "alpha": 0.24,
            "failure_weight": 2.4,
            "latency_weight": 0.6,
            "route_bias": {"local": 0.08, "elevenlabs": 0.22},
            "providers": {
                "local": {"ready": True, "retry_after_s": 0.0, "failure_ema": 0.06, "latency_ema_s": 0.13},
                "elevenlabs": {"ready": True, "retry_after_s": 0.0, "failure_ema": 0.08, "latency_ema_s": 0.46},
            },
            "recommended_provider": "elevenlabs",
            "recommended_chain": ["elevenlabs", "local"],
            "decision_history": [],
            "history_tail": [],
        }
        self.stt_policy_state: Dict[str, Any] = {
            "provider_failure_streak_threshold": 3,
            "provider_cooldown_s": 10.0,
            "provider_max_cooldown_s": 180.0,
            "policy_failure_streak_threshold": 3,
            "policy_base_cooldown_s": 10.0,
            "policy_max_cooldown_s": 240.0,
            "provider_state_enabled": True,
            "provider_state_persist_interval_s": 5.0,
            "provider_health": "healthy",
            "providers": {
                "local": {
                    "enabled": True,
                    "attempts": 12,
                    "success": 11,
                    "error": 1,
                    "failure_streak": 0,
                    "health": "healthy",
                    "latency_ema_ms": 220.0,
                    "cooldown_remaining_s": 0.0,
                },
                "groq": {
                    "enabled": True,
                    "attempts": 3,
                    "success": 2,
                    "error": 1,
                    "failure_streak": 0,
                    "health": "healthy",
                    "latency_ema_ms": 410.0,
                    "cooldown_remaining_s": 0.0,
                },
            },
            "provider_policies": {},
            "provider_order_ema": {"local": 0.78, "groq": 0.22},
            "fallback_rate_ema": 0.12,
            "success_rate": 0.86,
            "health": "healthy",
            "attempt_chain_history": [],
            "autotune": {
                "enabled": True,
                "alpha": 0.22,
                "min_samples": 8,
                "bad_threshold": 0.58,
                "good_threshold": 0.32,
                "apply_cooldown_s": 8.0,
                "scope_count": 2,
                "recent_count": 6,
                "last_save_error": "",
            },
        }
        self.stt_runtime_profile_state: Dict[str, Any] = {
            "status": "success",
            "task": "stt",
            "profile_id": "",
            "template_id": "",
            "model": "whisper-large-v3",
            "local_model_path": "E:/J.A.R.V.I.S/stt/whisper-large-v3(Speech-To-text_model)",
            "voice_running": False,
            "available": True,
            "updated_at": 0.0,
        }
        self.launch_template_history: list[Dict[str, Any]] = []
        self.launch_template_forced_status_by_id: Dict[str, str] = {}
        self.voice_continuous_runs: Dict[str, Dict[str, Any]] = {}
        self.context_state: Dict[str, Any] = {
            "status": "success",
            "available": True,
            "running": False,
            "monitoring_interval_s": 3.0,
            "pattern_detection_enabled": True,
            "proactive_suggestions_enabled": True,
            "pattern_count": 1,
            "opportunity_count": 1,
        }
        self.context_policy: Dict[str, Any] = {
            "autorun": False,
            "min_priority": 7,
            "min_confidence": 0.75,
            "cooldown_s": 60.0,
            "max_workers": 2,
            "priority_weight": 1.0,
            "confidence_weight": 2.0,
            "retry_penalty": 0.75,
            "aging_window_s": 30.0,
            "fairness_window_s": 60.0,
            "per_type_max_in_window": 4,
            "per_type_max_concurrency": 1,
            "class_weights": {"recovery": 1.15, "automation": 1.0, "external": 0.95, "insight": 0.9, "other": 0.85},
            "class_limits_in_window": {"recovery": 6, "automation": 4, "external": 3, "insight": 3, "other": 2},
            "class_max_concurrency": {"recovery": 2, "automation": 1, "external": 1, "insight": 1, "other": 1},
            "starvation_override_s": 45.0,
            "multiobjective_enabled": True,
            "deadline_weight": 2.6,
            "utility_weight": 1.8,
            "risk_weight": 1.4,
            "duration_weight": 0.65,
            "self_tune_enabled": True,
            "self_tune_alpha": 0.28,
            "self_tune_min_samples": 5,
            "self_tune_bad_threshold": 0.48,
            "self_tune_good_threshold": 0.8,
            "preflight_external_contract_enabled": True,
            "preflight_external_max_checks": 3,
            "external_pressure_enabled": True,
            "external_refresh_s": 15.0,
            "external_penalty_weight": 2.8,
            "external_recovery_boost": 1.2,
            "external_limit_floor_scale": 0.35,
            "external_concurrency_floor_scale": 0.4,
            "external_autotune_enabled": True,
            "external_autotune_alpha": 0.24,
            "external_autotune_bad_threshold": 0.58,
            "external_autotune_good_threshold": 0.36,
        }
        self.context_opportunity_records: list[Dict[str, Any]] = [
            {
                "opportunity_id": "opp-1",
                "opportunity_type": "automation",
                "description": "Repeatable workflow detected",
                "priority": 7,
                "confidence": 0.88,
                "context": {"active_application": "notepad", "active_window_title": "Fake Window"},
                "received_at": datetime.now(timezone.utc).isoformat(),
            }
        ]
        self.context_opportunity_runs: list[Dict[str, Any]] = []
        self.state_history: list[Dict[str, Any]] = [
            {
                "state_id": "1",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "action": "active_window",
                "state_hash": "hash_1",
                "previous_hash": "",
                "changed_paths": ["window.title"],
                "normalized": {"window": {"title": "Notepad"}},
            }
        ]
        self.desktop_anchor_items: list[Dict[str, Any]] = [
            {
                "key": "computer_click_target|submit||||",
                "action": "computer_click_target",
                "query": "submit",
                "app": "",
                "window_title": "",
                "target_mode": "accessibility",
                "element_id": "btn_submit",
                "samples": 4,
                "successes": 3,
                "failures": 1,
                "success_rate": 0.75,
                "consecutive_failures": 0,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        ]
        self.desktop_anchor_quarantine: list[Dict[str, Any]] = [
            {
                "key": "computer_click_target|submit||||",
                "action": "computer_click_target",
                "query": "submit",
                "reason": "guardrail_context_shift",
                "severity": "hard",
                "signals": ["guardrail_context_shift"],
                "hits": 1,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "expires_at": datetime.now(timezone.utc).isoformat(),
            }
        ]
        self.desktop_workflow_items: list[Dict[str, Any]] = [
            {
                "key": "command|microsoft-visual-studio-code|vscode|preferences open settings json",
                "action": "command",
                "profile_id": "microsoft-visual-studio-code",
                "app_name": "vscode",
                "window_title": "main.py - Visual Studio Code",
                "intent": "preferences: open settings (json)",
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "variants": {
                    '{"focus_first":true,"keys":["f1"]}': {
                        "strategy_id": "workflow_retry_2",
                        "samples": 3,
                        "verified_successes": 2,
                    }
                },
            },
            {
                "key": "terminal_command|powershell|powershell|npm test",
                "action": "terminal_command",
                "profile_id": "powershell",
                "app_name": "powershell",
                "window_title": "PowerShell",
                "intent": "npm test",
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "variants": {
                    "primary": {
                        "strategy_id": "primary",
                        "samples": 2,
                        "verified_successes": 2,
                    }
                },
            },
        ]
        self.desktop_mission_items: list[Dict[str, Any]] = [
            {
                "mission_id": "dm_pause_wizard_1",
                "status": "paused",
                "mission_kind": "wizard",
                "resume_action": "complete_wizard_flow",
                "resume_signature": "wizard-resume-1",
                "surface_signature": "surface-uac-1",
                "app_name": "installer",
                "anchor_window_title": "Setup Wizard",
                "blocking_window_title": "User Account Control",
                "stop_reason_code": "elevation_consent_required",
                "stop_reason": "Administrator approval is still required before the installer can continue.",
                "approval_kind": "elevation_consent",
                "dialog_kind": "elevation_prompt",
                "risk_level": "high",
                "page_count": 2,
                "pages_completed": 1,
                "requested_target_count": 0,
                "resolved_target_count": 0,
                "remaining_target_count": 0,
                "pause_count": 1,
                "resume_attempts": 0,
                "latest_result_status": "partial",
                "latest_result_message": "Installer paused on administrator approval.",
                "warnings": ["Administrator approval is still required before the installer can continue."],
                "recommended_actions": ["resume_mission"],
                "resume_contract": {
                    "mission_id": "dm_pause_wizard_1",
                    "mission_kind": "wizard",
                    "resume_action": "complete_wizard_flow",
                    "resume_strategy": "reacquire_app_surface",
                    "resume_signature": "wizard-resume-1",
                    "resume_payload": {
                        "action": "complete_wizard_flow",
                        "app_name": "installer",
                        "mission_id": "dm_pause_wizard_1",
                        "mission_kind": "wizard",
                    },
                    "resume_preconditions": ["approve_elevation_request"],
                },
                "blocking_surface": {
                    "mission_id": "dm_pause_wizard_1",
                    "mission_kind": "wizard",
                    "resume_action": "complete_wizard_flow",
                    "approval_kind": "elevation_consent",
                    "dialog_kind": "elevation_prompt",
                    "window_title": "User Account Control",
                    "recommended_actions": ["resume_mission"],
                },
                "final_page": {"screen_hash": "wizard_uac_dialog"},
                "page_history_tail": [{"page_index": 1, "status": "blocked"}],
                "created_at": "2026-03-14T08:00:00+00:00",
                "updated_at": "2026-03-14T08:10:00+00:00",
                "last_resume_at": "",
                "completed_at": "",
            },
            {
                "mission_id": "dm_completed_form_1",
                "status": "completed",
                "mission_kind": "form",
                "resume_action": "complete_form_flow",
                "resume_signature": "form-resume-1",
                "surface_signature": "surface-settings-1",
                "app_name": "settings",
                "anchor_window_title": "Settings",
                "blocking_window_title": "",
                "stop_reason_code": "",
                "stop_reason": "",
                "approval_kind": "",
                "dialog_kind": "",
                "risk_level": "medium",
                "page_count": 2,
                "pages_completed": 2,
                "requested_target_count": 2,
                "resolved_target_count": 2,
                "remaining_target_count": 0,
                "pause_count": 1,
                "resume_attempts": 1,
                "latest_result_status": "success",
                "latest_result_message": "Settings flow completed.",
                "warnings": [],
                "recommended_actions": [],
                "resume_contract": {},
                "blocking_surface": {},
                "final_page": {"screen_hash": "settings_done"},
                "page_history_tail": [{"page_index": 2, "status": "completed"}],
                "created_at": "2026-03-14T07:50:00+00:00",
                "updated_at": "2026-03-14T08:05:00+00:00",
                "last_resume_at": "2026-03-14T08:04:00+00:00",
                "completed_at": "2026-03-14T08:05:00+00:00",
            },
        ]
        self.desktop_recovery_supervisor_state: Dict[str, Any] = {
            "enabled": False,
            "active": True,
            "inflight": False,
            "interval_s": 45.0,
            "limit": 12,
            "max_auto_resumes": 2,
            "policy_profile": "balanced",
            "allow_high_risk": True,
            "allow_critical_risk": False,
            "allow_admin_clearance": False,
            "allow_destructive": False,
            "mission_status": "paused",
            "mission_kind": "",
            "app_name": "",
            "stop_reason_code": "",
            "resume_force": False,
            "last_tick_at": "",
            "last_success_at": "",
            "last_error_at": "",
            "last_duration_ms": 0.0,
            "last_result_status": "idle",
            "last_result_message": "",
            "last_trigger_source": "",
            "last_trigger_at": "",
            "last_config_source": "",
            "next_due_at": "",
            "run_count": 0,
            "manual_trigger_count": 0,
            "auto_trigger_count": 0,
            "consecutive_error_count": 0,
            "last_summary": {},
            "updated_at": "",
        }
        self.desktop_recovery_watchdog_runs: list[Dict[str, Any]] = []
        self.desktop_workflow_catalog_items: list[Dict[str, Any]] = [
            {
                "action": "focus_address_bar",
                "title": "Focus Address Bar",
                "route_mode": "workflow_focus_address_bar",
                "category_hints": ["browser", "file_manager"],
                "requires_input": False,
                "supported": True,
                "primary_hotkey": ["ctrl", "l"],
                "verify_hint": "address",
            },
            {
                "action": "open_bookmarks",
                "title": "Open Bookmarks",
                "route_mode": "workflow_bookmarks",
                "category_hints": ["browser"],
                "requires_input": False,
                "supported": True,
                "primary_hotkey": ["ctrl", "shift", "o"],
                "verify_hint": "bookmarks",
            },
            {
                "action": "open_history",
                "title": "Open History",
                "route_mode": "workflow_history",
                "category_hints": ["browser"],
                "requires_input": False,
                "supported": True,
                "primary_hotkey": ["ctrl", "h"],
                "verify_hint": "history",
            },
            {
                "action": "open_downloads",
                "title": "Open Downloads",
                "route_mode": "workflow_downloads",
                "category_hints": ["browser"],
                "requires_input": False,
                "supported": True,
                "primary_hotkey": ["ctrl", "j"],
                "verify_hint": "downloads",
            },
            {
                "action": "toggle_terminal",
                "title": "Toggle Terminal",
                "route_mode": "workflow_toggle_terminal",
                "category_hints": ["code_editor", "ide"],
                "requires_input": False,
                "supported": True,
                "primary_hotkey": ["ctrl", "`"],
                "verify_hint": "terminal",
            },
            {
                "action": "format_document",
                "title": "Format Document",
                "route_mode": "workflow_format_document",
                "category_hints": ["code_editor", "ide"],
                "requires_input": False,
                "supported": True,
                "primary_hotkey": ["shift", "alt", "f"],
                "verify_hint": "",
            },
            {
                "action": "new_folder",
                "title": "New Folder",
                "route_mode": "workflow_new_folder",
                "category_hints": ["file_manager"],
                "requires_input": False,
                "supported": True,
                "primary_hotkey": ["ctrl", "shift", "n"],
                "verify_hint": "new folder",
            },
        ]
        self.desktop_evaluation_items: list[Dict[str, Any]] = [
            {
                "name": "settings_multi_control_apply",
                "user_text": "Open settings and turn on bluetooth and set brightness slider to 80 and apply settings",
                "expected_actions": ["open_app", "desktop_interact", "desktop_interact", "desktop_interact"],
                "required_actions": ["desktop_interact"],
                "weight": 1.6,
                "strict_order": False,
                "category": "settings",
                "capabilities": ["settings_control", "form_mission", "switch_control", "value_control"],
                "risk_level": "guarded",
                "notes": "Covers multi-control settings flow completion.",
                "pack": "settings_and_admin",
                "platform": "windows",
                "mission_family": "form",
                "autonomy_tier": "autonomous",
                "apps": ["settings"],
                "recovery_expected": True,
                "native_hybrid_focus": True,
                "replayable": True,
                "horizon_steps": 3,
                "tags": ["settings", "multi_control", "apply"],
            },
            {
                "name": "unsupported_child_dialog_chain",
                "user_text": "Explore surface for add bluetooth device in settings and continue through the child dialog chain",
                "expected_actions": ["desktop_interact"],
                "required_actions": ["desktop_interact"],
                "weight": 1.4,
                "strict_order": False,
                "category": "unsupported_app",
                "capabilities": ["surface_exploration", "child_window_adoption", "recovery"],
                "risk_level": "guarded",
                "notes": "Measures unsupported-app child dialog recovery.",
                "pack": "unsupported_and_recovery",
                "platform": "windows",
                "mission_family": "exploration",
                "autonomy_tier": "autonomous",
                "apps": ["settings"],
                "recovery_expected": True,
                "native_hybrid_focus": True,
                "replayable": True,
                "horizon_steps": 5,
                "tags": ["exploration", "child_window", "dialog_chain"],
            },
            {
                "name": "installer_resume_after_prompt",
                "user_text": "Resume the blocked installer after approval is completed",
                "expected_actions": ["desktop_interact"],
                "required_actions": ["desktop_interact"],
                "weight": 1.8,
                "strict_order": False,
                "category": "installer",
                "capabilities": ["wizard_mission", "desktop_recovery", "governance"],
                "risk_level": "high",
                "notes": "Covers approval-gated installer recovery.",
                "pack": "installer_and_governance",
                "platform": "windows",
                "mission_family": "recovery",
                "autonomy_tier": "autonomous",
                "apps": ["installer"],
                "recovery_expected": True,
                "native_hybrid_focus": True,
                "replayable": True,
                "horizon_steps": 5,
                "tags": ["installer", "resume", "approval"],
            },
            {
                "name": "vscode_long_horizon_debug_loop",
                "user_text": "Open vscode, run npm test in the terminal, inspect failures, and reopen the failing file with quick open",
                "expected_actions": ["desktop_interact"],
                "required_actions": ["desktop_interact"],
                "weight": 1.7,
                "strict_order": False,
                "category": "editor_workflow",
                "capabilities": ["editor", "terminal", "quick_open", "desktop_workflow", "command_execution"],
                "risk_level": "standard",
                "notes": "Covers long-horizon IDE replay loops.",
                "pack": "long_horizon_and_replay",
                "platform": "windows",
                "mission_family": "workflow",
                "autonomy_tier": "autonomous",
                "apps": ["vscode"],
                "recovery_expected": False,
                "native_hybrid_focus": True,
                "replayable": True,
                "horizon_steps": 6,
                "tags": ["editor", "long_horizon", "replayable"],
            },
        ]
        self.desktop_evaluation_last_run: Dict[str, Any] = {
            "status": "success",
            "executed_at": "2026-03-17T09:30:00+00:00",
            "scenario_count": 2,
            "summary": {
                "weighted_pass_rate": 0.92,
                "weighted_score": 0.9,
            },
            "regression": {
                "status": "stable",
                "weighted_pass_rate_delta": 0.0,
                "weighted_score_delta": 0.0,
                "scenario_regressions": [],
                "pack_regressions": [],
                "category_regressions": [],
                "capability_regressions": [],
            },
        }
        self.desktop_evaluation_history_items: list[Dict[str, Any]] = [dict(self.desktop_evaluation_last_run)]
        self.desktop_evaluation_lab_sessions_items: list[Dict[str, Any]] = []
        self.model_connector_policy: Dict[str, float] = {
            "readiness_weight": 1.8,
            "reliability_weight": 2.2,
            "quality_weight": 1.7,
            "latency_weight": 1.1,
            "privacy_weight": 1.2,
        }
        self.reasoning_runtime_state: Dict[str, Any] = {
            "status": "success",
            "enabled": True,
            "timeout_s": 90.0,
            "max_new_tokens": 768,
            "prompt_max_chars": 12000,
            "candidate_count": 2,
            "runtime_ready": True,
            "loaded_count": 1,
            "error_count": 0,
            "items": [
                {
                    "name": "local-auto-reasoning-qwen3-14b",
                    "path": "E:/J.A.R.V.I.S/all_rounder/Qwen3-14B-GGUF/qwen3-14b-q8_0.gguf",
                    "backend": "llama_cpp",
                    "runtime_transport": "bridge",
                    "runtime_bridge_required": True,
                    "runtime_bridge_ready": True,
                    "runtime_supported": True,
                    "runtime_reason": "available",
                    "runtime_loaded": True,
                    "runtime_last_error": "",
                    "runtime_last_loaded_at": 1741305600.0,
                    "runtime_load_latency_s": 1.84,
                },
                {
                    "name": "local-auto-reasoning-llama-3.1-8b",
                    "path": "E:/J.A.R.V.I.S/reasoning/Meta-Llama-3.1-8B-Instruct-Q8_0.gguf",
                    "backend": "llama_cpp",
                    "runtime_transport": "bridge",
                    "runtime_bridge_required": True,
                    "runtime_bridge_ready": True,
                    "runtime_supported": True,
                    "runtime_reason": "available",
                    "runtime_loaded": False,
                    "runtime_last_error": "",
                    "runtime_last_loaded_at": 0.0,
                    "runtime_load_latency_s": 0.0,
                },
            ],
        }
        self.voice_route_policy_history_state: Dict[str, Any] = {
            "status": "success",
            "count": 3,
            "total": 3,
            "limit": 120,
            "items": list(self.voice_route_policy_timeline_state.get("items", [])),
            "history_path": "data/runtime/voice_route_policy_history.jsonl",
            "diagnostics": {
                "status_counts": {"rerouted": 1, "recovery": 1, "stable": 1},
                "task_counts": {"stt": 1, "wakeword": 1, "tts": 1},
                "blocked_events": 0,
                "rerouted_events": 1,
                "recovery_pending_events": 2,
                "blacklisted_events": 2,
                "recovered_events": 1,
                "avg_cooldown_hint_s": 90.0,
                "latest_event_at": "2026-03-08T10:00:40+00:00",
                "latest_next_retry_at": "2026-03-08T10:03:00+00:00",
                "latest_blocked_at": "",
                "latest_recovery_at": "2026-03-08T10:00:40+00:00",
                "timeline_buckets": [
                    {
                        "bucket_start": "2026-03-08T09:00:00+00:00",
                        "count": 2,
                        "blocked_count": 0,
                        "rerouted_count": 1,
                        "recovery_pending_count": 2,
                    },
                    {
                        "bucket_start": "2026-03-08T10:00:00+00:00",
                        "count": 1,
                        "blocked_count": 0,
                        "rerouted_count": 0,
                        "recovery_pending_count": 0,
                    },
                ],
            },
        }
        self.wakeword_supervision_history_state: Dict[str, Any] = {
            "status": "success",
            "count": 3,
            "total": 3,
            "limit": 120,
            "items": [
                {
                    "event_id": "wakeword-supervision-1",
                    "occurred_at": "2026-03-08T09:58:30+00:00",
                    "mission_id": "mission-voice-1",
                    "risk_level": "medium",
                    "policy_profile": "balanced",
                    "status": "recovery",
                    "previous_status": "active",
                    "reason_code": "local_launch_template_blacklisted",
                    "reason": "Wakeword route cooling down after launcher instability.",
                    "strategy": "hybrid_polling",
                    "allow_wakeword": False,
                    "restart_delay_s": 16.0,
                    "next_retry_at": "2026-03-08T10:03:00+00:00",
                    "fallback_interval_s": 2.5,
                    "resume_stability_s": 1.1,
                    "local_voice_pressure_score": 0.72,
                    "mission_sessions": 4,
                    "wakeword_gate_events": 3,
                    "route_policy_pause_count": 3,
                    "route_policy_resume_count": 2,
                    "recovered": False,
                },
                {
                    "event_id": "wakeword-supervision-2",
                    "occurred_at": "2026-03-08T09:59:40+00:00",
                    "mission_id": "mission-voice-1",
                    "risk_level": "medium",
                    "policy_profile": "balanced",
                    "status": "hybrid_polling",
                    "previous_status": "recovery",
                    "reason_code": "mission_reliability_hybrid_polling",
                    "reason": "Mission recovery history prefers hybrid polling.",
                    "strategy": "hybrid_polling",
                    "allow_wakeword": False,
                    "restart_delay_s": 8.0,
                    "next_retry_at": "2026-03-08T10:03:30+00:00",
                    "fallback_interval_s": 1.8,
                    "resume_stability_s": 0.8,
                    "local_voice_pressure_score": 0.68,
                    "mission_sessions": 4,
                    "wakeword_gate_events": 3,
                    "route_policy_pause_count": 3,
                    "route_policy_resume_count": 2,
                    "recovered": False,
                },
                {
                    "event_id": "wakeword-supervision-3",
                    "occurred_at": "2026-03-08T10:02:20+00:00",
                    "mission_id": "mission-voice-1",
                    "risk_level": "medium",
                    "policy_profile": "balanced",
                    "status": "active",
                    "previous_status": "hybrid_polling",
                    "reason_code": "",
                    "reason": "",
                    "strategy": "wakeword",
                    "allow_wakeword": True,
                    "restart_delay_s": 0.0,
                    "next_retry_at": "",
                    "fallback_interval_s": 0.0,
                    "resume_stability_s": 0.8,
                    "local_voice_pressure_score": 0.34,
                    "mission_sessions": 4,
                    "wakeword_gate_events": 3,
                    "route_policy_pause_count": 3,
                    "route_policy_resume_count": 3,
                    "recovered": True,
                },
            ],
            "history_path": "data/runtime/wakeword_supervision_history.jsonl",
            "current": {
                "status": "active",
                "strategy": "wakeword",
                "allow_wakeword": True,
                "restart_delay_s": 0.0,
                "next_retry_at": "",
                "reason_code": "",
                "reason": "",
                "resume_stability_s": 0.8,
                "mission_sessions": 4,
                "wakeword_gate_events": 3,
                "local_voice_pressure_score": 0.34,
            },
            "diagnostics": {
                "status_counts": {"recovery": 1, "hybrid_polling": 1, "active": 1},
                "strategy_counts": {"hybrid_polling": 2, "wakeword": 1},
                "recovered_events": 1,
                "deferred_events": 2,
                "latest_event_at": "2026-03-08T10:02:20+00:00",
                "latest_next_retry_at": "2026-03-08T10:03:30+00:00",
                "latest_active_at": "2026-03-08T10:02:20+00:00",
                "latest_pause_at": "2026-03-08T09:59:40+00:00",
                "avg_restart_delay_s": 12.0,
                "timeline_buckets": [
                    {
                        "bucket_start": "2026-03-08T09:00:00+00:00",
                        "count": 2,
                        "active_count": 0,
                        "paused_count": 2,
                        "recovered_count": 0,
                    },
                    {
                        "bucket_start": "2026-03-08T10:00:00+00:00",
                        "count": 1,
                        "active_count": 1,
                        "paused_count": 0,
                        "recovered_count": 1,
                    },
                ],
            },
        }
        self.wakeword_restart_history_state: Dict[str, Any] = {
            "status": "success",
            "count": 5,
            "total": 5,
            "limit": 120,
            "items": [
                {
                    "event_id": "wakeword-restart-1",
                    "occurred_at": "2026-03-08T10:00:15+00:00",
                    "event_type": "start_failed",
                    "status": "degraded:wakeword bootstrap failed",
                    "reason_code": "wakeword_start_failed",
                    "reason": "wakeword bootstrap failed",
                    "restart_delay_s": 6.0,
                    "next_retry_at": "2026-03-08T10:00:21+00:00",
                    "exhausted_until": "",
                    "failure_count": 1,
                    "recovered": False,
                    "exhausted": False,
                    "policy": {
                        "recent_failures": 1,
                        "recent_successes": 0,
                        "long_failures": 2,
                        "long_successes": 0,
                        "long_exhaustions": 0,
                        "long_recoveries": 0,
                        "long_recovery_ratio": 0.0,
                        "consecutive_failures": 1,
                        "threshold_bias": -1,
                        "max_failures_before_polling": 3,
                        "cooldown_scale": 1.8,
                        "recommended_fallback_interval_s": 2.6,
                        "recommended_resume_stability_s": 1.2,
                        "recovery_expiry_s": 8.0,
                        "exhausted": False,
                    },
                },
                {
                    "event_id": "wakeword-restart-2",
                    "occurred_at": "2026-03-08T10:00:15+00:00",
                    "event_type": "restart_backoff",
                    "status": "degraded:wakeword bootstrap failed",
                    "reason_code": "wakeword_start_failed",
                    "reason": "wakeword bootstrap failed",
                    "restart_delay_s": 6.0,
                    "next_retry_at": "2026-03-08T10:00:21+00:00",
                    "exhausted_until": "",
                    "failure_count": 1,
                    "recovered": False,
                    "exhausted": False,
                    "policy": {
                        "recent_failures": 1,
                        "recent_successes": 0,
                        "long_failures": 2,
                        "long_successes": 0,
                        "long_exhaustions": 0,
                        "long_recoveries": 0,
                        "long_recovery_ratio": 0.0,
                        "consecutive_failures": 1,
                        "threshold_bias": -1,
                        "max_failures_before_polling": 3,
                        "cooldown_scale": 1.8,
                        "recommended_fallback_interval_s": 2.6,
                        "recommended_resume_stability_s": 1.2,
                        "recovery_expiry_s": 8.0,
                        "exhausted": False,
                    },
                },
                {
                    "event_id": "wakeword-restart-3",
                    "occurred_at": "2026-03-08T10:00:21+00:00",
                    "event_type": "restart_exhausted",
                    "status": "degraded:wakeword bootstrap failed",
                    "reason_code": "wakeword_start_failed",
                    "reason": "Wakeword restart failures crossed the adaptive exhaustion threshold.",
                    "restart_delay_s": 6.0,
                    "next_retry_at": "2026-03-08T10:00:29+00:00",
                    "exhausted_until": "2026-03-08T10:00:29+00:00",
                    "failure_count": 3,
                    "recovered": False,
                    "exhausted": True,
                    "policy": {
                        "recent_failures": 3,
                        "recent_successes": 0,
                        "long_failures": 5,
                        "long_successes": 0,
                        "long_exhaustions": 1,
                        "long_recoveries": 0,
                        "long_recovery_ratio": 0.0,
                        "consecutive_failures": 3,
                        "threshold_bias": -1,
                        "max_failures_before_polling": 3,
                        "cooldown_scale": 2.4,
                        "recommended_fallback_interval_s": 3.1,
                        "recommended_resume_stability_s": 1.9,
                        "recovery_expiry_s": 8.0,
                        "exhausted": True,
                    },
                },
                {
                    "event_id": "wakeword-restart-4",
                    "occurred_at": "2026-03-08T10:00:30+00:00",
                    "event_type": "restart_exhaustion_expired",
                    "status": "recovery:mission_recovery_policy",
                    "reason_code": "wakeword_start_failed",
                    "reason": "Wakeword restart exhaustion recovery window elapsed.",
                    "restart_delay_s": 0.0,
                    "next_retry_at": "2026-03-08T10:00:29+00:00",
                    "exhausted_until": "2026-03-08T10:00:29+00:00",
                    "failure_count": 0,
                    "recovered": True,
                    "exhausted": False,
                    "policy": {
                        "recent_failures": 3,
                        "recent_successes": 1,
                        "long_failures": 5,
                        "long_successes": 1,
                        "long_exhaustions": 1,
                        "long_recoveries": 1,
                        "long_recovery_ratio": 0.166667,
                        "consecutive_failures": 0,
                        "threshold_bias": -1,
                        "max_failures_before_polling": 3,
                        "cooldown_scale": 2.0,
                        "recommended_fallback_interval_s": 2.8,
                        "recommended_resume_stability_s": 1.7,
                        "recovery_expiry_s": 8.0,
                        "exhausted": False,
                    },
                },
                {
                    "event_id": "wakeword-restart-5",
                    "occurred_at": "2026-03-08T10:00:31+00:00",
                    "event_type": "started",
                    "status": "active",
                    "reason_code": "",
                    "reason": "",
                    "restart_delay_s": 0.0,
                    "next_retry_at": "",
                    "exhausted_until": "",
                    "failure_count": 0,
                    "recovered": True,
                    "exhausted": False,
                    "policy": {
                        "recent_failures": 3,
                        "recent_successes": 2,
                        "long_failures": 5,
                        "long_successes": 2,
                        "long_exhaustions": 1,
                        "long_recoveries": 2,
                        "long_recovery_ratio": 0.285714,
                        "consecutive_failures": 0,
                        "threshold_bias": 0,
                        "max_failures_before_polling": 4,
                        "cooldown_scale": 1.2,
                        "recommended_fallback_interval_s": 2.0,
                        "recommended_resume_stability_s": 1.2,
                        "recovery_expiry_s": 6.0,
                        "exhausted": False,
                    },
                },
            ],
            "history_path": "data/runtime/wakeword_restart_history.jsonl",
            "current": {
                "recent_failures": 3,
                "recent_successes": 2,
                "long_failures": 5,
                "long_successes": 2,
                "long_exhaustions": 1,
                "long_recoveries": 2,
                "long_recovery_ratio": 0.285714,
                "consecutive_failures": 0,
                "threshold_bias": 0,
                "max_failures_before_polling": 4,
                "cooldown_scale": 1.2,
                "recommended_fallback_interval_s": 2.0,
                "recommended_resume_stability_s": 1.2,
                "recovery_expiry_s": 6.0,
                "exhausted": False,
                "next_retry_at": "",
                "exhausted_until": "",
                "last_exhausted_at": "2026-03-08T10:00:21+00:00",
                "last_exhaustion_expired_at": "2026-03-08T10:00:29+00:00",
                "recovery_expiry_count": 1,
            },
            "diagnostics": {
                "event_counts": {
                    "start_failed": 1,
                    "restart_backoff": 1,
                    "restart_exhausted": 1,
                    "restart_exhaustion_expired": 1,
                    "started": 1,
                },
                "exhausted_events": 1,
                "recovered_events": 2,
                "recovery_expiry_events": 1,
                "exhaustion_transition_count": 1,
                "latest_event_at": "2026-03-08T10:00:31+00:00",
                "latest_next_retry_at": "2026-03-08T10:00:29+00:00",
                "latest_exhausted_at": "2026-03-08T10:00:21+00:00",
                "latest_exhausted_until": "2026-03-08T10:00:29+00:00",
                "latest_recovery_expiry_at": "2026-03-08T10:00:30+00:00",
                "avg_restart_delay_s": 6.0,
                "timeline_buckets": [
                    {
                        "bucket_start": "2026-03-08T10:00:00+00:00",
                        "count": 5,
                        "failure_count": 2,
                        "recovered_count": 2,
                        "exhausted_count": 1,
                        "expiry_count": 1,
                        "exhaustion_transition_count": 1,
                    }
                ],
            },
        }
        self.wakeword_restart_policy_history_state: Dict[str, Any] = {
            "status": "success",
            "count": 5,
            "total": 5,
            "limit": 120,
            "items": [
                {
                    "event_id": "wakeword-restart-1",
                    "source_event_id": "wakeword-restart-1",
                    "recorded_at": "2026-03-08T10:00:15+00:00",
                    "occurred_at": "2026-03-08T10:00:15+00:00",
                    "event_type": "start_failed",
                    "status": "degraded:wakeword bootstrap failed",
                    "threshold_bias": -1,
                    "max_failures_before_polling": 3,
                    "cooldown_scale": 1.8,
                    "recovery_credit": 0.0,
                    "recent_recovery_ratio": 0.0,
                    "cooldown_recovery_factor": 1.0,
                    "effective_long_exhaustions": 0.0,
                    "recommended_delay_decay_factor": 1.0,
                    "recommended_backoff_relaxation": 0,
                    "recommended_exhaustion_relaxation": 0,
                    "recovery_expiry_s": 8.0,
                    "fallback_interval_s": 2.6,
                    "resume_stability_s": 1.2,
                    "wakeword_sensitivity": 0.58,
                    "polling_bias": 0.32,
                    "restart_delay_s": 6.0,
                    "next_retry_at": "2026-03-08T10:00:21+00:00",
                    "exhausted_until": "",
                    "exhausted": False,
                    "recovered": False,
                },
                {
                    "event_id": "wakeword-restart-2",
                    "source_event_id": "wakeword-restart-2",
                    "recorded_at": "2026-03-08T10:00:15+00:00",
                    "occurred_at": "2026-03-08T10:00:15+00:00",
                    "event_type": "restart_backoff",
                    "status": "degraded:wakeword bootstrap failed",
                    "threshold_bias": -1,
                    "max_failures_before_polling": 3,
                    "cooldown_scale": 1.8,
                    "recovery_credit": 0.0,
                    "recent_recovery_ratio": 0.0,
                    "cooldown_recovery_factor": 1.0,
                    "effective_long_exhaustions": 0.0,
                    "recommended_delay_decay_factor": 1.0,
                    "recommended_backoff_relaxation": 0,
                    "recommended_exhaustion_relaxation": 0,
                    "recovery_expiry_s": 8.0,
                    "fallback_interval_s": 2.6,
                    "resume_stability_s": 1.2,
                    "wakeword_sensitivity": 0.58,
                    "polling_bias": 0.32,
                    "restart_delay_s": 6.0,
                    "next_retry_at": "2026-03-08T10:00:21+00:00",
                    "exhausted_until": "",
                    "exhausted": False,
                    "recovered": False,
                },
                {
                    "event_id": "wakeword-restart-3",
                    "source_event_id": "wakeword-restart-3",
                    "recorded_at": "2026-03-08T10:00:21+00:00",
                    "occurred_at": "2026-03-08T10:00:21+00:00",
                    "event_type": "restart_exhausted",
                    "status": "degraded:wakeword bootstrap failed",
                    "threshold_bias": -1,
                    "max_failures_before_polling": 3,
                    "cooldown_scale": 2.4,
                    "recovery_credit": 0.0,
                    "recent_recovery_ratio": 0.0,
                    "cooldown_recovery_factor": 1.0,
                    "effective_long_exhaustions": 1.0,
                    "recommended_delay_decay_factor": 1.0,
                    "recommended_backoff_relaxation": 0,
                    "recommended_exhaustion_relaxation": 0,
                    "recovery_expiry_s": 8.0,
                    "fallback_interval_s": 3.1,
                    "resume_stability_s": 1.9,
                    "wakeword_sensitivity": 0.54,
                    "polling_bias": 0.38,
                    "restart_delay_s": 6.0,
                    "next_retry_at": "2026-03-08T10:00:29+00:00",
                    "exhausted_until": "2026-03-08T10:00:29+00:00",
                    "exhausted": True,
                    "recovered": False,
                },
                {
                    "event_id": "wakeword-restart-4",
                    "source_event_id": "wakeword-restart-4",
                    "recorded_at": "2026-03-08T10:00:30+00:00",
                    "occurred_at": "2026-03-08T10:00:30+00:00",
                    "event_type": "restart_exhaustion_expired",
                    "status": "recovery:mission_recovery_policy",
                    "threshold_bias": -1,
                    "max_failures_before_polling": 3,
                    "cooldown_scale": 2.0,
                    "recovery_credit": 0.32,
                    "recent_recovery_ratio": 0.25,
                    "cooldown_recovery_factor": 0.84,
                    "effective_long_exhaustions": 0.75,
                    "recommended_delay_decay_factor": 0.86,
                    "recommended_backoff_relaxation": 1,
                    "recommended_exhaustion_relaxation": 0,
                    "recovery_expiry_s": 8.0,
                    "fallback_interval_s": 2.8,
                    "resume_stability_s": 1.7,
                    "wakeword_sensitivity": 0.58,
                    "polling_bias": 0.29,
                    "restart_delay_s": 0.0,
                    "next_retry_at": "2026-03-08T10:00:29+00:00",
                    "exhausted_until": "2026-03-08T10:00:29+00:00",
                    "exhausted": False,
                    "recovered": True,
                },
                {
                    "event_id": "wakeword-restart-5",
                    "source_event_id": "wakeword-restart-5",
                    "recorded_at": "2026-03-08T10:00:31+00:00",
                    "occurred_at": "2026-03-08T10:00:31+00:00",
                    "event_type": "started",
                    "status": "active",
                    "threshold_bias": 0,
                    "max_failures_before_polling": 4,
                    "cooldown_scale": 1.2,
                    "recovery_credit": 0.68,
                    "recent_recovery_ratio": 0.5,
                    "cooldown_recovery_factor": 0.52,
                    "effective_long_exhaustions": 0.0,
                    "recommended_delay_decay_factor": 0.62,
                    "recommended_backoff_relaxation": 1,
                    "recommended_exhaustion_relaxation": 1,
                    "recovery_expiry_s": 6.0,
                    "fallback_interval_s": 2.0,
                    "resume_stability_s": 1.2,
                    "wakeword_sensitivity": 0.62,
                    "polling_bias": 0.2,
                    "restart_delay_s": 0.0,
                    "next_retry_at": "",
                    "exhausted_until": "",
                    "exhausted": False,
                    "recovered": True,
                },
            ],
            "history_path": "data/runtime/wakeword_restart_policy_history.jsonl",
            "current": {
                "event_id": "wakeword-restart-5",
                "source_event_id": "wakeword-restart-5",
                "recorded_at": "2026-03-08T10:00:31+00:00",
                "occurred_at": "2026-03-08T10:00:31+00:00",
                "event_type": "started",
                "status": "active",
                "threshold_bias": 0,
                "max_failures_before_polling": 4,
                "cooldown_scale": 1.2,
                "recovery_credit": 0.68,
                "recent_recovery_ratio": 0.5,
                "cooldown_recovery_factor": 0.52,
                "effective_long_exhaustions": 0.0,
                "recommended_delay_decay_factor": 0.62,
                "recommended_backoff_relaxation": 1,
                "recommended_exhaustion_relaxation": 1,
                "recovery_expiry_s": 6.0,
                "fallback_interval_s": 2.0,
                "resume_stability_s": 1.2,
                "wakeword_sensitivity": 0.62,
                "polling_bias": 0.2,
                "restart_delay_s": 0.0,
                "next_retry_at": "",
                "exhausted_until": "",
                "exhausted": False,
                "recovered": True,
                "drift_score": 0.18,
                "recommended_profile": "recovered_wakeword",
                "profile_action": "recover",
                "profile_reason": "Sustained recovery allows wakeword policy pressure to relax.",
                "profile_shift_timeline": [
                    {
                        "bucket_start": "2026-03-08T10",
                        "from_profile": "hybrid_guarded",
                        "to_profile": "recovered_wakeword",
                        "profile_action": "recover",
                        "drift_score": 0.18,
                    }
                ],
                "runtime_posture": {
                    "runtime_mode": "recovered_wakeword",
                    "wakeword_supervision_mode": "recovered_wakeword",
                    "continuous_resume_mode": "resume_ready",
                    "barge_in_enabled": True,
                    "hard_barge_in": True,
                },
            },
            "diagnostics": {
                "avg_threshold_bias": -0.6,
                "avg_cooldown_scale": 1.84,
                "avg_recovery_credit": 0.2,
                "latest_recorded_at": "2026-03-08T10:00:31+00:00",
                "exhausted_count": 1,
                "recovered_count": 2,
                "recent_exhaustion_rate": 0.2,
                "recent_recovery_rate": 0.4,
                "drift_score": 0.18,
                "recommended_profile": "recovered_wakeword",
                "profile_action": "recover",
                "profile_reason": "Sustained recovery allows wakeword policy pressure to relax.",
                "profile_shift_timeline": [
                    {
                        "bucket_start": "2026-03-08T10",
                        "from_profile": "hybrid_guarded",
                        "to_profile": "recovered_wakeword",
                        "profile_action": "recover",
                        "drift_score": 0.18,
                    }
                ],
                "runtime_posture": {
                    "runtime_mode": "recovered_wakeword",
                    "wakeword_supervision_mode": "recovered_wakeword",
                    "continuous_resume_mode": "resume_ready",
                    "barge_in_enabled": True,
                    "hard_barge_in": True,
                },
                "timeline_buckets": [
                    {
                        "bucket_start": "2026-03-08T10",
                        "count": 5,
                        "avg_threshold_bias": -0.6,
                        "avg_cooldown_scale": 1.84,
                        "avg_recovery_credit": 0.2,
                        "avg_fallback_interval_s": 2.62,
                        "avg_resume_stability_s": 1.44,
                        "avg_wakeword_sensitivity": 0.58,
                        "min_threshold_bias": -1,
                        "exhausted_count": 1,
                        "recovered_count": 2,
                    }
                ],
            },
        }
        self.voice_mission_reliability_state: Dict[str, Any] = {
            "status": "success",
            "mission_id": "mission-voice-1",
            "count": 1,
            "total": 1,
            "current": {
                "mission_id": "mission-voice-1",
                "updated_at": "2026-03-08T10:05:00+00:00",
                "sessions": 4,
                "successful_sessions": 3,
                "route_policy_failures": 1,
                "route_policy_pause_count": 3,
                "route_policy_resume_count": 2,
                "route_policy_pause_total_s": 11.4,
                "wakeword_gate_events": 3,
                "stt_block_events": 1,
                "last_profile_hint": "balanced",
                "last_policy_profile": "automation_safe",
                "last_risk_level": "medium",
                "recent_runs": [
                    {
                        "recorded_at": "2026-03-08T10:05:00+00:00",
                        "end_reason": "max_turns",
                        "captured_turns": 2,
                        "route_policy_pause_count": 1,
                        "route_policy_resume_count": 1,
                        "route_policy_pause_total_s": 3.0,
                    }
                ],
            },
            "items": [],
        }
        self.runtime_health_history_state: list[Dict[str, Any]] = [
            {
                "snapshot_id": "health-1",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "status": "success",
                "score": 93.2,
                "blocker_count": 0,
                "warning_count": 0,
                "stack_name": "desktop_agent",
                "preferred_model_name": "",
                "mission_profile": "balanced",
                "subsystems": {
                    "routing": {"status": "ready", "ready": True, "score": 100.0, "message": ""},
                    "reasoning": {"status": "ready", "ready": True, "score": 96.0, "message": ""},
                    "reasoning_bridge": {"status": "ready", "ready": True, "score": 100.0, "message": "reasoning bridge healthy"},
                },
                "alerts": [],
                "recommendations": ["Keep the managed reasoning bridge warm for local planner traffic."],
            }
        ]
        self.vision_runtime_state: Dict[str, Any] = {
            "status": "success",
            "device": "cpu",
            "models_dir": "E:/J.A.R.V.I.S/JARVIS_BACKEND/models/vision",
            "cache_embeddings": True,
            "embedding_cache_size": 0,
            "loaded_count": 1,
            "error_count": 0,
            "items": [
                {
                    "model": "yolo",
                    "artifact_path": "E:/J.A.R.V.I.S/JARVIS_BACKEND/models/vision/yolov10n.pt",
                    "artifact_exists": True,
                    "loaded": True,
                    "attempts": 1,
                    "successes": 1,
                    "failures": 0,
                    "last_error": "",
                    "last_loaded_at": 1741305600.0,
                    "load_latency_s": 0.94,
                },
                {
                    "model": "sam",
                    "artifact_path": "E:/J.A.R.V.I.S/JARVIS_BACKEND/models/vision/sam_vit_b_01ec64.pth",
                    "artifact_exists": False,
                    "loaded": False,
                    "attempts": 0,
                    "successes": 0,
                    "failures": 0,
                    "last_error": "",
                    "last_loaded_at": 0.0,
                    "load_latency_s": 0.0,
                },
                {
                    "model": "clip",
                    "artifact_path": "E:/J.A.R.V.I.S/JARVIS_BACKEND/models/vision/clip",
                    "artifact_exists": True,
                    "loaded": False,
                    "attempts": 0,
                    "successes": 0,
                    "failures": 0,
                    "last_error": "",
                    "last_loaded_at": 0.0,
                    "load_latency_s": 0.0,
                },
                {
                    "model": "blip",
                    "artifact_path": "E:/J.A.R.V.I.S/JARVIS_BACKEND/models/vision/blip2",
                    "artifact_exists": False,
                    "loaded": False,
                    "attempts": 0,
                    "successes": 0,
                    "failures": 0,
                    "last_error": "",
                    "last_loaded_at": 0.0,
                    "load_latency_s": 0.0,
                },
            ],
        }
        self.vision_runtime_profile_state: Dict[str, Any] = {
            "status": "success",
            "task": "vision",
            "profile_id": "",
            "template_id": "",
            "models": [],
            "loaded_count": int(self.vision_runtime_state.get("loaded_count", 0) or 0),
            "available": True,
            "updated_at": 0.0,
        }
        self.connector_simulation_history: list[Dict[str, Any]] = []
        self.connector_remediation_policy_state: Dict[str, Any] = {
            "version": "1.0",
            "updated_at": "",
            "profiles": {},
            "history": [],
            "autotune": {
                "runs": 0,
                "applied": 0,
                "last_status": "",
                "last_reason": "",
                "last_run_at": "",
                "last_apply_at": "",
                "last_scope_key": "",
                "last_run_monotonic": 0.0,
                "last_apply_by_scope": {},
                "history": [],
            },
        }
        self.connector_execution_contract_state: Dict[str, Any] = {
            "version": "1.0",
            "updated_at": "",
            "contracts": {},
            "history": [],
        }
        self.external_mission_policy_state: Dict[str, Any] = {
            "bias": 0.22,
            "pressure_ema": 0.61,
            "risk_ema": 0.58,
            "quality_ema": 0.46,
            "failed_ratio_ema": 0.34,
            "blocked_ratio_ema": 0.18,
            "mode": "worsening",
            "profile": "defensive",
            "profile_confidence": 0.73,
            "profile_pressure_ema": 0.64,
            "profile_stability_ema": 0.38,
            "profile_switch_count": 3,
            "profile_last_switch_at": "2026-03-03T00:03:00+00:00",
            "profile_last_reason": "mission_worsening_pressure",
            "profile_history": [
                {
                    "at": "2026-03-03T00:00:00+00:00",
                    "profile": "cautious",
                    "mode": "stable",
                    "volatility_index": 0.34,
                    "target_pressure": 0.44,
                    "recommendation": "stability",
                    "reason": "elevated_risk_or_stability_recommendation",
                },
                {
                    "at": "2026-03-03T00:03:00+00:00",
                    "profile": "defensive",
                    "mode": "worsening",
                    "volatility_index": 0.57,
                    "target_pressure": 0.63,
                    "recommendation": "stability",
                    "reason": "mission_worsening_pressure",
                },
            ],
            "capability_bias": {
                "email": {
                    "bias": 0.28,
                    "pressure_ema": 0.64,
                    "samples": 12,
                    "weight": 0.82,
                    "top_action": "external_email_send",
                    "updated_at": "2026-03-03T00:03:00+00:00",
                },
                "document": {
                    "bias": 0.11,
                    "pressure_ema": 0.34,
                    "samples": 6,
                    "weight": 0.46,
                    "top_action": "external_doc_update",
                    "updated_at": "2026-03-03T00:03:00+00:00",
                },
            },
            "updated_at": "2026-03-03T00:03:00+00:00",
            "last_reason": "seeded",
        }
        self.external_mission_policy_config: Dict[str, Any] = {
            "mission_outage_autotune_enabled": True,
            "mission_outage_profile_autotune_enabled": True,
            "mission_provider_policy_autotune_enabled": True,
            "mission_outage_bias_gain": 0.48,
            "mission_outage_bias_decay": 0.8,
            "mission_outage_quality_relief": 0.18,
            "mission_outage_profile_decay": 0.78,
            "mission_outage_profile_stability_decay": 0.82,
            "mission_outage_profile_hysteresis": 0.09,
            "mission_outage_capability_bias_gain": 0.24,
            "mission_outage_capability_bias_decay": 0.84,
            "mission_outage_capability_limit": 12,
            "provider_policy_max_providers": 80,
            "outage_trip_threshold": 0.62,
            "outage_recover_threshold": 0.36,
            "outage_route_hard_block_threshold": 0.86,
            "outage_preflight_block_threshold": 0.92,
        }
        self.external_provider_bias_rows: list[Dict[str, Any]] = [
            {
                "provider": "google",
                "health_score": 0.41,
                "failure_ema": 0.62,
                "outage_ema": 0.69,
                "outage_policy_bias": 0.31,
                "cooldown_bias": 1.42,
                "mission_pressure": 0.72,
                "trip_threshold": 0.46,
                "recover_threshold": 0.24,
                "route_block_threshold": 0.54,
                "preflight_block_threshold": 0.5,
                "mission_profile_alignment": -0.34,
                "mission_profile_samples": 9,
                "cooldown_active": True,
                "outage_active": True,
                "top_operation_bias": [{"operation": "write", "bias": 1.38, "deviation": 0.38}],
                "top_action_risks": [{"action": "external_email_send", "failure_ema": 0.72}],
                "top_operation_risks": [{"operation": "write", "failure_ema": 0.68}],
                "updated_at": "2026-03-03T00:03:00+00:00",
            },
            {
                "provider": "graph",
                "health_score": 0.83,
                "failure_ema": 0.14,
                "outage_ema": 0.09,
                "outage_policy_bias": 0.08,
                "cooldown_bias": 1.08,
                "mission_pressure": 0.28,
                "trip_threshold": 0.52,
                "recover_threshold": 0.3,
                "route_block_threshold": 0.61,
                "preflight_block_threshold": 0.58,
                "mission_profile_alignment": 0.24,
                "mission_profile_samples": 11,
                "cooldown_active": False,
                "outage_active": False,
                "top_operation_bias": [{"operation": "read", "bias": 1.06, "deviation": 0.06}],
                "top_action_risks": [{"action": "external_calendar_list_events", "failure_ema": 0.18}],
                "top_operation_risks": [{"operation": "read", "failure_ema": 0.16}],
                "updated_at": "2026-03-03T00:03:00+00:00",
            },
        ]
        self._goal_counter = 0
        self._approval_counter = 0
        self._schedule_counter = 0
        self._trigger_counter = 0
        self._macro_counter = 0
        self._mission_counter = 0
        self._browser_session_counter = 0
        self._rollback_counter = 0
        self._oauth_flow_counter = 0
        self._connector_simulation_counter = 0
        self.telemetry_events = [
            {
                "event_id": 1,
                "event": "goal.started",
                "timestamp": 1700000000.0,
                "created_at": "2026-02-24T10:00:00+00:00",
                "payload": {"goal_id": "goal-1"},
            },
            {
                "event_id": 2,
                "event": "goal.completed",
                "timestamp": 1700000001.0,
                "created_at": "2026-02-24T10:00:01+00:00",
                "payload": {"goal_id": "goal-1", "steps": 1},
            },
            {
                "event_id": 3,
                "event": "voice.transcribed",
                "timestamp": 1700000002.0,
                "created_at": "2026-02-24T10:00:02+00:00",
                "payload": {
                    "trigger_type": "manual",
                    "source": "local",
                    "text": "open notepad",
                    "confidence": 0.92,
                    "attempt_chain": [{"provider": "local", "status": "success"}],
                },
            },
            {
                "event_id": 4,
                "event": "voice.callback_completed",
                "timestamp": 1700000003.0,
                "created_at": "2026-02-24T10:00:03+00:00",
                "payload": {"trigger_type": "manual", "has_reply": True, "latency_ms": 182.4},
            },
        ]

        self._macro_counter += 1
        macro_id = f"macro-{self._macro_counter}"
        now_iso = datetime.now(timezone.utc).isoformat()
        self.macros[macro_id] = {
            "macro_id": macro_id,
            "name": "UTC time lookup",
            "text": "what time is it in UTC",
            "source": "desktop-ui",
            "actions": ["time_now"],
            "success_count": 3,
            "usage_count": 1,
            "created_at": now_iso,
            "updated_at": now_iso,
            "last_used_at": "",
        }

        self._rollback_counter += 1
        rollback_id = f"rollback-{self._rollback_counter}"
        self.rollbacks[rollback_id] = {
            "rollback_id": rollback_id,
            "goal_id": "goal-rollback-seed",
            "action": "write_file",
            "status": "ready",
            "created_at": now_iso,
            "updated_at": now_iso,
            "operations": [{"type": "restore_text_file", "path": "notes.txt"}],
        }

    def health(self) -> Dict[str, Any]:
        return {
            "status": "ok",
            "service": "fake-desktop-api",
            "tool_count": 2,
            "oauth_maintenance": dict(self.oauth_maintenance),
            "pending_auto_resumes": 0,
            "circuit_breakers": {"count": 1, "open_count": 1},
            "recovery": self.list_recovery_profiles(),
        }

    def rust_health(self) -> Dict[str, Any]:
        return {
            "status": "success",
            "available": True,
            "running": True,
            "binary_path": "backend/rust/target/release/jarvis_backend_bin",
            "probe": {
                "status": "success",
                "event": "health_check",
                "data": {"status": "ok", "service": "fake-rust-backend"},
            },
        }

    def rust_diagnostics(self) -> Dict[str, Any]:
        return {
            "status": "success",
            "runtime": {
                "running": True,
                "binary_path": "backend/rust/target/release/jarvis_backend_bin",
                "requests_total": 42,
                "requests_success": 40,
                "requests_error": 2,
                "timeouts": 1,
                "spawn_count": 1,
                "restart_count": 0,
                "last_event": "batch_execute",
                "last_status": "success",
                "last_error": "",
                "last_roundtrip_ms": 12.4,
                "avg_roundtrip_ms": 8.9,
                "max_roundtrip_ms": 28.1,
                "disabled": False,
                "stderr_tail": [],
            },
        }

    def rust_desktop_context(self, *, timeout_s: float = 8.0) -> Dict[str, Any]:
        return self.rust_request(event="desktop_context", payload={}, timeout_s=timeout_s)

    def rust_batch_execute(
        self,
        *,
        requests: list[Dict[str, Any]],
        continue_on_error: bool = False,
        include_timing: bool = True,
        max_steps: int = 64,
        timeout_s: float = 15.0,
    ) -> Dict[str, Any]:
        del continue_on_error, include_timing, max_steps, timeout_s
        return self.rust_request(event="batch_execute", payload={"requests": requests})

    def rust_automation_plan_execute(
        self,
        *,
        tasks: list[Dict[str, Any]],
        options: Dict[str, Any] | None = None,
        timeout_s: float = 25.0,
    ) -> Dict[str, Any]:
        del timeout_s
        payload: Dict[str, Any] = {"tasks": tasks}
        if isinstance(options, dict):
            payload["options"] = options
        return self.rust_request(event="automation_plan_execute", payload=payload)

    def rust_request(self, *, event: str, payload: Dict[str, Any] | None = None, timeout_s: float = 8.0) -> Dict[str, Any]:
        del timeout_s
        request_payload = payload if isinstance(payload, dict) else {}
        event_name = str(event or "").strip()
        if not event_name:
            return {"status": "error", "message": "event is required"}
        if event_name == "health_check":
            return {"status": "success", "event": event_name, "data": {"status": "ok", "service": "fake-rust-backend"}}
        if event_name == "safety_status":
            return {"status": "success", "event": event_name, "data": {"safe_mode": True, "platform": "windows"}}
        if event_name == "system_snapshot":
            return {
                "status": "success",
                "event": event_name,
                "data": {
                    "memory": {"used": 1024, "total": 4096, "percent": "25.00"},
                    "cpu": {"cores": [8.5, 13.2]},
                    "temperature": [],
                },
            }
        if event_name == "desktop_context":
            return {
                "status": "success",
                "event": event_name,
                "data": {
                    "status": "success",
                    "system": {
                        "memory": {"used": 1024, "total": 4096, "percent": "25.00"},
                        "cpu": {"cores": [8.5, 13.2]},
                        "temperature": [],
                    },
                    "input": {"keys_pressed": 12, "typing_speed_keys_per_sec": 2.4},
                    "window": {"status": "success", "data": {"title": "Fake Window"}},
                    "windows": {"status": "success", "count": 7},
                    "collected_in_ms": 11,
                },
            }
        if event_name == "batch_execute":
            requests = request_payload.get("requests", [])
            items = []
            success_count = 0
            error_count = 0
            if isinstance(requests, list):
                for index, row in enumerate(requests):
                    event_value = str((row or {}).get("event", "")).strip() if isinstance(row, dict) else ""
                    if event_value == "unsupported_event":
                        items.append({"index": index, "event": event_value, "status": "error", "message": "Unsupported"})
                        error_count += 1
                    else:
                        items.append({"index": index, "event": event_value or "echo", "status": "success", "data": {"ok": True}})
                        success_count += 1
            status = "success" if error_count == 0 else ("partial" if success_count > 0 else "failed")
            return {
                "status": "success",
                "event": event_name,
                "data": {
                    "status": status,
                    "count": len(items),
                    "success_count": success_count,
                    "error_count": error_count,
                    "results": items,
                },
            }
        if event_name == "audio_probe":
            return {
                "status": "success",
                "event": event_name,
                "data": {
                    "path": str(request_payload.get("path", "sample.wav")),
                    "format": "wav",
                    "duration_s": 1.2,
                    "sample_rate": 16000,
                },
            }
        if event_name == "automation_plan_execute":
            tasks = request_payload.get("tasks", [])
            task_count = len(tasks) if isinstance(tasks, list) else 0
            return {
                "status": "success",
                "event": event_name,
                "data": {
                    "status": "success",
                    "total": task_count,
                    "completed": task_count,
                    "failed": 0,
                    "errors": [],
                },
            }
        if event_name == "echo":
            return {"status": "success", "event": event_name, "data": request_payload}
        return {"status": "error", "message": f"event '{event_name}' is not allowed via desktop API"}

    def list_tools(self) -> Dict[str, Dict[str, Any]]:
        return {"time_now": {"risk": "low"}, "copy_file": {"risk": "high"}}

    def list_models(self) -> Dict[str, Any]:
        return {
            "models": [],
            "runtime_supervisors": self.model_runtime_supervisors(limit=8),
            "capabilities": self.model_capability_summary(limit_per_task=4),
            "route_bundles": {
                "desktop_agent": self.model_route_bundle(stack_name="desktop_agent"),
                "voice": self.model_route_bundle(stack_name="voice", privacy_mode=True),
            },
        }

    def model_operations_summary(
        self,
        *,
        stack_name: str = "desktop_agent",
        preferred_model_name: str = "",
        limit_per_task: int = 4,
        runtime_limit: int = 8,
        requires_offline: bool = False,
        privacy_mode: bool = False,
        latency_sensitive: bool = False,
        mission_profile: str = "balanced",
        cost_sensitive: bool = False,
        max_cost_units: float | None = None,
    ) -> Dict[str, Any]:
        return {
            "status": "success",
            "stack_name": str(stack_name or "desktop_agent").strip().lower() or "desktop_agent",
            "preferred_model_name": str(preferred_model_name or "").strip().lower(),
            "requires_offline": bool(requires_offline),
            "privacy_mode": bool(privacy_mode),
            "latency_sensitive": bool(latency_sensitive),
            "mission_profile": str(mission_profile or "balanced").strip().lower() or "balanced",
            "cost_sensitive": bool(cost_sensitive),
            "max_cost_units": max_cost_units,
            "provider_credentials": {
                "status": "success",
                "providers": {
                    "groq": {"provider": "groq", "ready": True, "present": True},
                    "nvidia": {"provider": "nvidia", "ready": True, "present": True},
                    "elevenlabs": {"provider": "elevenlabs", "ready": True, "present": True},
                    "local": {"provider": "local", "ready": True, "present": True},
                },
            },
            "capabilities": self.model_capability_summary(limit_per_task=limit_per_task),
            "route_bundle": self.model_route_bundle(
                stack_name=stack_name,
                requires_offline=requires_offline,
                privacy_mode=privacy_mode,
                latency_sensitive=latency_sensitive,
                mission_profile=mission_profile,
                cost_sensitive=cost_sensitive,
                max_cost_units=max_cost_units,
            ),
            "runtime_supervisors": self.model_runtime_supervisors(
                preferred_model_name=preferred_model_name,
                limit=runtime_limit,
            ),
            "connector_diagnostics": self.model_connector_diagnostics(
                include_route_plan=True,
                requires_offline=requires_offline,
                privacy_mode=privacy_mode,
                mission_profile=mission_profile,
            ),
        }

    def model_runtime_supervisors(self, *, preferred_model_name: str = "", limit: int = 8) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 32))
        preferred = str(preferred_model_name or "").strip().lower()
        now_ts = datetime.now(timezone.utc).timestamp()
        raw_reasoning_items = list(self.reasoning_runtime_state.get("items", []))
        if preferred:
            raw_reasoning_items = [
                item
                for item in raw_reasoning_items
                if preferred in str(item.get("name", "")).strip().lower()
            ] or raw_reasoning_items
        reasoning_items: list[Dict[str, Any]] = []
        for item in raw_reasoning_items:
            cooldown_until = float(item.get("runtime_cooldown_until", 0.0) or 0.0)
            cooldown_remaining = max(0.0, cooldown_until - now_ts)
            reasoning_items.append(
                {
                    **item,
                    "runtime_supported": bool(item.get("runtime_supported", True)),
                    "runtime_reason": str(item.get("runtime_reason", "available") or "available"),
                    "runtime_loaded": bool(item.get("runtime_loaded", False)),
                    "runtime_last_error": str(item.get("runtime_last_error", "") or ""),
                    "runtime_last_loaded_at": float(item.get("runtime_last_loaded_at", 0.0) or 0.0),
                    "runtime_load_latency_s": float(item.get("runtime_load_latency_s", 0.0) or 0.0),
                    "runtime_failure_streak": int(item.get("runtime_failure_streak", 0) or 0),
                    "runtime_cooldown_until": cooldown_until,
                    "runtime_cooldown_remaining_s": round(cooldown_remaining, 3),
                    "runtime_last_probe_at": float(item.get("runtime_last_probe_at", 0.0) or 0.0),
                    "runtime_last_probe_ok": bool(item.get("runtime_last_probe_ok", False)),
                    "runtime_last_probe_error": str(item.get("runtime_last_probe_error", "") or ""),
                    "runtime_last_probe_latency_s": float(item.get("runtime_last_probe_latency_s", 0.0) or 0.0),
                    "runtime_probe_attempts": int(item.get("runtime_probe_attempts", 0) or 0),
                    "runtime_last_probe_preview": str(item.get("runtime_last_probe_preview", "") or ""),
                    "runtime_last_probe_prompt": str(item.get("runtime_last_probe_prompt", "") or ""),
                    "runtime_transport": str(item.get("runtime_transport", item.get("backend", "")) or ""),
                    "runtime_bridge_required": bool(item.get("runtime_bridge_required", False)),
                    "runtime_bridge_ready": bool(item.get("runtime_bridge_ready", self.local_reasoning_bridge_state.get("ready", False))),
                }
            )
        loaded_count = sum(1 for item in reasoning_items if bool(item.get("runtime_loaded", False)))
        error_count = sum(1 for item in reasoning_items if str(item.get("runtime_last_error", "")).strip())
        cooldown_count = sum(1 for item in reasoning_items if float(item.get("runtime_cooldown_remaining_s", 0.0) or 0.0) > 0.0)
        probe_healthy_count = sum(1 for item in reasoning_items if bool(item.get("runtime_last_probe_ok", False)))
        bridge_transport_count = sum(1 for item in reasoning_items if str(item.get("runtime_transport", "")).strip().lower() == "bridge")
        ready_count = sum(
            1
            for item in reasoning_items
            if bool(item.get("runtime_supported", False))
            and float(item.get("runtime_cooldown_remaining_s", 0.0) or 0.0) <= 0.0
            and (not bool(item.get("runtime_bridge_required", False)) or bool(item.get("runtime_bridge_ready", False)))
        )
        active_candidate = next((item for item in reasoning_items if bool(item.get("runtime_loaded", False))), reasoning_items[0] if reasoning_items else {})
        reasoning = {
            "status": "success",
            "enabled": True,
            "timeout_s": 90.0,
            "max_new_tokens": 768,
            "prompt_max_chars": 12000,
            "probe_enabled": True,
            "probe_prompt": "Summarize runtime readiness in one short sentence.",
            "probe_max_chars": 320,
            "failure_streak_threshold": 2,
            "failure_cooldown_s": 45.0,
            "candidate_count": len(reasoning_items),
            "runtime_ready": bool(ready_count > 0),
            "loaded_count": loaded_count,
            "error_count": error_count,
            "cooldown_count": cooldown_count,
            "probe_healthy_count": probe_healthy_count,
            "bridge_transport_count": bridge_transport_count,
            "active_model": str(active_candidate.get("name", "") or "").strip().lower(),
            "active_backend": str(active_candidate.get("backend", "") or "").strip().lower(),
            "active_path": str(active_candidate.get("path", "") or "").strip(),
            "bridge": dict(self.local_reasoning_bridge_state),
            "items": reasoning_items[:bounded],
        }
        vision = dict(self.vision_runtime_state)
        vision["items"] = list(self.vision_runtime_state.get("items", []))[:bounded]
        vision["loaded_count"] = sum(1 for item in vision["items"] if bool(item.get("loaded", False)))
        vision["error_count"] = sum(1 for item in vision["items"] if str(item.get("last_error", "")).strip())
        return {
            "status": "success",
            "preferred_model_name": preferred,
            "limit": bounded,
            "reasoning": reasoning,
            "vision": vision,
        }

    def get_local_reasoning_bridge_status(self, *, probe: bool = False) -> Dict[str, Any]:
        payload = dict(self.local_reasoning_bridge_state)
        if probe:
            payload["probe_attempts"] = int(payload.get("probe_attempts", 0) or 0) + 1
            payload["message"] = "reasoning bridge healthy" if payload.get("ready", False) else "reasoning bridge probe failed"
            self.local_reasoning_bridge_state.update(payload)
        return dict(self.local_reasoning_bridge_state)

    def start_local_reasoning_bridge(
        self,
        *,
        wait_ready: bool = True,
        timeout_s: float | None = None,
        reason: str = "desktop-ui",
        force: bool = False,
    ) -> Dict[str, Any]:
        self.local_reasoning_bridge_state["running"] = True
        self.local_reasoning_bridge_state["ready"] = True
        self.local_reasoning_bridge_state["message"] = "reasoning bridge started"
        self.local_reasoning_bridge_state["wait_ready"] = bool(wait_ready)
        self.local_reasoning_bridge_state["timeout_s"] = float(timeout_s or 0.0)
        self.local_reasoning_bridge_state["reason"] = str(reason)
        self.local_reasoning_bridge_state["force"] = bool(force)
        self.local_reasoning_bridge_state["start_attempts"] = int(self.local_reasoning_bridge_state.get("start_attempts", 0) or 0) + 1
        return dict(self.local_reasoning_bridge_state)

    def stop_local_reasoning_bridge(self, *, reason: str = "desktop-ui") -> Dict[str, Any]:
        self.local_reasoning_bridge_state["running"] = False
        self.local_reasoning_bridge_state["ready"] = False
        self.local_reasoning_bridge_state["message"] = "reasoning bridge stopped"
        self.local_reasoning_bridge_state["reason"] = str(reason)
        return dict(self.local_reasoning_bridge_state)

    def probe_local_reasoning_bridge(self, *, force: bool = True) -> Dict[str, Any]:
        self.local_reasoning_bridge_state["probe_attempts"] = int(self.local_reasoning_bridge_state.get("probe_attempts", 0) or 0) + 1
        self.local_reasoning_bridge_state["message"] = (
            "reasoning bridge healthy" if self.local_reasoning_bridge_state.get("ready", False) else "reasoning bridge probe failed"
        )
        self.local_reasoning_bridge_state["force"] = bool(force)
        return dict(self.local_reasoning_bridge_state)

    def restart_local_reasoning_bridge(
        self,
        *,
        wait_ready: bool = True,
        timeout_s: float | None = None,
        reason: str = "desktop-ui",
        force: bool = True,
    ) -> Dict[str, Any]:
        self.local_reasoning_bridge_state["restart_count"] = int(self.local_reasoning_bridge_state.get("restart_count", 0) or 0) + 1
        return self.start_local_reasoning_bridge(wait_ready=wait_ready, timeout_s=timeout_s, reason=reason, force=force)

    def apply_local_reasoning_bridge_profile(
        self,
        *,
        profile_id: str,
        replace: bool = True,
        restart: bool = False,
        wait_ready: bool = True,
        timeout_s: float | None = None,
        force: bool = False,
        override_updates: Dict[str, Any] | None = None,
        template_id: str = "",
    ) -> Dict[str, Any]:
        clean_profile_id = str(profile_id or "").strip().lower()
        clean_template_id = str(template_id or "").strip().lower()
        selected = next(
            (
                row
                for row in self.model_bridge_profiles(task="reasoning", limit=64).get("profiles", [])
                if str((row or {}).get("profile_id", "")).strip().lower() == clean_profile_id
            ),
            None,
        )
        if not isinstance(selected, dict):
            return {"status": "error", "message": f"reasoning bridge profile not found: {clean_profile_id}", "ready": False}
        override_patch = dict(override_updates) if isinstance(override_updates, dict) else (
            dict(selected.get("override_patch", {})) if isinstance(selected.get("override_patch"), dict) else {}
        )
        if replace:
            self.local_reasoning_bridge_state["runtime_overrides"] = {}
        self.local_reasoning_bridge_state["runtime_overrides"] = dict(override_patch)
        self.local_reasoning_bridge_state["active_profile_id"] = clean_profile_id
        self.local_reasoning_bridge_state["active_template_id"] = clean_template_id
        self.local_reasoning_bridge_state["endpoint"] = str(override_patch.get("endpoint", self.local_reasoning_bridge_state.get("endpoint", "")) or "")
        self.local_reasoning_bridge_state["api_mode"] = str(override_patch.get("api_mode", self.local_reasoning_bridge_state.get("api_mode", "")) or "")
        self.local_reasoning_bridge_state["model_hint"] = str(override_patch.get("model_hint", self.local_reasoning_bridge_state.get("model_hint", "")) or "")
        self.local_reasoning_bridge_state["message"] = (
            f"Applied reasoning bridge template '{clean_template_id}' for profile '{clean_profile_id}'."
            if clean_template_id
            else f"Applied reasoning bridge profile '{clean_profile_id}'."
        )
        bridge_payload = dict(self.local_reasoning_bridge_state)
        restart_payload = None
        if restart:
            restart_payload = self.restart_local_reasoning_bridge(
                wait_ready=wait_ready,
                timeout_s=timeout_s,
                reason=f"profile:{clean_profile_id}",
                force=force,
            )
            bridge_payload = dict(restart_payload)
        return {
            "status": "success",
            "profile_id": clean_profile_id,
            "template_id": clean_template_id,
            "profile": dict(selected),
            "replace": bool(replace),
            "override_patch": dict(override_patch),
            "ready": bool(bridge_payload.get("ready", False)),
            "bridge": bridge_payload,
            "restart": restart_payload,
        }

    def warm_reasoning_runtime(
        self,
        *,
        preferred_model_name: str = "",
        load_all: bool = False,
        force_reload: bool = False,
    ) -> Dict[str, Any]:
        preferred = str(preferred_model_name or "").strip().lower()
        selected = list(self.reasoning_runtime_state.get("items", []))
        if preferred:
            selected = [
                item
                for item in selected
                if preferred in str(item.get("name", "")).strip().lower()
            ] or selected
        if not load_all:
            selected = selected[:1]
        now_ts = datetime.now(timezone.utc).timestamp()
        results: list[Dict[str, Any]] = []
        for item in selected:
            item["runtime_loaded"] = True
            item["runtime_last_error"] = ""
            item["runtime_load_latency_s"] = 1.11 if force_reload else float(item.get("runtime_load_latency_s", 0.93) or 0.93)
            item["runtime_last_loaded_at"] = now_ts
            item["runtime_failure_streak"] = 0
            item["runtime_cooldown_until"] = 0.0
            results.append(
                {
                    "status": "success",
                    "model": item.get("name"),
                    "backend": item.get("backend"),
                    "path": item.get("path"),
                    "load_latency_s": item.get("runtime_load_latency_s"),
                }
            )
        self.reasoning_runtime_state["loaded_count"] = sum(
            1 for item in self.reasoning_runtime_state.get("items", []) if bool(item.get("runtime_loaded", False))
        )
        return {
            "status": "success",
            "load_all": bool(load_all),
            "force_reload": bool(force_reload),
            "count": len(results),
            "items": results,
            "runtime": self.model_runtime_supervisors(preferred_model_name=preferred, limit=8)["reasoning"],
        }

    def probe_reasoning_runtime(
        self,
        *,
        preferred_model_name: str = "",
        prompt: str = "",
        force_reload: bool = False,
    ) -> Dict[str, Any]:
        preferred = str(preferred_model_name or "").strip().lower()
        selected = list(self.reasoning_runtime_state.get("items", []))
        if preferred:
            selected = [
                item
                for item in selected
                if preferred in str(item.get("name", "")).strip().lower()
            ] or selected
        if not selected:
            return {
                "status": "error",
                "message": "No local reasoning candidates available.",
                "runtime": self.model_runtime_supervisors(preferred_model_name=preferred, limit=8)["reasoning"],
            }
        item = selected[0]
        now_ts = datetime.now(timezone.utc).timestamp()
        cooldown_until = float(item.get("runtime_cooldown_until", 0.0) or 0.0)
        cooldown_remaining = max(0.0, cooldown_until - now_ts)
        if cooldown_remaining > 0.0 and not force_reload:
            message = f"Runtime probe is cooling down for {round(cooldown_remaining, 3)}s."
            item["runtime_last_probe_at"] = now_ts
            item["runtime_last_probe_ok"] = False
            item["runtime_last_probe_error"] = message
            item["runtime_last_probe_latency_s"] = 0.0
            item["runtime_probe_attempts"] = int(item.get("runtime_probe_attempts", 0) or 0) + 1
            item["runtime_last_probe_prompt"] = ""
            item["runtime_last_probe_preview"] = ""
            return {
                "status": "error",
                "model": item.get("name"),
                "backend": item.get("backend"),
                "path": item.get("path"),
                "message": message,
                "cooldown_remaining_s": round(cooldown_remaining, 3),
                "runtime": self.model_runtime_supervisors(preferred_model_name=preferred or str(item.get("name", "")), limit=8)["reasoning"],
            }
        clean_prompt = str(prompt or "").strip() or "Summarize runtime readiness in one short sentence."
        preview = f"Runtime ready via {str(item.get('backend', 'llama_cpp') or 'llama_cpp')} for {str(item.get('name', 'model') or 'model')}."
        item["runtime_loaded"] = True
        item["runtime_last_error"] = ""
        item["runtime_last_loaded_at"] = now_ts
        item["runtime_load_latency_s"] = 0.84 if force_reload else float(item.get("runtime_load_latency_s", 0.78) or 0.78)
        item["runtime_failure_streak"] = 0
        item["runtime_cooldown_until"] = 0.0
        item["runtime_last_probe_at"] = now_ts
        item["runtime_last_probe_ok"] = True
        item["runtime_last_probe_error"] = ""
        item["runtime_last_probe_latency_s"] = 0.42
        item["runtime_probe_attempts"] = int(item.get("runtime_probe_attempts", 0) or 0) + 1
        item["runtime_last_probe_prompt"] = clean_prompt
        item["runtime_last_probe_preview"] = preview
        return {
            "status": "success",
            "model": item.get("name"),
            "backend": item.get("backend"),
            "path": item.get("path"),
            "probe_prompt": clean_prompt,
            "probe_latency_s": 0.42,
            "response_preview": preview,
            "runtime": self.model_runtime_supervisors(preferred_model_name=preferred or str(item.get("name", "")), limit=8)["reasoning"],
        }

    def reset_reasoning_runtime(self, *, model_name: str = "", clear_all: bool = False) -> Dict[str, Any]:
        target = str(model_name or "").strip().lower()
        removed_paths: list[str] = []
        for item in self.reasoning_runtime_state.get("items", []):
            name = str(item.get("name", "")).strip().lower()
            if clear_all or not target or target in name:
                item["runtime_loaded"] = False
                item["runtime_last_error"] = ""
                item["runtime_failure_streak"] = 0
                item["runtime_cooldown_until"] = 0.0
                item["runtime_last_probe_ok"] = False
                item["runtime_last_probe_error"] = ""
                item["runtime_last_probe_latency_s"] = 0.0
                removed_paths.append(str(item.get("path", "")))
        self.reasoning_runtime_state["loaded_count"] = sum(
            1 for item in self.reasoning_runtime_state.get("items", []) if bool(item.get("runtime_loaded", False))
        )
        return {
            "status": "success",
            "cleared_all": bool(clear_all),
            "removed_count": len(removed_paths),
            "removed_paths": removed_paths,
            "runtime": self.model_runtime_supervisors(preferred_model_name=target, limit=8)["reasoning"],
        }

    def restart_reasoning_runtime(
        self,
        *,
        preferred_model_name: str = "",
        prompt: str = "",
        load_all: bool = False,
        force_reload: bool = True,
        probe: bool = True,
        clear_all: bool = False,
        restart_bridge: bool = False,
    ) -> Dict[str, Any]:
        preferred = str(preferred_model_name or "").strip().lower()
        clear_scope = bool(clear_all or (load_all and not preferred))
        bridge_payload = (
            self.restart_local_reasoning_bridge(
                wait_ready=True,
                timeout_s=12.0,
                reason="desktop_reasoning_restart",
                force=bool(force_reload),
            )
            if restart_bridge
            else self.get_local_reasoning_bridge_status(probe=False)
        )
        reset_payload = self.reset_reasoning_runtime(model_name=preferred, clear_all=clear_scope)
        warm_payload = self.warm_reasoning_runtime(
            preferred_model_name=preferred,
            load_all=bool(load_all),
            force_reload=bool(force_reload),
        )
        probe_payload = (
            self.probe_reasoning_runtime(
                preferred_model_name=preferred,
                prompt=str(prompt or "").strip(),
                force_reload=False,
            )
            if probe
            else None
        )
        runtime_payload = self.model_runtime_supervisors(preferred_model_name=preferred, limit=8)
        reasoning = runtime_payload.get("reasoning", {}) if isinstance(runtime_payload.get("reasoning"), dict) else {}
        recovered = bool(reasoning.get("runtime_ready", False))
        status = "success" if recovered else "degraded"
        return {
            "status": status,
            "message": "Reasoning runtime restart completed." if recovered else "Reasoning runtime restart completed without a fully ready runtime.",
            "preferred_model_name": preferred,
            "load_all": bool(load_all),
            "force_reload": bool(force_reload),
            "probe": bool(probe),
            "clear_all": clear_scope,
            "restart_bridge": bool(restart_bridge),
            "recovered": recovered,
            "active_model": str(reasoning.get("active_model", "") or ""),
            "active_backend": str(reasoning.get("active_backend", "") or ""),
            "stages": [
                {
                    "stage": "bridge_restart",
                    "status": str(bridge_payload.get("status", "unknown")),
                    "message": str(bridge_payload.get("message", "") or ""),
                },
                {
                    "stage": "reset",
                    "status": str(reset_payload.get("status", "unknown")),
                    "message": str(reset_payload.get("message", "") or ""),
                    "removed_count": int(reset_payload.get("removed_count", 0) or 0),
                },
                {
                    "stage": "warm",
                    "status": str(warm_payload.get("status", "unknown")),
                    "message": str(warm_payload.get("message", "") or ""),
                    "count": int(warm_payload.get("count", 0) or 0),
                },
                {
                    "stage": "probe",
                    "status": str((probe_payload or {}).get("status", "skipped")),
                    "message": str((probe_payload or {}).get("message", "") or ""),
                    "model": str((probe_payload or {}).get("model", "") or ""),
                },
            ],
            "bridge": bridge_payload,
            "reset": reset_payload,
            "warm": warm_payload,
            "probe_result": probe_payload,
            "runtime": runtime_payload,
            "recommendations": [
                "Probe the preferred local reasoning runtime after restart so route decisions are based on fresh runtime evidence."
            ],
        }

    def rust_runtime_load(self, *, timeout_s: float = 2.0) -> Dict[str, Any]:
        return {
            "status": "success",
            "event": "runtime_load",
            "data": {
                "max_concurrent": 6,
                "max_queue": 64,
                "running": 1,
                "queued": 0,
                "available_permits": 5,
                "utilization": 0.17,
                "overloaded": False,
                "accepted_total": 42,
                "completed_total": 40,
                "backpressure_rejected_total": 0,
                "cancel_requested_total": 1,
                "cancel_applied_total": 1,
                "timeout_s": float(timeout_s),
            },
        }

    def bridge_status(
        self,
        *,
        include_context: bool = False,
        refresh_rust_caps: bool = False,
        include_tools: bool = True,
        include_voice: bool = False,
    ) -> Dict[str, Any]:
        return {
            "status": "success",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "ready_for_workflow": True,
            "routing": {
                "shared_actions": ["click", "type_text", "hotkey"],
                "python_only": ["win32_window_focus"],
                "rust_only": ["ocr_capture"],
            },
            "python": {"tool_count": 12 if include_tools else 0},
            "rust": {
                "status": "success",
                "readiness": "ready",
                "score": 0.91,
                "refresh_rust_caps": bool(refresh_rust_caps),
            },
            "rust_runtime_load": self.rust_runtime_load(timeout_s=1.2),
            "voice": {"included": bool(include_voice), "tts_bridge_ready": bool(self.tts_bridge_state.get("ready", False))},
            "context_included": bool(include_context),
            "recommendations": ["Keep the Rust bridge warm for desktop execution-heavy missions."],
        }

    def runtime_health_summary(
        self,
        *,
        stack_name: str = "desktop_agent",
        preferred_model_name: str = "",
        limit_per_task: int = 4,
        runtime_limit: int = 8,
        requires_offline: bool = False,
        privacy_mode: bool = False,
        latency_sensitive: bool = False,
        mission_profile: str = "balanced",
        cost_sensitive: bool = False,
        max_cost_units: float | None = None,
        refresh_provider_credentials: bool = False,
        include_bridge_context: bool = False,
        refresh_rust_caps: bool = False,
        probe_tts_bridge: bool = True,
        record_history: bool = True,
    ) -> Dict[str, Any]:
        operations = self.model_operations_summary(
            stack_name=stack_name,
            preferred_model_name=preferred_model_name,
            limit_per_task=limit_per_task,
            runtime_limit=runtime_limit,
            requires_offline=requires_offline,
            privacy_mode=privacy_mode,
            latency_sensitive=latency_sensitive,
            mission_profile=mission_profile,
            cost_sensitive=cost_sensitive,
            max_cost_units=max_cost_units,
        )
        runtime = self.model_runtime_supervisors(preferred_model_name=preferred_model_name, limit=runtime_limit)
        bridge = self.bridge_status(
            include_context=include_bridge_context,
            refresh_rust_caps=refresh_rust_caps,
            include_tools=False,
            include_voice=False,
        )
        rust_load = self.rust_runtime_load(timeout_s=1.8)
        tts_bridge = self.get_local_neural_tts_bridge_status(probe=probe_tts_bridge)
        provider_credentials = operations.get("provider_credentials", {})
        provider_rows = provider_credentials.get("providers", {}) if isinstance(provider_credentials, dict) else {}
        route_bundle = operations.get("route_bundle", {}) if isinstance(operations.get("route_bundle"), dict) else {}
        route_items = route_bundle.get("items", []) if isinstance(route_bundle.get("items"), list) else []
        route_warnings = route_bundle.get("warnings", []) if isinstance(route_bundle.get("warnings"), list) else []
        route_policy_summary = route_bundle.get("launch_policy_summary", {}) if isinstance(route_bundle.get("launch_policy_summary"), dict) else {}
        reasoning = runtime.get("reasoning", {}) if isinstance(runtime.get("reasoning"), dict) else {}
        reasoning_bridge = reasoning.get("bridge", {}) if isinstance(reasoning.get("bridge"), dict) else dict(self.local_reasoning_bridge_state)
        vision = runtime.get("vision", {}) if isinstance(runtime.get("vision"), dict) else {}
        launch_profiles = self.model_bridge_profiles(limit=192)
        launch_health = launch_profiles.get("launch_health_summary", {}) if isinstance(launch_profiles.get("launch_health_summary"), dict) else {}
        rust_data = rust_load.get("data", {}) if isinstance(rust_load.get("data"), dict) else {}
        missing_cloud = [
            name
            for name in ("groq", "nvidia", "elevenlabs")
            if not bool((provider_rows.get(name, {}) if isinstance(provider_rows.get(name, {}), dict) else {}).get("ready", False))
        ]
        alerts: list[Dict[str, Any]] = []
        if not route_items:
            alerts.append({"code": "route_bundle_empty", "severity": "blocker", "message": "No runtime route bundle candidates available."})
        if int(route_policy_summary.get("blocked_task_count", 0) or 0) > 0:
            alerts.append({"code": "route_local_policy_blocked", "severity": "warning", "message": "One or more local routes are blocked by launch policy."})
        elif int(route_policy_summary.get("rerouted_task_count", 0) or 0) > 0:
            alerts.append({"code": "route_local_policy_rerouted", "severity": "info", "message": "One or more local routes were rerouted away from degraded launchers."})
        if int(route_policy_summary.get("blacklisted_task_count", 0) or 0) > 0:
            alerts.append({"code": "route_local_policy_blacklisted", "severity": "warning", "message": "A monitored local route is blacklisted by launch policy."})
        if not bool(reasoning.get("runtime_ready", False)):
            alerts.append({"code": "reasoning_runtime_unready", "severity": "warning", "message": "Reasoning runtime is not fully ready."})
        if bool(reasoning_bridge.get("configured", False)) and not bool(reasoning_bridge.get("ready", False)):
            alerts.append({"code": "reasoning_bridge_not_ready", "severity": "warning", "message": "Managed reasoning bridge is not ready."})
        if not bool(bridge.get("ready_for_workflow", False)):
            alerts.append({"code": "bridge_not_ready", "severity": "blocker", "message": "Bridge is not ready for workflow execution."})
        if bool(rust_data.get("overloaded", False)):
            alerts.append({"code": "rust_runtime_overloaded", "severity": "warning", "message": "Rust runtime is overloaded."})
        if missing_cloud:
            alerts.append(
                {
                    "code": "cloud_provider_headroom_reduced",
                    "severity": "warning",
                    "message": f"Missing ready cloud providers: {', '.join(missing_cloud)}.",
                }
            )
        if bool(tts_bridge.get("configured", False)) and not bool(tts_bridge.get("ready", False)):
            alerts.append({"code": "tts_bridge_not_ready", "severity": "warning", "message": "Local neural TTS bridge is not ready."})
        if int(launch_health.get("demoted_template_count", 0) or 0) > 0:
            alerts.append({"code": "launch_templates_demoted", "severity": "warning", "message": "One or more launch templates are demoted."})
        if int(launch_health.get("blacklisted_template_count", 0) or 0) > 0:
            alerts.append({"code": "launch_templates_blacklisted", "severity": "warning", "message": "One or more launch templates are blacklisted."})
        if int(launch_health.get("template_count", 0) or 0) > 0 and int(launch_health.get("autonomous_ready_template_count", 0) or 0) <= 0:
            alerts.append({"code": "launch_templates_no_autonomy_ready", "severity": "warning", "message": "No launch templates are autonomy-ready."})
        blocker_count = sum(1 for item in alerts if item.get("severity") == "blocker")
        warning_count = sum(1 for item in alerts if item.get("severity") == "warning")
        score = 92.0
        if blocker_count:
            score = 54.0
        elif warning_count:
            score = 78.0
        status = "success" if blocker_count == 0 and warning_count == 0 else ("blocked" if blocker_count else "degraded")
        return {
            "status": status,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "stack_name": str(stack_name or "desktop_agent").strip().lower() or "desktop_agent",
            "preferred_model_name": str(preferred_model_name or "").strip().lower(),
            "mission_profile": str(mission_profile or "balanced").strip().lower() or "balanced",
            "requires_offline": bool(requires_offline),
            "privacy_mode": bool(privacy_mode),
            "latency_sensitive": bool(latency_sensitive),
            "cost_sensitive": bool(cost_sensitive),
            "max_cost_units": max_cost_units,
            "score": score,
            "blocker_count": blocker_count,
            "warning_count": warning_count,
            "alerts": alerts,
            "recommendations": [
                "Restart the reasoning runtime before planner-heavy local work if readiness or probe health falls.",
                "Keep Groq, NVIDIA, and ElevenLabs credentials configured so routing has cloud failover headroom.",
            ],
            "subsystems": {
                "routing": {
                    "status": "ready" if route_items else "blocked",
                    "ready": bool(route_items),
                    "score": 100.0 if route_items else 20.0,
                    "stack_name": str(stack_name or "desktop_agent").strip().lower() or "desktop_agent",
                    "mission_profile": str(mission_profile or "balanced").strip().lower() or "balanced",
                "item_count": len(route_items),
                "provider_counts": route_bundle.get("provider_counts", {}),
                "warning_count": len(route_warnings),
                "local_provider_task_count": int(route_policy_summary.get("local_provider_task_count", 0) or 0),
                "policy_monitored_count": int(route_policy_summary.get("policy_monitored_task_count", 0) or 0),
                "matched_policy_count": int(route_policy_summary.get("matched_policy_task_count", 0) or 0),
                "unmatched_policy_count": int(route_policy_summary.get("unmatched_policy_task_count", 0) or 0),
                "local_viable_count": int(route_policy_summary.get("local_viable_task_count", 0) or 0),
                "autonomy_safe_count": int(route_policy_summary.get("autonomy_safe_task_count", 0) or 0),
                "review_required_count": int(route_policy_summary.get("review_required_task_count", 0) or 0),
                "blacklisted_count": int(route_policy_summary.get("blacklisted_task_count", 0) or 0),
                "rerouted_count": int(route_policy_summary.get("rerouted_task_count", 0) or 0),
                "blocked_count": int(route_policy_summary.get("blocked_task_count", 0) or 0),
                "recovery_pending_count": int(route_policy_summary.get("recovery_pending_task_count", 0) or 0),
            },
                "reasoning": {
                    "status": "ready" if bool(reasoning.get("runtime_ready", False)) else "degraded",
                    "ready": bool(reasoning.get("runtime_ready", False)),
                    "score": 100.0 if bool(reasoning.get("runtime_ready", False)) else 55.0,
                    "candidate_count": int(reasoning.get("candidate_count", 0) or 0),
                    "loaded_count": int(reasoning.get("loaded_count", 0) or 0),
                    "probe_healthy_count": int(reasoning.get("probe_healthy_count", 0) or 0),
                    "cooldown_count": int(reasoning.get("cooldown_count", 0) or 0),
                    "active_model": str(reasoning.get("active_model", "") or ""),
                    "active_backend": str(reasoning.get("active_backend", "") or ""),
                },
                "reasoning_bridge": {
                    "status": "ready" if bool(reasoning_bridge.get("ready", False)) else "degraded",
                    "ready": bool(reasoning_bridge.get("ready", False)),
                    "score": 100.0 if bool(reasoning_bridge.get("ready", False)) else 48.0,
                    "configured": bool(reasoning_bridge.get("configured", False)),
                    "running": bool(reasoning_bridge.get("running", False)),
                    "message": str(reasoning_bridge.get("message", "") or ""),
                },
                "vision": {
                    "status": "ready" if int(vision.get("loaded_count", 0) or 0) > 0 else "idle",
                    "ready": bool(int(vision.get("loaded_count", 0) or 0) > 0),
                    "score": 100.0 if int(vision.get("loaded_count", 0) or 0) > 0 else 45.0,
                    "loaded_count": int(vision.get("loaded_count", 0) or 0),
                    "artifact_count": len(vision.get("items", [])) if isinstance(vision.get("items"), list) else 0,
                    "device": str(vision.get("device", "") or ""),
                },
                "launch_templates": {
                    "status": "ready" if int(launch_health.get("stable_ready_template_count", 0) or 0) > 0 else "degraded",
                    "ready": bool(int(launch_health.get("stable_ready_template_count", 0) or 0) > 0),
                    "score": 100.0 if int(launch_health.get("stable_ready_template_count", 0) or 0) > 0 else 58.0,
                    "template_count": int(launch_health.get("template_count", 0) or 0),
                    "ready_count": int(launch_health.get("ready_template_count", 0) or 0),
                    "stable_ready_count": int(launch_health.get("stable_ready_template_count", 0) or 0),
                    "unstable_count": int(launch_health.get("unstable_template_count", 0) or 0),
                    "demoted_count": int(launch_health.get("demoted_template_count", 0) or 0),
                    "suppressed_count": int(launch_health.get("suppressed_template_count", 0) or 0),
                    "blacklisted_count": int(launch_health.get("blacklisted_template_count", 0) or 0),
                    "autonomous_ready_count": int(launch_health.get("autonomous_ready_template_count", 0) or 0),
                    "history_count": int(launch_health.get("history_count", 0) or 0),
                },
                "bridge": {
                    "status": "ready" if bool(bridge.get("ready_for_workflow", False)) else "blocked",
                    "ready": bool(bridge.get("ready_for_workflow", False)),
                    "score": 100.0 if bool(bridge.get("ready_for_workflow", False)) else 25.0,
                    "ready_for_workflow": bool(bridge.get("ready_for_workflow", False)),
                    "message": str(bridge.get("message", "") or ""),
                },
                "rust_runtime": {
                    "status": "ready" if not bool(rust_data.get("overloaded", False)) else "degraded",
                    "ready": not bool(rust_data.get("overloaded", False)),
                    "score": 100.0 if not bool(rust_data.get("overloaded", False)) else 40.0,
                    "running": int(rust_data.get("running", 0) or 0),
                    "queued": int(rust_data.get("queued", 0) or 0),
                    "overloaded": bool(rust_data.get("overloaded", False)),
                },
                "voice_bridge": {
                    "status": "ready" if bool(tts_bridge.get("ready", False)) else "degraded",
                    "ready": bool(tts_bridge.get("ready", False)),
                    "score": 100.0 if bool(tts_bridge.get("ready", False)) else 30.0,
                    "configured": bool(tts_bridge.get("configured", False)),
                    "running": bool(tts_bridge.get("running", False)),
                    "ready_flag": bool(tts_bridge.get("ready", False)),
                    "message": str(tts_bridge.get("message", "") or ""),
                },
                "providers": {
                    "status": "ready" if not missing_cloud else "degraded",
                    "ready": bool(len(provider_rows) > 0),
                    "score": 100.0 if not missing_cloud else 60.0,
                    "ready_count": sum(
                        1 for row in provider_rows.values() if isinstance(row, dict) and bool(row.get("ready", row.get("present", False)))
                    ),
                    "total_count": len(provider_rows),
                    "missing_cloud": missing_cloud,
                },
            },
            "model_operations": operations,
            "runtime_supervisors": runtime,
            "reasoning_bridge": reasoning_bridge,
            "bridge_status": bridge,
            "rust_runtime_load": rust_load,
            "tts_local_neural_bridge": tts_bridge,
            "provider_credentials": provider_credentials,
            "connector_diagnostics": operations.get("connector_diagnostics", {}),
            "launch_health": launch_health,
            "route_policy_summary": route_policy_summary,
        }

    def runtime_health_history(
        self,
        *,
        limit: int = 120,
        stack_name: str = "",
        preferred_model_name: str = "",
        refresh: bool = False,
    ) -> Dict[str, Any]:
        if refresh:
            current = self.runtime_health_summary(
                stack_name=stack_name or "desktop_agent",
                preferred_model_name=preferred_model_name,
                probe_tts_bridge=False,
                record_history=True,
            )
            self.runtime_health_history_state.append(
                {
                    "snapshot_id": f"health-{len(self.runtime_health_history_state) + 1}",
                    "created_at": current.get("generated_at", datetime.now(timezone.utc).isoformat()),
                    "status": current.get("status", "success"),
                    "score": current.get("score", 0.0),
                    "blocker_count": current.get("blocker_count", 0),
                    "warning_count": current.get("warning_count", 0),
                    "stack_name": current.get("stack_name", "desktop_agent"),
                    "preferred_model_name": current.get("preferred_model_name", ""),
                    "mission_profile": current.get("mission_profile", "balanced"),
                    "subsystems": current.get("subsystems", {}),
                    "alerts": current.get("alerts", []),
                    "recommendations": current.get("recommendations", []),
                }
            )
        rows = self.runtime_health_history_state[-max(1, min(int(limit), 5000)) :]
        latest = rows[-1] if rows else {}
        earliest = rows[0] if rows else {}
        latest_score = float(latest.get("score", 0.0) or 0.0) if isinstance(latest, dict) else 0.0
        earliest_score = float(earliest.get("score", 0.0) or 0.0) if isinstance(earliest, dict) else 0.0
        return {
            "status": "success",
            "count": len(rows),
            "total": len(rows),
            "limit": max(1, min(int(limit), 5000)),
            "items": rows,
            "latest": latest,
            "history_path": "data/runtime/runtime_health_history.jsonl",
            "diagnostics": {
                "status_counts": {
                    "success": sum(1 for row in rows if str(row.get("status", "")).strip().lower() == "success"),
                    "degraded": sum(1 for row in rows if str(row.get("status", "")).strip().lower() == "degraded"),
                    "blocked": sum(1 for row in rows if str(row.get("status", "")).strip().lower() == "blocked"),
                },
                "degradation_events": sum(1 for row in rows if str(row.get("status", "")).strip().lower() in {"degraded", "blocked"}),
                "latest_score": latest_score,
                "earliest_score": earliest_score,
                "score_delta": round(latest_score - earliest_score, 4),
                "score_min": min((float(row.get("score", 0.0) or 0.0) for row in rows), default=0.0),
                "score_max": max((float(row.get("score", 0.0) or 0.0) for row in rows), default=0.0),
                "unstable_subsystems": [{"name": "reasoning_bridge", "count": 1}],
            },
        }

    def warm_vision_runtime(self, *, models: list[str] | None = None, force_reload: bool = False) -> Dict[str, Any]:
        selected = {str(item or "").strip().lower() for item in (models or []) if str(item or "").strip()}
        if not selected:
            selected = {"yolo", "clip"}
        results: list[Dict[str, Any]] = []
        for item in self.vision_runtime_state.get("items", []):
            model_name = str(item.get("model", "")).strip().lower()
            if model_name not in selected:
                continue
            item["loaded"] = True
            item["last_error"] = ""
            item["load_latency_s"] = 0.82 if force_reload else max(0.12, float(item.get("load_latency_s", 0.0) or 0.0))
            item["attempts"] = int(item.get("attempts", 0) or 0) + 1
            item["successes"] = int(item.get("successes", 0) or 0) + 1
            results.append({**item, "status": "success", "warm_latency_s": item["load_latency_s"]})
        self.vision_runtime_state["loaded_count"] = sum(
            1 for item in self.vision_runtime_state.get("items", []) if bool(item.get("loaded", False))
        )
        return {
            "status": "success",
            "count": len(results),
            "items": results,
            "runtime": self.model_runtime_supervisors(limit=8)["vision"],
        }

    def reset_vision_runtime(self, *, models: list[str] | None = None, clear_cache: bool = False) -> Dict[str, Any]:
        selected = {str(item or "").strip().lower() for item in (models or []) if str(item or "").strip()}
        if not selected:
            selected = {"yolo", "sam", "clip", "blip"}
        removed: list[str] = []
        for item in self.vision_runtime_state.get("items", []):
            model_name = str(item.get("model", "")).strip().lower()
            if model_name not in selected:
                continue
            item["loaded"] = False
            item["last_error"] = ""
            removed.append(model_name)
        self.vision_runtime_state["loaded_count"] = sum(
            1 for item in self.vision_runtime_state.get("items", []) if bool(item.get("loaded", False))
        )
        if clear_cache:
            self.vision_runtime_state["embedding_cache_size"] = 0
        return {
            "status": "success",
            "removed": removed,
            "clear_cache": bool(clear_cache),
            "runtime": self.model_runtime_supervisors(limit=8)["vision"],
        }

    def model_capability_summary(self, *, limit_per_task: int = 4) -> Dict[str, Any]:
        bounded = max(1, min(int(limit_per_task), 20))
        return {
            "status": "success",
            "task_count": 6,
            "provider_count": 3,
            "providers": {
                "groq": {"provider": "groq", "ready": True, "present": True},
                "nvidia": {"provider": "nvidia", "ready": True, "present": True},
                "elevenlabs": {"provider": "elevenlabs", "ready": True, "present": True},
            },
            "tasks": [
                {
                    "task": "reasoning",
                    "profile_count": 3,
                    "available_count": 3,
                    "inventory_count": 2,
                    "providers": {"local": 1, "groq": 1, "nvidia": 1},
                    "local_paths": ["E:/J.A.R.V.I.S/reasoning/Meta-Llama-3.1-8B-Instruct-Q8_0.gguf"],
                    "top_models": [{"name": "local-auto-reasoning-qwen3-14b", "provider": "local"}][:bounded],
                },
                {
                    "task": "embedding",
                    "profile_count": 2,
                    "available_count": 2,
                    "inventory_count": 1,
                    "providers": {"local": 1, "nvidia": 1},
                    "local_paths": ["E:/J.A.R.V.I.S/embeddings/all-mpnet-base-v2(Embeddings_model)"],
                    "top_models": [{"name": "local-auto-embedding-all-mpnet-base-v2", "provider": "local"}][:bounded],
                },
                {
                    "task": "intent",
                    "profile_count": 1,
                    "available_count": 1,
                    "inventory_count": 1,
                    "providers": {"local": 1},
                    "local_paths": ["E:/J.A.R.V.I.S/custom_intents/bart-large-mnli (Custom_intent_model)"],
                    "top_models": [{"name": "local-auto-intent-bart-large-mnli", "provider": "local"}][:bounded],
                },
            ],
        }

    def _resolve_model_setup_scope(
        self,
        *,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, str]:
        clean_manifest = str(manifest_path or "").strip().replace("\\", "/")
        clean_workspace = str(workspace_root or "").strip().replace("\\", "/")
        if not clean_workspace and clean_manifest:
            manifest_parts = [part for part in clean_manifest.split("/") if part]
            if len(manifest_parts) >= 2:
                if manifest_parts[-2].lower() == "jarvis_backend":
                    clean_workspace = "/".join(manifest_parts[:-2])
                else:
                    clean_workspace = "/".join(manifest_parts[:-1])
        if not clean_workspace:
            clean_workspace = "E:/J.A.R.V.I.S"
        if not clean_manifest:
            clean_manifest = f"{clean_workspace}/JARVIS_BACKEND/Models to Download.txt"
        return {
            "manifest_path": clean_manifest,
            "workspace_root": clean_workspace,
            "scope_key": f"{clean_workspace.lower()}::{clean_manifest.lower()}",
        }

    def _record_model_setup_scope(
        self,
        route: str,
        *,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, str]:
        scope = self._resolve_model_setup_scope(
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        self.model_setup_scope_calls.append({"route": route, **scope})
        return scope

    def _build_model_setup_manual_run(
        self,
        *,
        task: str = "",
        manifest_path: str = "",
        workspace_root: str = "",
        status: str = "success",
        run_id: str = "manual-run-demo",
    ) -> Dict[str, Any]:
        scope = self._resolve_model_setup_scope(
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        return {
            "status": status,
            "run_id": run_id,
            "task": task,
            "manifest_path": scope["manifest_path"],
            "workspace_root": scope["workspace_root"],
            "scope_key": scope["scope_key"],
            "selected_count": 1,
            "selected_item_keys": ["manual-convert-qwen"],
            "planned_count": 1,
            "success_count": 1 if status == "success" else 0,
            "warning_count": 0,
            "error_count": 0,
            "blocked_count": 0,
            "cancelled_count": 0,
            "step_success_count": 1 if status == "success" else 0,
            "step_error_count": 0,
            "step_skipped_count": 0,
            "created_at": "2026-03-15T08:04:00+00:00",
            "updated_at": "2026-03-15T08:05:00+00:00",
            "message": "manual pipeline run ready",
            "progress": {"completed_items": 1, "total_items": 1, "percent": 100.0},
            "items": [],
        }

    def _build_model_setup_install_run(
        self,
        *,
        task: str = "",
        manifest_path: str = "",
        workspace_root: str = "",
        status: str = "success",
        run_id: str = "install-run-demo",
    ) -> Dict[str, Any]:
        scope = self._resolve_model_setup_scope(
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        return {
            "status": status,
            "run_id": run_id,
            "task": task,
            "manifest_path": scope["manifest_path"],
            "workspace_root": scope["workspace_root"],
            "scope_key": scope["scope_key"],
            "selected_count": 1,
            "selected_item_keys": ["reasoning-llama"],
            "requested_item_keys": ["reasoning-llama"],
            "success_count": 1 if status == "success" else 0,
            "error_count": 0,
            "skipped_count": 0,
            "blocked_count": 0,
            "cancelled_count": 0,
            "verified_count": 1 if status == "success" else 0,
            "observed_count": 1 if status == "success" else 0,
            "verification_error_count": 0,
            "created_at": "2026-03-15T08:04:00+00:00",
            "updated_at": "2026-03-15T08:05:00+00:00",
            "message": "install run ready",
            "progress": {"completed_items": 1, "total_items": 1, "percent": 100.0},
            "items": [],
        }

    def model_local_inventory(
        self,
        *,
        task: str = "",
        limit: int = 200,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        scope = self._record_model_setup_scope(
            "model_local_inventory",
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        bounded = max(1, min(int(limit), 2000))
        items = [
            {
                "key": "reasoning:E:/J.A.R.V.I.S/all_rounder/Qwen3-14B-GGUF/qwen3-14b-q8_0.gguf".lower(),
                "task": "reasoning",
                "name": "qwen3-14b-q8_0",
                "path": "E:/J.A.R.V.I.S/all_rounder/Qwen3-14B-GGUF/qwen3-14b-q8_0.gguf",
                "source": "file",
                "format": ".gguf",
                "size_bytes": 12 * 1024 * 1024 * 1024,
            },
            {
                "key": "reasoning:E:/J.A.R.V.I.S/reasoning/Meta-Llama-3.1-8B-Instruct-Q8_0.gguf".lower(),
                "task": "reasoning",
                "name": "Meta-Llama-3.1-8B-Instruct-Q8_0",
                "path": "E:/J.A.R.V.I.S/reasoning/Meta-Llama-3.1-8B-Instruct-Q8_0.gguf",
                "source": "file",
                "format": ".gguf",
                "size_bytes": 8 * 1024 * 1024 * 1024,
            },
            {
                "key": "tts:E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf".lower(),
                "task": "tts",
                "name": "Orpheus-3B-TTS.f16",
                "path": "E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf",
                "source": "file",
                "format": ".gguf",
                "size_bytes": 3 * 1024 * 1024 * 1024,
            },
            {
                "key": "stt:E:/J.A.R.V.I.S/stt/whisper-large-v3".lower(),
                "task": "stt",
                "name": "whisper-large-v3",
                "path": "E:/J.A.R.V.I.S/stt/whisper-large-v3",
                "source": "directory",
                "format": "directory",
                "size_bytes": 5 * 1024 * 1024 * 1024,
            },
            {
                "key": "embedding:E:/J.A.R.V.I.S/embeddings/bge-large-en-v1.5".lower(),
                "task": "embedding",
                "name": "bge-large-en-v1.5",
                "path": "E:/J.A.R.V.I.S/embeddings/bge-large-en-v1.5",
                "source": "directory",
                "format": "directory",
                "size_bytes": 512 * 1024 * 1024,
            },
        ]
        clean_task = str(task or "").strip().lower()
        if clean_task:
            items = [item for item in items if str(item.get("task", "")).strip().lower() == clean_task]
        task_counts: Dict[str, int] = {}
        for item in items:
            task_name = str(item.get("task", "")).strip().lower() or "unknown"
            task_counts[task_name] = int(task_counts.get(task_name, 0)) + 1
        launch_by_task = {
            "reasoning": {"profile_count": 1, "ready_profile_count": 1},
            "tts": {"profile_count": 1, "ready_profile_count": 1},
        }
        if clean_task:
            launch_by_task = {clean_task: dict(launch_by_task.get(clean_task, {"profile_count": 0, "ready_profile_count": 0}))}
        return {
            "status": "success",
            "task": clean_task,
            "limit": bounded,
            "workspace_root": scope["workspace_root"],
            "manifest_path": scope["manifest_path"],
            "task_counts": task_counts,
            "launch_summary": {
                "profile_count": 2 if not clean_task else int(launch_by_task.get(clean_task, {}).get("profile_count", 0)),
                "ready_profile_count": 2 if not clean_task else int(launch_by_task.get(clean_task, {}).get("ready_profile_count", 0)),
                "by_task": launch_by_task,
            },
            "inventory": {
                "status": "success",
                "count": len(items),
                "items": items[:bounded],
                "task": clean_task,
            },
            "runtime": {"status": "success", "count": len(items)},
            "bridge_profiles": self.model_bridge_profiles(task=clean_task or "reasoning", limit=min(64, bounded)).get("profiles", []),
            "manifest": {
                "status": "success",
                "path": scope["manifest_path"],
                "workspace_root": scope["workspace_root"],
                "model_count": len(items),
            },
            "provider_credentials": {
                "status": "success",
                "providers": {
                    "groq": {"provider": "groq", "ready": True, "present": True},
                    "nvidia": {"provider": "nvidia", "ready": True, "present": True},
                    "elevenlabs": {"provider": "elevenlabs", "ready": True, "present": True},
                    "local": {"provider": "local", "ready": True, "present": True},
                },
            },
        }

    def model_setup_workspace(
        self,
        *,
        refresh_provider_credentials: bool = False,
        limit: int = 200,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        scope = self._record_model_setup_scope(
            "model_setup_workspace",
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        _ = refresh_provider_credentials
        _ = limit
        directories = [
            {
                "key": "directory:e:/j.a.r.v.i.s/all_rounder",
                "name": "all_rounder",
                "task": "reasoning",
                "path": "E:/J.A.R.V.I.S/all_rounder",
                "workspace_relative_path": "all_rounder",
                "present": True,
                "missing": False,
                "aliases": ["all_rounder"],
            },
            {
                "key": "directory:e:/j.a.r.v.i.s/custom_intents",
                "name": "custom_intent",
                "task": "intent",
                "path": "E:/J.A.R.V.I.S/custom_intents",
                "workspace_relative_path": "custom_intents",
                "present": False,
                "missing": True,
                "aliases": ["custom_intent", "custom_intents"],
            },
        ]
        return {
            "status": "success",
            "workspace_root": scope["workspace_root"],
            "manifest_path": scope["manifest_path"],
            "directories": directories,
            "directory_actions": [
                {
                    "kind": "create_directory",
                    "name": "custom_intent",
                    "task": "intent",
                    "path": "E:/J.A.R.V.I.S/custom_intents",
                    "workspace_relative_path": "custom_intents",
                    "aliases": ["custom_intent", "custom_intents"],
                    "safe": True,
                    "present": False,
                }
            ],
            "required_providers": [
                {"provider": "groq", "required_by_manifest": True, "present": True, "ready": True, "source": "env"},
                {
                    "provider": "elevenlabs",
                    "required_by_manifest": True,
                    "present": True,
                    "ready": False,
                    "source": "config",
                    "missing_requirements": ["ELEVENLABS_VOICE_ID"],
                },
            ],
            "recommendations": [
                "Create 1 missing manifest directory before placing new local models.",
            ],
            "summary": {
                "directory_count": 2,
                "present_directory_count": 1,
                "missing_directory_count": 1,
                "required_provider_count": 2,
                "ready_required_provider_count": 1,
                "missing_required_provider_count": 1,
                "model_count": 4,
                "present_model_count": 2,
                "missing_model_count": 2,
                "workspace_ready": False,
                "stack_ready": False,
                "readiness_score": 56,
            },
            "manifest": {
                "status": "success",
                "path": scope["manifest_path"],
                "workspace_root": scope["workspace_root"],
                "model_count": 4,
                "directory_count": 2,
                "provider_count": 2,
                "providers": ["groq", "elevenlabs"],
                "directories": directories,
            },
            "inventory": self.model_local_inventory(limit=12, manifest_path=scope["manifest_path"], workspace_root=scope["workspace_root"]),
            "provider_credentials": self.model_local_inventory(
                limit=12,
                manifest_path=scope["manifest_path"],
                workspace_root=scope["workspace_root"],
            ).get("provider_credentials", {}),
        }

    def model_setup_workspace_scaffold(
        self,
        *,
        dry_run: bool = False,
        refresh_provider_credentials: bool = False,
        limit: int = 200,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        scope = self._record_model_setup_scope(
            "model_setup_workspace_scaffold",
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        _ = refresh_provider_credentials
        _ = limit
        workspace = self.model_setup_workspace(
            manifest_path=scope["manifest_path"],
            workspace_root=scope["workspace_root"],
        )
        action_status = "planned" if dry_run else "created"
        action_message = "directory would be created" if dry_run else "directory created"
        actions = [
            {
                "kind": "create_directory",
                "name": "custom_intent",
                "task": "intent",
                "path": "E:/J.A.R.V.I.S/custom_intents",
                "workspace_relative_path": "custom_intents",
                "aliases": ["custom_intent", "custom_intents"],
                "safe": True,
                "present": False,
                "status": action_status,
                "message": action_message,
            }
        ]
        if not dry_run:
            workspace = dict(workspace)
            workspace["summary"] = dict(workspace.get("summary", {}))
            workspace["summary"]["present_directory_count"] = 2
            workspace["summary"]["missing_directory_count"] = 0
            workspace["summary"]["workspace_ready"] = False
            workspace["summary"]["readiness_score"] = 69
        return {
            "status": "success",
            "dry_run": bool(dry_run),
            "action_count": len(actions),
            "created_count": 0 if dry_run else 1,
            "existing_count": 0,
            "blocked_count": 0,
            "error_count": 0,
            "actions": actions,
            "workspace": workspace,
            "inventory": self.model_local_inventory(
                limit=12,
                manifest_path=scope["manifest_path"],
                workspace_root=scope["workspace_root"],
            ),
            "provider_credentials": self.model_local_inventory(
                limit=12,
                manifest_path=scope["manifest_path"],
                workspace_root=scope["workspace_root"],
            ).get("provider_credentials", {}),
            "setup_plan": {
                "status": "success",
                "summary": {"planned_count": 2},
                "items": [],
            },
        }

    def model_setup_mission(
        self,
        *,
        refresh_provider_credentials: bool = False,
        limit: int = 200,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        scope = self._record_model_setup_scope(
            "model_setup_mission",
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        _ = refresh_provider_credentials
        _ = limit
        workspace = self.model_setup_workspace(
            manifest_path=scope["manifest_path"],
            workspace_root=scope["workspace_root"],
        )
        stored_mission = {
            "mission_id": "msm_demo_scope",
            "status": "blocked",
            "mission_status": "manual",
            "workspace_root": scope["workspace_root"],
            "manifest_path": scope["manifest_path"],
            "resume_ready": False,
            "manual_attention_required": True,
            "recovery_profile": "provider_credentials",
            "recovery_hint": "Configure elevenlabs",
            "auto_resume_candidate": False,
            "resume_trigger": "manual_attention",
            "resume_blockers": ["provider_credentials"],
            "auto_resume_reason": "Provider credentials still need to be configured and verified.",
            "ready_action_count": 2,
            "manual_action_count": 1,
            "blocked_action_count": 1,
            "launch_count": 1,
            "resume_count": 0,
            "updated_at": "2026-03-15T08:01:30+00:00",
        }
        auto_resume_candidate = {
            "mission_id": "msm_auto_scope",
            "status": "resume_ready",
            "mission_status": "ready",
            "workspace_root": scope["workspace_root"],
            "manifest_path": scope["manifest_path"],
            "resume_ready": True,
            "manual_attention_required": False,
            "recovery_profile": "resume_ready",
            "recovery_hint": "Resume the next auto-runnable local-model setup actions.",
            "auto_resume_candidate": True,
            "resume_trigger": "ready_now",
            "resume_blockers": [],
            "auto_resume_reason": "Auto-runnable setup actions are ready right now.",
            "ready_action_count": 1,
            "manual_action_count": 0,
            "blocked_action_count": 0,
            "launch_count": 0,
            "resume_count": 1,
            "updated_at": "2026-03-15T08:03:15+00:00",
        }
        mission_history = {
            "status": "success",
            "count": 2,
            "total": 2,
            "items": [stored_mission, auto_resume_candidate],
            "status_counts": {"blocked": 1, "resume_ready": 1},
            "recovery_profile_counts": {"provider_credentials": 1, "resume_ready": 1},
            "resume_ready_count": 1,
            "manual_attention_count": 1,
            "running_count": 0,
            "auto_resume_candidate_count": 1,
            "latest_resume_ready": auto_resume_candidate,
            "latest_attention_required": stored_mission,
            "latest_running": None,
            "latest_auto_resume_candidate": auto_resume_candidate,
        }
        return {
            "status": "success",
            "generated_at": "2026-03-15T08:00:00+00:00",
            "mission_status": "ready",
            "summary": {
                "action_count": 4,
                "ready_action_count": 2,
                "manual_action_count": 1,
                "blocked_action_count": 1,
                "in_progress_count": 0,
                "launch_recommended": True,
                "workspace_ready": False,
                "stack_ready": False,
                "readiness_score": 56,
            },
            "recommendations": [
                "Create missing manifest directories",
                "Run auto-installable model setup tasks",
            ],
            "actions": [
                {
                    "id": "scaffold_workspace",
                    "kind": "scaffold_workspace",
                    "stage": "workspace",
                    "title": "Create missing manifest directories",
                    "status": "ready",
                    "auto_runnable": True,
                    "item_count": 1,
                },
                {
                    "id": "configure_provider:elevenlabs",
                    "kind": "configure_provider_credentials",
                    "stage": "provider",
                    "title": "Configure elevenlabs",
                    "status": "manual",
                    "auto_runnable": False,
                    "provider": "elevenlabs",
                    "blockers": ["ELEVENLABS_VOICE_ID"],
                },
                {
                    "id": "launch_setup_install:auto",
                    "kind": "launch_setup_install",
                    "stage": "setup",
                    "title": "Run auto-installable model setup tasks",
                    "status": "ready",
                    "auto_runnable": True,
                    "item_count": 2,
                    "item_keys": ["embedding-all-mpnet-base-v2", "stt-whisper-large-v3"],
                },
                {
                    "id": "review_manual_pipeline_blockers",
                    "kind": "review_manual_pipeline_blockers",
                    "stage": "manual_review",
                    "title": "Review blocked manual model tasks",
                    "status": "blocked",
                    "auto_runnable": False,
                    "blockers": ["A verified Hugging Face access token is required before this source can be downloaded."],
                },
            ],
            "workspace": workspace,
            "setup_plan": {
                "status": "success",
                "summary": {"planned_count": 3, "auto_installable_count": 2, "manual_count": 1},
            },
            "preflight": {
                "status": "success",
                "summary": {"blocked_count": 0},
            },
            "manual_pipeline": {
                "status": "success",
                "summary": {"ready_count": 0, "blocked_count": 1},
            },
            "install_runs": {"status": "success", "active_count": 0, "items": []},
            "manual_runs": {"status": "success", "active_count": 0, "items": []},
            "stored_mission": stored_mission,
            "mission_history": mission_history,
            "resume_advice": {
                "status": "blocked",
                "can_resume_now": False,
                "can_auto_resume_now": False,
                "auto_resume_candidate": False,
                "resume_ready": False,
                "waiting_on_active_runs": False,
                "manual_attention_required": True,
                "recovery_profile": "provider_credentials",
                "resume_trigger": "manual_attention",
                "resume_blockers": ["provider_credentials"],
                "auto_resume_reason": "Provider credentials still need to be configured and verified.",
                "recovery_hint": "Configure elevenlabs",
                "active_run_count": 0,
                "selected_action_ids": [],
                "selected_action_count": 0,
                "resolved_mission": stored_mission,
                "message": "Provider credentials still need to be configured and verified.",
                "mission_id": "msm_demo_scope",
            },
        }

    def model_setup_mission_launch(
        self,
        *,
        dry_run: bool = False,
        selected_action_ids: Optional[List[str]] = None,
        continue_on_error: bool = True,
        limit: int = 200,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        scope = self._record_model_setup_scope(
            "model_setup_mission_launch",
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        _ = selected_action_ids
        _ = continue_on_error
        _ = limit
        mission = self.model_setup_mission(
            manifest_path=scope["manifest_path"],
            workspace_root=scope["workspace_root"],
        )
        result_status = "planned" if dry_run else "success"
        item_status = "planned" if dry_run else "success"
        return {
            "status": result_status,
            "generated_at": "2026-03-15T08:02:00+00:00",
            "dry_run": bool(dry_run),
            "executed_count": 2,
            "skipped_count": 0,
            "error_count": 0,
            "items": [
                {
                    "action_id": "scaffold_workspace",
                    "kind": "scaffold_workspace",
                    "status": item_status,
                    "ok": True,
                },
                {
                    "action_id": "launch_setup_install:auto",
                    "kind": "launch_setup_install",
                    "status": item_status,
                    "ok": True,
                },
            ],
            "mission": mission,
            "updated_mission": mission,
            "workspace": self.model_setup_workspace(
                manifest_path=scope["manifest_path"],
                workspace_root=scope["workspace_root"],
            ),
            "setup_plan": mission["setup_plan"],
            "mission_record": mission["stored_mission"],
            "mission_history": mission["mission_history"],
            "resume_advice": mission["resume_advice"],
        }

    def model_setup_mission_history(
        self,
        *,
        limit: int = 20,
        mission_id: str = "",
        status: str = "",
        recovery_profile: str = "",
        current_scope: bool = True,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        scope = self._record_model_setup_scope(
            "model_setup_mission_history",
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        _ = limit
        _ = mission_id
        _ = status
        _ = recovery_profile
        _ = current_scope
        mission = self.model_setup_mission(
            manifest_path=scope["manifest_path"],
            workspace_root=scope["workspace_root"],
        )
        history = dict(mission["mission_history"])
        history["filters"] = {
            "manifest_path": scope["manifest_path"],
            "workspace_root": scope["workspace_root"],
        }
        return history

    def model_setup_mission_resume(
        self,
        *,
        mission_id: str = "",
        dry_run: bool = False,
        continue_on_error: bool = True,
        limit: int = 200,
        refresh_provider_credentials: bool = False,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        scope = self._record_model_setup_scope(
            "model_setup_mission_resume",
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        _ = mission_id
        _ = continue_on_error
        _ = limit
        _ = refresh_provider_credentials
        payload = self.model_setup_mission_launch(
            dry_run=dry_run,
            manifest_path=scope["manifest_path"],
            workspace_root=scope["workspace_root"],
        )
        payload["message"] = "resumed setup mission"
        payload["resolved_mission"] = self.model_setup_mission(
            manifest_path=scope["manifest_path"],
            workspace_root=scope["workspace_root"],
        )["stored_mission"]
        payload["resume_advice"] = self.model_setup_mission_resume_advice(
            mission_id=mission_id,
            limit=limit,
            refresh_provider_credentials=refresh_provider_credentials,
            current_scope=not bool(mission_id),
            manifest_path=scope["manifest_path"],
            workspace_root=scope["workspace_root"],
        )
        return payload

    def model_setup_mission_resume_advice(
        self,
        *,
        mission_id: str = "",
        limit: int = 200,
        refresh_provider_credentials: bool = False,
        current_scope: bool = True,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        scope = self._record_model_setup_scope(
            "model_setup_mission_resume_advice",
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        _ = limit
        _ = refresh_provider_credentials
        mission = self.model_setup_mission(
            manifest_path=scope["manifest_path"],
            workspace_root=scope["workspace_root"],
        )
        blocked = dict(mission["resume_advice"])
        blocked["mission"] = mission
        blocked["current_scope"] = bool(current_scope)
        blocked["mission_id"] = str(mission_id or blocked.get("mission_id") or "msm_demo_scope")
        if str(mission_id or "").strip() == "msm_auto_scope":
            resolved = mission["mission_history"]["latest_auto_resume_candidate"]
            return {
                "status": "ready",
                "can_resume_now": True,
                "can_auto_resume_now": True,
                "auto_resume_candidate": True,
                "resume_ready": True,
                "waiting_on_active_runs": False,
                "manual_attention_required": False,
                "recovery_profile": "resume_ready",
                "resume_trigger": "ready_now",
                "resume_blockers": [],
                "auto_resume_reason": "Auto-runnable setup actions are ready right now.",
                "recovery_hint": "Resume the next auto-runnable local-model setup actions.",
                "active_run_count": 0,
                "selected_action_ids": ["launch_setup_install:auto"],
                "selected_action_count": 1,
                "resolved_mission": resolved,
                "mission": mission,
                "message": "The stored model setup mission can resume immediately.",
                "current_scope": bool(current_scope),
                "mission_id": "msm_auto_scope",
            }
        return blocked

    def auto_resume_model_setup_mission(
        self,
        *,
        mission_id: str = "",
        dry_run: bool = False,
        continue_on_error: bool = True,
        limit: int = 200,
        refresh_provider_credentials: bool = False,
        current_scope: bool = True,
        continue_followup_actions: bool = True,
        max_followup_waves: int = 3,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        scope = self._record_model_setup_scope(
            "auto_resume_model_setup_mission",
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        advice = self.model_setup_mission_resume_advice(
            mission_id=mission_id,
            limit=limit,
            refresh_provider_credentials=refresh_provider_credentials,
            current_scope=current_scope,
            manifest_path=scope["manifest_path"],
            workspace_root=scope["workspace_root"],
        )
        if not bool(advice.get("can_auto_resume_now", False)):
            return {
                "status": str(advice.get("status", "") or "blocked"),
                "dry_run": bool(dry_run),
                "auto_resume_attempted": False,
                "auto_resume_triggered": False,
                "initial_resume_advice": advice,
                "resume_advice": advice,
                "continue_followup_actions_requested": bool(continue_followup_actions),
                "continue_followup_actions_status": "skipped",
                "continued_action_ids": [],
                "executed_action_ids": [],
                "continuation": {
                    "status": "skipped",
                    "enabled": bool(continue_followup_actions),
                    "max_waves": max(0, min(int(max_followup_waves), 8)),
                    "waves_executed": 0,
                    "continued_action_ids": [],
                    "stop_reason": "no_initial_auto_resume_actions",
                    "final_ready_action_ids": [],
                    "wave_summaries": [],
                },
                "message": str(advice.get("message", "") or "No auto-resumable setup actions are ready right now."),
                "resolved_mission": advice.get("resolved_mission", {}) if isinstance(advice.get("resolved_mission", {}), dict) else {},
                "mission": advice.get("mission", {}) if isinstance(advice.get("mission", {}), dict) else {},
            }
        payload = self.model_setup_mission_resume(
            mission_id=mission_id,
            dry_run=dry_run,
            continue_on_error=continue_on_error,
            limit=limit,
            refresh_provider_credentials=refresh_provider_credentials,
            manifest_path=scope["manifest_path"],
            workspace_root=scope["workspace_root"],
        )
        payload["auto_resume_attempted"] = True
        payload["auto_resume_triggered"] = True
        payload["initial_resume_advice"] = advice
        payload["continued_action_ids"] = ["verify_provider:huggingface"] if bool(continue_followup_actions) else []
        payload["executed_action_ids"] = [
            *[str(item).strip() for item in payload.get("selected_action_ids", []) if str(item).strip()],
            *payload["continued_action_ids"],
        ]
        payload["continue_followup_actions_requested"] = bool(continue_followup_actions)
        payload["continue_followup_actions_status"] = "success" if bool(continue_followup_actions) else "skipped"
        payload["continuation"] = {
            "status": "success" if bool(continue_followup_actions) else "skipped",
            "enabled": bool(continue_followup_actions),
            "max_waves": max(0, min(int(max_followup_waves), 8)),
            "waves_executed": 1 if bool(continue_followup_actions) else 0,
            "continued_action_ids": payload["continued_action_ids"],
            "stop_reason": "no_ready_followup_actions" if bool(continue_followup_actions) else "disabled",
            "final_ready_action_ids": [],
            "wave_summaries": [
                {
                    "wave": 1,
                    "status": "success",
                    "selected_action_ids": payload["continued_action_ids"],
                    "executed_count": len(payload["continued_action_ids"]),
                    "skipped_count": 0,
                    "error_count": 0,
                    "message": "continued follow-up setup actions",
                }
            ] if bool(continue_followup_actions) else [],
        }
        payload["resume_advice"] = {
            **advice,
            "status": "idle",
            "can_resume_now": False,
            "can_auto_resume_now": False,
            "auto_resume_candidate": False,
            "selected_action_ids": [],
            "selected_action_count": 0,
            "message": "No additional auto-resumable setup actions are ready right now.",
        }
        payload["message"] = "auto-resumed setup mission and continued follow-up actions"
        return payload

    def model_setup_mission_recovery_sweep(
        self,
        *,
        mission_id: str = "",
        dry_run: bool = False,
        continue_on_error: bool = True,
        limit: int = 200,
        refresh_provider_credentials: bool = False,
        current_scope: bool = True,
        max_auto_resume_passes: int = 3,
        continue_followup_actions: bool = True,
        max_followup_waves: int = 3,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        scope = self._record_model_setup_scope(
            "model_setup_mission_recovery_sweep",
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        advice = self.model_setup_mission_resume_advice(
            mission_id=mission_id,
            limit=limit,
            refresh_provider_credentials=refresh_provider_credentials,
            current_scope=current_scope,
            manifest_path=scope["manifest_path"],
            workspace_root=scope["workspace_root"],
        )
        if not bool(advice.get("can_auto_resume_now", False)):
            return {
                "status": str(advice.get("status", "") or "idle"),
                "message": str(advice.get("message", "") or "No auto-resumable setup actions are ready right now."),
                "mission_id": str(mission_id or advice.get("mission_id") or "").strip(),
                "dry_run": bool(dry_run),
                "current_scope": bool(current_scope),
                "continue_on_error": bool(continue_on_error),
                "max_auto_resume_passes": max(1, min(int(max_auto_resume_passes), 8)),
                "continue_followup_actions_requested": bool(continue_followup_actions),
                "max_followup_waves": max(0, min(int(max_followup_waves), 8)),
                "history_before": self.model_setup_mission_history(limit=12, current_scope=current_scope),
                "history_after": self.model_setup_mission_history(limit=12, current_scope=current_scope),
                "initial_resume_advice": advice,
                "final_resume_advice": advice,
                "passes": [
                    {
                        "pass": 1,
                        "mission_id": str(mission_id or advice.get("mission_id") or "").strip(),
                        "advice_status": str(advice.get("status", "") or "idle"),
                        "can_resume_now": bool(advice.get("can_resume_now", False)),
                        "can_auto_resume_now": False,
                        "resume_trigger": str(advice.get("resume_trigger", "") or "").strip(),
                        "resume_blockers": list(advice.get("resume_blockers", [])) if isinstance(advice.get("resume_blockers", []), list) else [],
                        "selected_action_ids": [],
                        "message": str(advice.get("message", "") or "").strip(),
                        "status": str(advice.get("status", "") or "idle"),
                    }
                ],
                "passes_executed": 0,
                "auto_resume_attempted_count": 0,
                "auto_resume_triggered_count": 0,
                "continued_action_ids": [],
                "executed_action_ids": [],
                "executed_count": 0,
                "skipped_count": 0,
                "error_count": 0,
                "stop_reason": "no_auto_resume_candidate",
                "final_payload": {},
            }
        payload = self.auto_resume_model_setup_mission(
            mission_id=mission_id,
            dry_run=dry_run,
            continue_on_error=continue_on_error,
            limit=limit,
            refresh_provider_credentials=refresh_provider_credentials,
            current_scope=current_scope,
            continue_followup_actions=continue_followup_actions,
            max_followup_waves=max_followup_waves,
            manifest_path=scope["manifest_path"],
            workspace_root=scope["workspace_root"],
        )
        return {
            "status": "planned" if bool(dry_run) else "success",
            "message": "Recovery sweep auto-resumed the stored setup mission.",
            "mission_id": str(mission_id or advice.get("mission_id") or "msm_auto_scope").strip(),
            "dry_run": bool(dry_run),
            "current_scope": bool(current_scope),
            "continue_on_error": bool(continue_on_error),
            "max_auto_resume_passes": max(1, min(int(max_auto_resume_passes), 8)),
            "continue_followup_actions_requested": bool(continue_followup_actions),
            "max_followup_waves": max(0, min(int(max_followup_waves), 8)),
            "history_before": self.model_setup_mission_history(
                limit=12,
                current_scope=current_scope,
                manifest_path=scope["manifest_path"],
                workspace_root=scope["workspace_root"],
            ),
            "history_after": self.model_setup_mission_history(
                limit=12,
                current_scope=current_scope,
                manifest_path=scope["manifest_path"],
                workspace_root=scope["workspace_root"],
            ),
            "initial_resume_advice": advice,
            "final_resume_advice": payload.get("resume_advice", {}) if isinstance(payload.get("resume_advice", {}), dict) else {},
            "passes": [
                {
                    "pass": 1,
                    "mission_id": str(mission_id or advice.get("mission_id") or "msm_auto_scope").strip(),
                    "advice_status": str(advice.get("status", "") or "ready"),
                    "can_resume_now": bool(advice.get("can_resume_now", False)),
                    "can_auto_resume_now": bool(advice.get("can_auto_resume_now", False)),
                    "resume_trigger": str(advice.get("resume_trigger", "") or "").strip(),
                    "resume_blockers": list(advice.get("resume_blockers", [])) if isinstance(advice.get("resume_blockers", []), list) else [],
                    "selected_action_ids": list(advice.get("selected_action_ids", [])) if isinstance(advice.get("selected_action_ids", []), list) else [],
                    "continued_action_ids": list(payload.get("continued_action_ids", [])) if isinstance(payload.get("continued_action_ids", []), list) else [],
                    "auto_resume_triggered": bool(payload.get("auto_resume_triggered", False)),
                    "continue_followup_actions_status": str(payload.get("continue_followup_actions_status", "") or "skipped"),
                    "executed_count": int(payload.get("executed_count", 0) or 0),
                    "skipped_count": int(payload.get("skipped_count", 0) or 0),
                    "error_count": int(payload.get("error_count", 0) or 0),
                    "status": str(payload.get("status", "") or "success"),
                    "message": str(payload.get("message", "") or "").strip(),
                }
            ],
            "passes_executed": 1,
            "auto_resume_attempted_count": 1,
            "auto_resume_triggered_count": 1 if bool(payload.get("auto_resume_triggered", False)) else 0,
            "continued_action_ids": list(payload.get("continued_action_ids", [])) if isinstance(payload.get("continued_action_ids", []), list) else [],
            "executed_action_ids": list(payload.get("executed_action_ids", [])) if isinstance(payload.get("executed_action_ids", []), list) else [],
            "executed_count": int(payload.get("executed_count", 0) or 0),
            "skipped_count": int(payload.get("skipped_count", 0) or 0),
            "error_count": int(payload.get("error_count", 0) or 0),
            "stop_reason": "no_auto_resume_candidate",
            "final_payload": payload,
        }

    def model_setup_mission_recovery_watchdog(
        self,
        *,
        mission_id: str = "",
        dry_run: bool = False,
        continue_on_error: bool = True,
        limit: int = 200,
        refresh_provider_credentials: bool = False,
        current_scope: bool = True,
        max_missions: int = 6,
        max_auto_resumes: int = 2,
        continue_followup_actions: bool = True,
        max_followup_waves: int = 3,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        scope = self._record_model_setup_scope(
            "model_setup_mission_recovery_watchdog",
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        _ = mission_id
        _ = dry_run
        _ = continue_on_error
        _ = limit
        _ = refresh_provider_credentials
        _ = current_scope
        _ = max_missions
        _ = max_auto_resumes
        _ = continue_followup_actions
        _ = max_followup_waves
        payload = self.auto_resume_model_setup_mission(
            mission_id="msm_auto_scope",
            dry_run=False,
            continue_on_error=True,
            limit=200,
            current_scope=False,
            continue_followup_actions=True,
            max_followup_waves=3,
            manifest_path=scope["manifest_path"],
            workspace_root=scope["workspace_root"],
        )
        watchdog_run = {
            "run_id": "mswd_demo",
            "workspace_root": scope["workspace_root"],
            "manifest_path": scope["manifest_path"],
            "scope_label": "J.A.R.V.I.S::Models to Download.txt",
            "status": "success",
            "message": "Recovery watchdog auto-resumed 1 stored mission.",
            "source": "watchdog",
            "dry_run": False,
            "current_scope": bool(current_scope),
            "continue_on_error": bool(continue_on_error),
            "continue_followup_actions_requested": bool(continue_followup_actions),
            "max_missions": max(1, min(int(max_missions), 64)),
            "max_auto_resumes": max(0, min(int(max_auto_resumes), 64)),
            "max_followup_waves": max(0, min(int(max_followup_waves), 8)),
            "evaluated_count": 1,
            "auto_resume_attempted_count": 1,
            "auto_resume_triggered_count": 1,
            "ready_count": 0,
            "watch_count": 0,
            "stalled_count": 0,
            "blocked_count": 0,
            "idle_count": 1,
            "complete_count": 0,
            "error_count": 0,
            "triggered_mission_ids": ["msm_auto_scope"],
            "ready_mission_ids": [],
            "watched_mission_ids": [],
            "stalled_mission_ids": [],
            "blocked_mission_ids": [],
            "scope_counts": {"J.A.R.V.I.S::Models to Download.txt": 1},
            "latest_triggered_scope": scope,
            "latest_triggered_scope_label": "J.A.R.V.I.S::Models to Download.txt",
            "latest_triggered_status": "success",
            "latest_triggered_message": str(payload.get("message", "") or "auto-resumed setup mission"),
            "latest_triggered_mission_id": "msm_auto_scope",
            "stop_reason": "auto_resume_triggered",
            "created_at": "2026-03-15T10:05:00+00:00",
            "updated_at": "2026-03-15T10:05:00+00:00",
        }
        watchdog_history = self.model_setup_recovery_watchdog_history(
            limit=12,
            current_scope=current_scope,
            manifest_path=scope["manifest_path"],
            workspace_root=scope["workspace_root"],
        )
        return {
            "status": "success",
            "message": "Recovery watchdog auto-resumed 1 stored mission.",
            "mission_id": "msm_auto_scope",
            "dry_run": False,
            "current_scope": bool(current_scope),
            "continue_on_error": bool(continue_on_error),
            "max_missions": max(1, min(int(max_missions), 64)),
            "max_auto_resumes": max(0, min(int(max_auto_resumes), 64)),
            "continue_followup_actions_requested": bool(continue_followup_actions),
            "max_followup_waves": max(0, min(int(max_followup_waves), 8)),
            "history_before": self.model_setup_mission_history(
                limit=12,
                current_scope=current_scope,
                manifest_path=scope["manifest_path"],
                workspace_root=scope["workspace_root"],
            ),
            "history_after": self.model_setup_mission_history(
                limit=12,
                current_scope=current_scope,
                manifest_path=scope["manifest_path"],
                workspace_root=scope["workspace_root"],
            ),
            "results": [
                {
                    "mission_id": "msm_auto_scope",
                    "scope": scope,
                    "scope_label": "J.A.R.V.I.S::Models to Download.txt",
                    "stored_status": "running",
                    "classification_before": "ready",
                    "classification_after": "idle",
                    "advice_status": "ready",
                    "status": "success",
                    "can_resume_now": True,
                    "can_auto_resume_now": True,
                    "auto_resume_candidate": True,
                    "auto_resume_attempted": True,
                    "auto_resume_triggered": True,
                    "message": "Ready to continue immediately.",
                    "result_message": str(payload.get("message", "") or "auto-resumed setup mission"),
                }
            ],
            "evaluated_count": 1,
            "auto_resume_attempted_count": 1,
            "auto_resume_triggered_count": 1,
            "ready_count": 0,
            "watch_count": 0,
            "stalled_count": 0,
            "blocked_count": 0,
            "idle_count": 1,
            "complete_count": 0,
            "error_count": 0,
            "ready_mission_ids": [],
            "watched_mission_ids": [],
            "stalled_mission_ids": [],
            "blocked_mission_ids": [],
            "triggered_mission_ids": ["msm_auto_scope"],
            "scope_counts": {"J.A.R.V.I.S::Models to Download.txt": 1},
            "latest_triggered_payload": payload,
            "watchdog_run": watchdog_run,
            "watchdog_history": watchdog_history,
            "stop_reason": "auto_resume_triggered",
        }

    def model_setup_recovery_watchdog_history(
        self,
        *,
        limit: int = 20,
        status: str = "",
        current_scope: bool = True,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        scope = self._record_model_setup_scope(
            "model_setup_recovery_watchdog_history",
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        _ = limit
        _ = status
        _ = current_scope
        item = {
            "run_id": "mswd_demo",
            "workspace_root": scope["workspace_root"],
            "manifest_path": scope["manifest_path"],
            "scope_label": "J.A.R.V.I.S::Models to Download.txt",
            "status": "success",
            "message": "Recovery watchdog auto-resumed 1 stored mission.",
            "auto_resume_triggered_count": 1,
            "watch_count": 0,
            "stalled_count": 0,
            "updated_at": "2026-03-15T10:05:00+00:00",
        }
        return {
            "status": "success",
            "count": 1,
            "total": 1,
            "items": [item],
            "triggered_run_count": 1,
            "watch_run_count": 0,
            "stalled_run_count": 0,
            "error_run_count": 0,
            "latest_run": item,
            "latest_triggered_run": item,
            "filters": {
                "workspace_root": scope["workspace_root"],
                "manifest_path": scope["manifest_path"],
            },
        }

    def reset_model_setup_recovery_watchdog_history(
        self,
        *,
        run_id: str = "",
        status: str = "",
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        scope = self._record_model_setup_scope(
            "reset_model_setup_recovery_watchdog_history",
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        return {
            "status": "success",
            "removed": 1,
            "filters": {
                "run_id": str(run_id or "").strip(),
                "status": str(status or "").strip(),
                "workspace_root": scope["workspace_root"],
                "manifest_path": scope["manifest_path"],
            },
        }

    def model_setup_recovery_watchdog_supervisor_status(
        self,
        *,
        history_limit: int = 6,
    ) -> Dict[str, Any]:
        _ = history_limit
        history = self.model_setup_recovery_watchdog_history(
            limit=6,
            current_scope=False,
            manifest_path="E:/scopes/mission/JARVIS_BACKEND/Models_manifest.txt",
            workspace_root="E:/scopes/mission",
        )
        return {
            "status": "success",
            "active": True,
            "enabled": True,
            "inflight": False,
            "interval_s": 45.0,
            "current_scope": False,
            "manifest_path": "E:/scopes/mission/JARVIS_BACKEND/Models_manifest.txt",
            "workspace_root": "E:/scopes/mission",
            "max_missions": 6,
            "max_auto_resumes": 2,
            "continue_followup_actions": True,
            "max_followup_waves": 3,
            "last_tick_at": "2026-03-15T10:05:00+00:00",
            "last_result_status": "success",
            "last_result_message": "Recovery watchdog auto-resumed 1 stored mission.",
            "last_trigger_source": "daemon",
            "run_count": 3,
            "manual_trigger_count": 1,
            "auto_trigger_count": 2,
            "consecutive_error_count": 0,
            "watchdog_history": history,
        }

    def configure_model_setup_recovery_watchdog_supervisor(
        self,
        *,
        enabled: Optional[bool] = None,
        interval_s: Optional[float] = None,
        max_missions: Optional[int] = None,
        max_auto_resumes: Optional[int] = None,
        continue_followup_actions: Optional[bool] = None,
        max_followup_waves: Optional[int] = None,
        current_scope: Optional[bool] = None,
        manifest_path: Optional[str] = None,
        workspace_root: Optional[str] = None,
        history_limit: int = 6,
    ) -> Dict[str, Any]:
        _ = max_missions
        _ = max_auto_resumes
        _ = continue_followup_actions
        _ = max_followup_waves
        _ = current_scope
        scope = self._record_model_setup_scope(
            "configure_model_setup_recovery_watchdog_supervisor",
            manifest_path=str(manifest_path or ""),
            workspace_root=str(workspace_root or ""),
        )
        payload = self.model_setup_recovery_watchdog_supervisor_status(history_limit=history_limit)
        payload.update(
            {
                "enabled": bool(enabled) if enabled is not None else True,
                "interval_s": float(interval_s) if interval_s is not None else 45.0,
                "manifest_path": scope["manifest_path"] or payload["manifest_path"],
                "workspace_root": scope["workspace_root"] or payload["workspace_root"],
            }
        )
        return payload

    def trigger_model_setup_recovery_watchdog_supervisor(
        self,
        *,
        dry_run: Optional[bool] = None,
        current_scope: Optional[bool] = None,
        manifest_path: Optional[str] = None,
        workspace_root: Optional[str] = None,
        max_missions: Optional[int] = None,
        max_auto_resumes: Optional[int] = None,
        continue_followup_actions: Optional[bool] = None,
        max_followup_waves: Optional[int] = None,
        history_limit: int = 6,
    ) -> Dict[str, Any]:
        payload = self.model_setup_mission_recovery_watchdog(
            current_scope=bool(current_scope) if current_scope is not None else False,
            max_missions=int(max_missions) if max_missions is not None else 6,
            max_auto_resumes=int(max_auto_resumes) if max_auto_resumes is not None else 2,
            continue_followup_actions=bool(continue_followup_actions) if continue_followup_actions is not None else True,
            max_followup_waves=int(max_followup_waves) if max_followup_waves is not None else 3,
            manifest_path=str(manifest_path or ""),
            workspace_root=str(workspace_root or ""),
            dry_run=bool(dry_run) if dry_run is not None else False,
        )
        return {
            "status": "success",
            "message": "daemon tick executed",
            "result": payload,
            "supervisor": self.model_setup_recovery_watchdog_supervisor_status(history_limit=history_limit),
        }

    def model_setup_manual_pipeline(
        self,
        *,
        task: str = "",
        limit: int = 200,
        include_present: bool = False,
        item_keys: Optional[List[str]] = None,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        scope = self._record_model_setup_scope(
            "model_setup_manual_pipeline",
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        _ = limit
        _ = include_present
        return {
            "status": "success",
            "task": task,
            "selected_item_keys": list(item_keys or []),
            "manifest_path": scope["manifest_path"],
            "workspace_root": scope["workspace_root"],
            "summary": {"ready_count": 1, "blocked_count": 0},
            "items": [
                {
                    "key": "manual-convert-qwen",
                    "name": "Convert Qwen artifact",
                    "task": task or "reasoning",
                    "status": "ready",
                }
            ],
            "setup_plan": {"status": "success"},
        }

    def model_setup_manual_run_launch(
        self,
        *,
        task: str = "",
        item_keys: Optional[List[str]] = None,
        dry_run: bool = False,
        force: bool = False,
        limit: int = 200,
        step_ids: Optional[List[str]] = None,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        scope = self._record_model_setup_scope(
            "model_setup_manual_run_launch",
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        _ = limit
        run = self._build_model_setup_manual_run(
            task=task or "reasoning",
            manifest_path=scope["manifest_path"],
            workspace_root=scope["workspace_root"],
            status="planned" if dry_run else "success",
            run_id="manual-run-scope",
        )
        run["selected_item_keys"] = list(item_keys or ["manual-convert-qwen"])
        run["selected_step_ids"] = list(step_ids or [])
        run["dry_run"] = bool(dry_run)
        run["force"] = bool(force)
        return {
            "status": "planned" if dry_run else "success",
            "task": task,
            "selected_item_keys": list(item_keys or []),
            "selected_step_ids": list(step_ids or []),
            "dry_run": bool(dry_run),
            "force": bool(force),
            "run": run,
            "manual_pipeline": self.model_setup_manual_pipeline(
                task=task,
                item_keys=item_keys,
                manifest_path=scope["manifest_path"],
                workspace_root=scope["workspace_root"],
            ),
        }

    def model_setup_manual_runs(
        self,
        *,
        limit: int = 20,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        scope = self._record_model_setup_scope(
            "model_setup_manual_runs",
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        run = self._build_model_setup_manual_run(
            task="reasoning",
            manifest_path=scope["manifest_path"],
            workspace_root=scope["workspace_root"],
            run_id="manual-run-scope",
        )
        return {
            "status": "success",
            "count": 1,
            "total": 1,
            "active_count": 0,
            "items": [run][: max(1, int(limit))],
            "filters": {
                "manifest_path": scope["manifest_path"],
                "workspace_root": scope["workspace_root"],
            },
        }

    def model_setup_install(
        self,
        *,
        task: str = "",
        item_keys: Optional[List[str]] = None,
        dry_run: bool = False,
        force: bool = False,
        include_present: bool = False,
        limit: int = 200,
        refresh_remote: bool = False,
        remote_timeout_s: float = 6.0,
        verify_integrity: bool = False,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        scope = self._record_model_setup_scope(
            "model_setup_install",
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        _ = limit
        _ = force
        _ = include_present
        _ = refresh_remote
        _ = remote_timeout_s
        _ = verify_integrity
        run = self._build_model_setup_install_run(
            task=task or "reasoning",
            manifest_path=scope["manifest_path"],
            workspace_root=scope["workspace_root"],
            status="planned" if dry_run else "success",
            run_id="install-run-scope",
        )
        run["requested_item_keys"] = list(item_keys or ["reasoning-llama"])
        run["selected_item_keys"] = list(item_keys or ["reasoning-llama"])
        run["dry_run"] = bool(dry_run)
        return {
            "status": "success",
            "task": task,
            "dry_run": bool(dry_run),
            "install": run,
            "preflight": {
                "status": "success",
                "summary": {"launchable_count": len(item_keys or ["reasoning-llama"])},
                "manifest_path": scope["manifest_path"],
                "workspace_root": scope["workspace_root"],
            },
            "setup_plan": {"status": "success"},
        }

    def model_setup_install_launch(
        self,
        *,
        task: str = "",
        item_keys: Optional[List[str]] = None,
        dry_run: bool = False,
        force: bool = False,
        include_present: bool = False,
        limit: int = 200,
        refresh_remote: bool = False,
        remote_timeout_s: float = 6.0,
        verify_integrity: bool = False,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        scope = self._record_model_setup_scope(
            "model_setup_install_launch",
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        _ = limit
        _ = force
        _ = include_present
        _ = refresh_remote
        _ = remote_timeout_s
        _ = verify_integrity
        run = self._build_model_setup_install_run(
            task=task or "reasoning",
            manifest_path=scope["manifest_path"],
            workspace_root=scope["workspace_root"],
            status="planned" if dry_run else "success",
            run_id="install-run-scope",
        )
        run["requested_item_keys"] = list(item_keys or ["reasoning-llama"])
        run["selected_item_keys"] = list(item_keys or ["reasoning-llama"])
        run["dry_run"] = bool(dry_run)
        return {
            "status": "planned" if dry_run else "success",
            "task": task,
            "dry_run": bool(dry_run),
            "selected_item_keys": list(item_keys or ["reasoning-llama"]),
            "requested_item_keys": list(item_keys or ["reasoning-llama"]),
            "launch_item_keys": list(item_keys or ["reasoning-llama"]),
            "launch_scope": "full",
            "launchable_count": len(item_keys or ["reasoning-llama"]),
            "deferred_count": 0,
            "run": run,
            "setup_plan": {"status": "success"},
            "preflight": {
                "status": "success",
                "summary": {"launchable_count": len(item_keys or ["reasoning-llama"])},
                "manifest_path": scope["manifest_path"],
                "workspace_root": scope["workspace_root"],
            },
        }

    def model_setup_install_history(
        self,
        *,
        limit: int = 20,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        scope = self._record_model_setup_scope(
            "model_setup_install_history",
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        run = self._build_model_setup_install_run(
            task="reasoning",
            manifest_path=scope["manifest_path"],
            workspace_root=scope["workspace_root"],
            run_id="install-run-scope",
        )
        return {
            "status": "success",
            "count": 1,
            "total": 1,
            "active_count": 0,
            "items": [run][: max(1, int(limit))],
            "history_path": f"{scope['workspace_root']}/data/model_setup_install_history.jsonl",
            "filters": {
                "manifest_path": scope["manifest_path"],
                "workspace_root": scope["workspace_root"],
            },
        }

    def model_setup_install_runs(
        self,
        *,
        limit: int = 20,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        scope = self._record_model_setup_scope(
            "model_setup_install_runs",
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        history = self.model_setup_install_history(
            limit=limit,
            manifest_path=scope["manifest_path"],
            workspace_root=scope["workspace_root"],
        )
        history["filters"] = {
            "manifest_path": scope["manifest_path"],
            "workspace_root": scope["workspace_root"],
        }
        return history

    def reset_model_setup_mission(self, *, mission_id: str = "", status: str = "") -> Dict[str, Any]:
        _ = mission_id
        _ = status
        return {"status": "success", "removed": 1, "filters": {"mission_id": mission_id, "status": status}}

    def update_provider_credentials(
        self,
        *,
        provider: str,
        api_key: str = "",
        requirements: Optional[Dict[str, Any]] = None,
        persist_plaintext: bool = True,
        persist_encrypted: Optional[bool] = None,
        overwrite_env: bool = True,
        clear_api_key: bool = False,
        verify_after_update: bool = False,
        task: str = "",
        limit: int = 160,
        include_present: bool = False,
        item_keys: Optional[List[str]] = None,
        continue_setup_recovery: bool = False,
        continue_on_error: bool = True,
        continue_followup_actions: bool = True,
        max_followup_waves: int = 3,
        include_coworker_status: bool = False,
        refresh_remote: bool = False,
        timeout_s: float = 8.0,
    ) -> Dict[str, Any]:
        self.provider_update_calls.append(
            {
                "provider": provider,
                "api_key": api_key,
                "requirements": dict(requirements or {}),
                "persist_plaintext": persist_plaintext,
                "persist_encrypted": persist_encrypted,
                "overwrite_env": overwrite_env,
                "clear_api_key": clear_api_key,
                "verify_after_update": verify_after_update,
                "task": task,
                "limit": limit,
                "include_present": include_present,
                "item_keys": list(item_keys or []),
                "continue_setup_recovery": continue_setup_recovery,
                "continue_on_error": continue_on_error,
                "continue_followup_actions": continue_followup_actions,
                "max_followup_waves": max_followup_waves,
                "include_coworker_status": include_coworker_status,
                "refresh_remote": refresh_remote,
                "timeout_s": timeout_s,
            }
        )
        return {
            "status": "success",
            "provider": provider,
            "verification_requested": bool(verify_after_update),
            "verification_status": "success" if verify_after_update else "skipped",
            "verification": {
                "verified": bool(verify_after_update),
                "summary": f"verified {provider}" if verify_after_update else f"saved {provider}",
            },
            "affected_item_keys": list(item_keys or []),
            "affected_tasks": [task] if str(task or "").strip() else [],
            "setup_recovery": {
                "launchable_count": len(item_keys or []),
                "ready_action_count": 1 if item_keys else 0,
                "auto_runnable_ready_action_ids": ["install:reasoning-llama"] if item_keys else [],
                "next_action": {
                    "kind": "launch_setup_install",
                    "title": "Install ready setup items",
                },
            },
            "continue_setup_recovery_requested": continue_setup_recovery,
            "continue_setup_recovery_status": "success" if continue_setup_recovery else "skipped",
            "continue_followup_actions_requested": bool(continue_setup_recovery and continue_followup_actions),
            "continue_followup_actions_status": "success" if continue_setup_recovery and continue_followup_actions else "skipped",
            "recovery_launch": {
                "status": "success" if continue_setup_recovery else "skipped",
                "executed_count": 1 if continue_setup_recovery and item_keys else 0,
                "selected_action_ids": ["install:reasoning-llama"] if continue_setup_recovery and item_keys else [],
                "continued_action_ids": ["launch_setup_install:auto"] if continue_setup_recovery and continue_followup_actions and item_keys else [],
                "continue_followup_actions_requested": bool(continue_setup_recovery and continue_followup_actions),
                "continue_followup_actions_status": "success" if continue_setup_recovery and continue_followup_actions else "skipped",
                "continuation": {
                    "status": "success" if continue_setup_recovery and continue_followup_actions and item_keys else "skipped",
                    "waves_executed": 1 if continue_setup_recovery and continue_followup_actions and item_keys else 0,
                },
                "setup_recovery": {
                    "launchable_count": len(item_keys or []),
                    "auto_runnable_ready_count": 1 if continue_setup_recovery and item_keys else 0,
                    "auto_runnable_ready_action_ids": ["install:reasoning-llama"] if continue_setup_recovery and item_keys else [],
                    "next_action": {
                        "kind": "launch_setup_install",
                        "title": "Install ready setup items",
                    },
                },
            },
            "workspace": {"status": "success"},
            "setup_plan": {"status": "success"},
            "preflight": {"status": "success", "summary": {"launchable_count": len(item_keys or [])}},
            "manual_pipeline": {"status": "success"},
            "mission": {"status": "success"},
            "updated_mission": {"status": "success"},
            "inventory": {"status": "success", "items": []},
            "provider_credentials": {"providers": {provider: {"ready": True, "present": True}}},
            "coworker_stack": {"status": "success"},
            "coworker_recovery": {"status": "success"},
        }

    def verify_provider_credentials(
        self,
        *,
        provider: str,
        task: str = "",
        limit: int = 160,
        include_present: bool = False,
        item_keys: Optional[List[str]] = None,
        force_refresh: bool = True,
        timeout_s: float = 8.0,
        continue_setup_recovery: bool = False,
        continue_on_error: bool = True,
        continue_followup_actions: bool = True,
        max_followup_waves: int = 3,
        include_coworker_status: bool = False,
        refresh_remote: bool = False,
    ) -> Dict[str, Any]:
        self.provider_verify_calls.append(
            {
                "provider": provider,
                "task": task,
                "limit": limit,
                "include_present": include_present,
                "item_keys": list(item_keys or []),
                "force_refresh": force_refresh,
                "timeout_s": timeout_s,
                "continue_setup_recovery": continue_setup_recovery,
                "continue_on_error": continue_on_error,
                "continue_followup_actions": continue_followup_actions,
                "max_followup_waves": max_followup_waves,
                "include_coworker_status": include_coworker_status,
                "refresh_remote": refresh_remote,
            }
        )
        return {
            "status": "success",
            "provider": provider,
            "task": task,
            "verification": {
                "verified": True,
                "summary": f"verified {provider}",
            },
            "affected_item_keys": list(item_keys or []),
            "affected_tasks": [task] if str(task or "").strip() else [],
            "setup_recovery": {
                "launchable_count": len(item_keys or []),
                "auto_runnable_ready_action_ids": ["install:reasoning-llama"] if item_keys else [],
                "next_action": {
                    "kind": "launch_setup_install",
                    "title": "Install ready setup items",
                },
            },
            "continue_setup_recovery_requested": continue_setup_recovery,
            "continue_setup_recovery_status": "success" if continue_setup_recovery else "skipped",
            "continue_followup_actions_requested": bool(continue_setup_recovery and continue_followup_actions),
            "continue_followup_actions_status": "success" if continue_setup_recovery and continue_followup_actions else "skipped",
            "recovery_launch": {
                "status": "success" if continue_setup_recovery else "skipped",
                "executed_count": 1 if continue_setup_recovery and item_keys else 0,
                "selected_action_ids": ["install:reasoning-llama"] if continue_setup_recovery and item_keys else [],
                "continued_action_ids": ["launch_setup_install:auto"] if continue_setup_recovery and continue_followup_actions and item_keys else [],
                "continue_followup_actions_requested": bool(continue_setup_recovery and continue_followup_actions),
                "continue_followup_actions_status": "success" if continue_setup_recovery and continue_followup_actions else "skipped",
                "continuation": {
                    "status": "success" if continue_setup_recovery and continue_followup_actions and item_keys else "skipped",
                    "waves_executed": 1 if continue_setup_recovery and continue_followup_actions and item_keys else 0,
                },
                "setup_recovery": {
                    "launchable_count": len(item_keys or []),
                    "auto_runnable_ready_count": 1 if continue_setup_recovery and item_keys else 0,
                    "auto_runnable_ready_action_ids": ["install:reasoning-llama"] if continue_setup_recovery and item_keys else [],
                    "next_action": {
                        "kind": "launch_setup_install",
                        "title": "Install ready setup items",
                    },
                },
            },
            "workspace": {"status": "success"},
            "setup_plan": {"status": "success"},
            "preflight": {"status": "success", "summary": {"launchable_count": len(item_keys or [])}},
            "manual_pipeline": {"status": "success"},
            "mission": {"status": "success"},
            "updated_mission": {"status": "success"},
            "inventory": {"status": "success", "items": []},
            "provider_credentials": {"providers": {provider: {"ready": True, "present": True}}},
            "coworker_stack": {"status": "success"},
            "coworker_recovery": {"status": "success"},
        }

    def provider_setup_recovery_launch(
        self,
        *,
        provider: str,
        task: str = "",
        limit: int = 160,
        include_present: bool = False,
        item_keys: Optional[List[str]] = None,
        selected_action_ids: Optional[List[str]] = None,
        dry_run: bool = False,
        continue_on_error: bool = True,
        continue_followup_actions: bool = True,
        max_followup_waves: int = 3,
        refresh_provider_credentials: bool = False,
        refresh_remote: bool = False,
        timeout_s: float = 8.0,
    ) -> Dict[str, Any]:
        self.provider_recovery_calls.append(
            {
                "provider": provider,
                "task": task,
                "limit": limit,
                "include_present": include_present,
                "item_keys": list(item_keys or []),
                "selected_action_ids": list(selected_action_ids or []),
                "dry_run": dry_run,
                "continue_on_error": continue_on_error,
                "continue_followup_actions": continue_followup_actions,
                "max_followup_waves": max_followup_waves,
                "refresh_provider_credentials": refresh_provider_credentials,
                "refresh_remote": refresh_remote,
                "timeout_s": timeout_s,
            }
        )
        selected_ids = list(selected_action_ids or ["install:reasoning-llama"])
        return {
            "status": "planned" if dry_run else "success",
            "provider": provider,
            "task": task,
            "dry_run": dry_run,
            "executed_count": 0 if dry_run else len(selected_ids),
            "skipped_count": 0,
            "error_count": 0,
            "selected_action_ids": selected_ids,
            "auto_selected_action_ids": ["install:reasoning-llama"],
            "requested_action_ids": list(selected_action_ids or []),
            "ignored_action_ids": [],
            "continue_followup_actions_requested": continue_followup_actions,
            "continue_followup_actions_status": "planned" if dry_run and continue_followup_actions else "success" if continue_followup_actions else "skipped",
            "continued_action_ids": ["launch_setup_install:auto"] if continue_followup_actions and not dry_run else [],
            "executed_action_ids": selected_ids + (["launch_setup_install:auto"] if continue_followup_actions and not dry_run else []),
            "continuation": {
                "status": "planned" if dry_run and continue_followup_actions else "success" if continue_followup_actions else "skipped",
                "enabled": continue_followup_actions,
                "waves_executed": 1 if continue_followup_actions else 0,
                "continued_action_ids": ["launch_setup_install:auto"] if continue_followup_actions and not dry_run else [],
                "max_waves": max_followup_waves,
            },
            "affected_item_keys": list(item_keys or []),
            "affected_tasks": [task] if str(task or "").strip() else [],
            "setup_recovery": {
                "launchable_count": len(item_keys or []),
                "auto_runnable_ready_count": 1,
                "auto_runnable_ready_action_ids": ["install:reasoning-llama"],
                "next_action": {
                    "kind": "launch_setup_install",
                    "title": "Install ready setup items",
                },
            },
            "workspace": {"status": "success"},
            "setup_plan": {"status": "success"},
            "preflight": {"status": "success", "summary": {"launchable_count": len(item_keys or [])}},
            "manual_pipeline": {"status": "success"},
            "mission": {"status": "success"},
            "updated_mission": {"status": "success"},
            "inventory": {"status": "success", "items": []},
            "provider_credentials": {"providers": {provider: {"ready": True, "present": True}}},
            "provider_setup": {"provider": provider, "ready": True, "present": True},
            "coworker_stack": {"status": "success"},
            "coworker_recovery": {"status": "success"},
            "message": f"executed provider recovery for {provider}",
        }

    def _launch_history_rows(self, *, profile_id: str, template_id: str) -> list[Dict[str, Any]]:
        clean_profile_id = str(profile_id or "").strip().lower()
        clean_template_id = str(template_id or "").strip().lower()
        return [
            dict(item)
            for item in self.launch_template_history
            if isinstance(item, dict)
            and str(item.get("profile_id", "")).strip().lower() == clean_profile_id
            and (
                str(item.get("template_id", "")).strip().lower() == clean_template_id
                or str(item.get("requested_template_id", "")).strip().lower() == clean_template_id
            )
        ]

    def _launch_template_health(
        self,
        *,
        profile_id: str,
        template_id: str,
        bridge_kind: str,
        template_ready: bool,
        base_recommended: bool,
    ) -> Dict[str, Any]:
        rows = self._launch_history_rows(profile_id=profile_id, template_id=template_id)
        attempt_count = len(rows)
        success_count = sum(
            1
            for row in rows
            if str(row.get("status", "")).strip().lower() == "success" and bool(row.get("ready", False))
        )
        failure_rows = [
            row
            for row in rows
            if str(row.get("status", "")).strip().lower() in {"error", "blocked", "degraded"}
            or not bool(row.get("ready", False))
        ]
        degraded_count = sum(1 for row in rows if str(row.get("status", "")).strip().lower() == "degraded")
        error_count = sum(1 for row in rows if str(row.get("status", "")).strip().lower() in {"error", "blocked"})
        failure_count = len(failure_rows)
        failure_streak = 0
        success_streak = 0
        for row in reversed(rows):
            failed = str(row.get("status", "")).strip().lower() in {"error", "blocked", "degraded"} or not bool(row.get("ready", False))
            if failed:
                if success_streak > 0:
                    break
                failure_streak += 1
            else:
                if failure_streak > 0:
                    break
                success_streak += 1
        failure_rate = float(failure_count / attempt_count) if attempt_count else 0.0
        health_score = max(5.0, min(100.0, 100.0 - failure_rate * 55.0 - failure_streak * 10.0 + success_streak * 4.0))
        demoted = bool(attempt_count >= 3 and (failure_streak >= 2 or failure_rate >= 0.6))
        if success_streak >= 2 and failure_rate <= 0.25:
            demoted = False
        unstable = bool(failure_streak >= 2 or (attempt_count >= 2 and failure_rate >= 0.5))
        recent_outcomes = [
            {
                "status": str(row.get("status", "")).strip().lower(),
                "ready": bool(row.get("ready", False)),
                "occurred_at": str(row.get("occurred_at", "")).strip(),
            }
            for row in rows[-4:]
        ]
        return {
            "bridge_kind": str(bridge_kind or "").strip().lower(),
            "profile_id": str(profile_id or "").strip().lower(),
            "template_id": str(template_id or "").strip().lower(),
            "attempt_count": attempt_count,
            "recent_attempt_count": min(attempt_count, 24),
            "success_count": success_count,
            "degraded_count": degraded_count,
            "error_count": error_count,
            "failure_count": failure_count,
            "recent_success_count": success_count,
            "recent_failure_count": failure_count,
            "ready_rate": 1.0 if attempt_count == 0 and template_ready else (float(success_count / attempt_count) if attempt_count else 0.0),
            "failure_rate": round(failure_rate, 4),
            "recent_failure_rate": round(failure_rate, 4),
            "failure_streak": failure_streak,
            "success_streak": success_streak,
            "health_score": round(health_score, 2),
            "unstable": unstable,
            "demoted": demoted,
            "demotion_reason": "failure_streak" if demoted and failure_streak >= 2 else ("recent_failure_rate" if demoted else ""),
            "recovered": bool(success_streak >= 2 and attempt_count > 0),
            "template_ready": bool(template_ready),
            "base_recommended": bool(base_recommended),
            "last_status": str(rows[-1].get("status", "")).strip().lower() if rows else "",
            "last_event_at": str(rows[-1].get("occurred_at", "")).strip() if rows else "",
            "last_success_at": str(next((row.get("occurred_at", "") for row in reversed(rows) if str(row.get("status", "")).strip().lower() == "success" and bool(row.get("ready", False))), "")),
            "last_failure_at": str(next((row.get("occurred_at", "") for row in reversed(rows) if str(row.get("status", "")).strip().lower() in {"error", "blocked", "degraded"} or not bool(row.get("ready", False))), "")),
            "avg_duration_s": 0.18,
            "recent_outcomes": recent_outcomes,
        }

    @staticmethod
    def _infer_model_launch_retry_profile(template: Dict[str, Any] | None) -> str:
        row = template if isinstance(template, dict) else {}
        launcher = str(row.get("launcher", "") or "").strip().lower()
        manual_only = bool(row.get("manual_only", False))
        if launcher in {"llama-server", "reasoning_bridge", "tts_http_bridge", "coqui_cli"}:
            return "stabilized"
        if launcher in {"vision_reload", "command_runtime", "vision_runtime", "stt_local_runtime"}:
            return "adaptive"
        if manual_only:
            return "conservative"
        return "adaptive"

    def _launch_template_autonomy_policy(
        self,
        *,
        template_ready: bool,
        template_health: Dict[str, Any] | None,
        strategy_health: Dict[str, Any] | None,
    ) -> Dict[str, Any]:
        health = template_health if isinstance(template_health, dict) else {}
        strategy = strategy_health if isinstance(strategy_health, dict) else {}
        attempt_count = max(0, int(health.get("attempt_count", 0) or 0))
        failure_streak = max(0, int(health.get("failure_streak", 0) or 0))
        success_streak = max(0, int(health.get("success_streak", 0) or 0))
        failure_rate = max(0.0, min(1.0, float(health.get("failure_rate", 0.0) or 0.0)))
        recent_failure_rate = max(0.0, min(1.0, float(health.get("recent_failure_rate", 0.0) or 0.0)))
        health_score = max(0.0, min(100.0, float(health.get("health_score", 0.0) or 0.0)))
        strategy_score = max(0.0, min(100.0, float(strategy.get("score", 0.0) or 0.0)))
        demoted = bool(health.get("demoted", False))
        unstable = bool(health.get("unstable", False))
        strategy_demoted = bool(strategy.get("demoted", False))
        strategy_unstable = bool(strategy.get("unstable", False))
        blacklisted = False
        blacklist_reason = ""
        if attempt_count >= 5:
            if failure_streak >= 3:
                blacklisted = True
                blacklist_reason = "persistent_failure_streak"
            elif failure_rate >= 0.72 and recent_failure_rate >= 0.5:
                blacklisted = True
                blacklist_reason = "long_horizon_failure_rate"
            elif demoted and recent_failure_rate >= 0.67 and success_streak <= 0:
                blacklisted = True
                blacklist_reason = "demoted_recent_failure_pressure"
            elif demoted and strategy_demoted and strategy_score > 0.0 and strategy_score <= 28.0:
                blacklisted = True
                blacklist_reason = "strategy_and_template_demoted"
        if success_streak >= 3 and recent_failure_rate <= 0.25 and failure_rate <= 0.55:
            blacklisted = False
            blacklist_reason = ""
        cooldown_hint_s = 0
        if blacklisted:
            cooldown_hint_s = min(43_200, max(900, (failure_streak * 420) + (attempt_count * 180) + (600 if strategy_demoted else 0)))
        elif strategy_unstable and attempt_count >= 3:
            cooldown_hint_s = min(7_200, max(300, (attempt_count * 90) + (failure_streak * 120)))
        review_required = bool(blacklisted or (attempt_count >= 5 and (demoted or strategy_demoted)) or (attempt_count >= 3 and strategy_unstable))
        autonomy_score = 100.0
        if not bool(template_ready):
            autonomy_score -= 35.0
        autonomy_score -= min(42.0, failure_rate * 48.0)
        autonomy_score -= min(18.0, recent_failure_rate * 22.0)
        autonomy_score -= min(24.0, float(failure_streak) * 8.0)
        autonomy_score -= min(18.0, max(0.0, 50.0 - strategy_score) / 2.0) if strategy_score > 0.0 else 0.0
        if demoted:
            autonomy_score -= 12.0
        if strategy_demoted:
            autonomy_score -= 10.0
        if blacklisted:
            autonomy_score -= 28.0
        if success_streak > 0:
            autonomy_score += min(10.0, float(success_streak) * 2.5)
        autonomy_score = round(max(4.0, min(100.0, autonomy_score)), 4)
        autonomous_allowed = bool(template_ready and not blacklisted and not demoted and not unstable and not strategy_demoted)
        return {
            "template_ready": bool(template_ready),
            "attempt_count": attempt_count,
            "review_required": review_required,
            "autonomous_allowed": autonomous_allowed,
            "autonomy_score": autonomy_score,
            "blacklisted": blacklisted,
            "blacklist_reason": blacklist_reason,
            "cooldown_hint_s": cooldown_hint_s,
            "recovery_success_target": 3,
            "recovery_progress": round(min(1.0, float(success_streak) / 3.0), 4),
            "strategy_demoted": strategy_demoted,
            "strategy_unstable": strategy_unstable,
        }

    def _annotate_launch_templates(
        self,
        *,
        profile_id: str,
        bridge_kind: str,
        templates: list[Dict[str, Any]],
    ) -> tuple[list[Dict[str, Any]], Dict[str, Any]]:
        strategy_outcomes = self._model_launch_retry_strategy_outcomes(
            bridge_kind=str(bridge_kind or "").strip().lower(),
            profile_id=str(profile_id or "").strip().lower(),
        )
        strategy_rows = strategy_outcomes.get("items", []) if isinstance(strategy_outcomes, dict) else []
        strategy_profile_map = {
            str(row.get("retry_profile", "") or "").strip().lower(): dict(row)
            for row in strategy_rows
            if isinstance(row, dict) and str(row.get("retry_profile", "") or "").strip()
        }
        recommended_retry_profile = (
            str(strategy_outcomes.get("recommended_retry_profile", "") or "").strip().lower()
            if isinstance(strategy_outcomes, dict)
            else ""
        )
        annotated: list[Dict[str, Any]] = []
        ready_count = 0
        stable_ready_count = 0
        unstable_count = 0
        demoted_count = 0
        suppressed_count = 0
        blacklisted_count = 0
        autonomy_ready_count = 0
        history_count = 0
        missing_requirements: list[str] = []
        remediation_hints: list[str] = []
        for template in templates:
            if not isinstance(template, dict):
                continue
            row = dict(template)
            health = self._launch_template_health(
                profile_id=profile_id,
                template_id=str(row.get("template_id", "")),
                bridge_kind=bridge_kind,
                template_ready=bool(row.get("ready", False)),
                base_recommended=bool(row.get("recommended", False)),
            )
            selection_score = float(health.get("health_score", 0.0) or 0.0) + (24.0 if bool(row.get("ready", False)) else -24.0)
            if bool(row.get("recommended", False)):
                selection_score += 6.0
            if bool(health.get("demoted", False)):
                selection_score -= 20.0
            retry_profile_hint = self._infer_model_launch_retry_profile(row)
            strategy_health = strategy_profile_map.get(retry_profile_hint, {})
            autonomy_policy = self._launch_template_autonomy_policy(
                template_ready=bool(row.get("ready", False)),
                template_health=health,
                strategy_health=strategy_health,
            )
            if strategy_health:
                strategy_score = float(strategy_health.get("score", 0.0) or 0.0)
                selection_score += max(-10.0, min(6.0, (strategy_score - 50.0) / 8.0))
                if recommended_retry_profile:
                    if retry_profile_hint == recommended_retry_profile:
                        selection_score += 2.5
                    else:
                        selection_score -= 2.0
            if bool(autonomy_policy.get("blacklisted", False)):
                selection_score -= 42.0
            elif bool(autonomy_policy.get("review_required", False)):
                selection_score -= 8.0
            if bool(autonomy_policy.get("autonomous_allowed", False)):
                selection_score += 3.5
            row["recommended_base"] = bool(row.get("recommended", False))
            row["recommended"] = False
            row["selection_score"] = round(selection_score, 4)
            row["retry_profile_hint"] = retry_profile_hint
            row["retry_strategy_score"] = round(float(strategy_health.get("score", 0.0) or 0.0), 4) if strategy_health else 0.0
            row["retry_strategy_health"] = dict(strategy_health) if isinstance(strategy_health, dict) else {}
            row["autonomy_policy"] = dict(autonomy_policy)
            row["blacklisted"] = bool(autonomy_policy.get("blacklisted", False))
            row["blacklist_reason"] = str(autonomy_policy.get("blacklist_reason", "") or "").strip().lower()
            row["cooldown_hint_s"] = int(autonomy_policy.get("cooldown_hint_s", 0) or 0)
            row["suppressed"] = False
            row["suppression_reason"] = ""
            health["selection_score"] = row["selection_score"]
            health["retry_profile_hint"] = retry_profile_hint
            health["retry_strategy_score"] = row["retry_strategy_score"]
            health["retry_strategy_health"] = row["retry_strategy_health"]
            health["autonomy_policy"] = row["autonomy_policy"]
            health["blacklisted"] = row["blacklisted"]
            health["blacklist_reason"] = row["blacklist_reason"]
            health["cooldown_hint_s"] = row["cooldown_hint_s"]
            health["autonomous_allowed"] = bool(autonomy_policy.get("autonomous_allowed", False))
            health["autonomy_score"] = round(float(autonomy_policy.get("autonomy_score", 0.0) or 0.0), 4)
            row["health"] = dict(health)
            annotated.append(row)
            ready_count += 1 if bool(row.get("ready", False)) else 0
            unstable_count += 1 if bool(health.get("unstable", False)) else 0
            demoted_count += 1 if bool(health.get("demoted", False)) else 0
            blacklisted_count += 1 if bool(autonomy_policy.get("blacklisted", False)) else 0
            autonomy_ready_count += 1 if bool(autonomy_policy.get("autonomous_allowed", False)) else 0
            history_count += int(health.get("attempt_count", 0) or 0)
            missing_requirements.extend([str(item) for item in row.get("missing_requirements", []) if isinstance(item, str)])
            remediation_hints.extend([str(item) for item in row.get("remediation_hints", []) if isinstance(item, str)])
        ready_template_count = sum(1 for row in annotated if bool(row.get("ready", False)))
        healthy_ready_template_count = sum(
            1
            for row in annotated
            if bool(row.get("ready", False))
            and not bool(row.get("blacklisted", False))
            and not bool(((row.get("health", {}) if isinstance(row.get("health"), dict) else {}).get("demoted", False)))
            and not bool(((row.get("health", {}) if isinstance(row.get("health"), dict) else {}).get("unstable", False)))
        )
        for row in annotated:
            row_health = row.get("health", {}) if isinstance(row.get("health"), dict) else {}
            strategy_health = row.get("retry_strategy_health", {}) if isinstance(row.get("retry_strategy_health"), dict) else {}
            recommended_profile_alternative_exists = bool(
                recommended_retry_profile
                and any(
                    isinstance(other, dict)
                    and other is not row
                    and bool(other.get("ready", False))
                    and not bool(other.get("blacklisted", False))
                    and str(other.get("retry_profile_hint", "") or "").strip().lower() == recommended_retry_profile
                    and not bool((((other.get("health", {}) if isinstance(other.get("health"), dict) else {})).get("demoted", False)))
                    for other in annotated
                )
            )
            if bool(row.get("ready", False)) and ready_template_count > 1 and bool(strategy_health.get("demoted", False)):
                row["suppressed"] = True
                row["suppression_reason"] = "retry_strategy_demoted"
                row["selection_score"] = round(float(row.get("selection_score", 0.0) or 0.0) - 18.0, 4)
            elif (
                bool(row.get("ready", False))
                and healthy_ready_template_count > 1
                and bool(strategy_health.get("unstable", False))
                and recommended_profile_alternative_exists
            ):
                row["suppressed"] = True
                row["suppression_reason"] = "retry_strategy_unstable"
                row["selection_score"] = round(float(row.get("selection_score", 0.0) or 0.0) - 10.0, 4)
            if bool(row.get("suppressed", False)):
                row_health["suppressed"] = True
                row_health["suppression_reason"] = str(row.get("suppression_reason", "")).strip()
                row_health["selection_score"] = row["selection_score"]
                row["health"] = dict(row_health)
                suppressed_count += 1
                remediation_hints.append(
                    f"Template '{str(row.get('template_id', '')).strip()}' is temporarily suppressed while healthier retry strategies are available."
                )
            autonomy_policy = row.get("autonomy_policy", {}) if isinstance(row.get("autonomy_policy"), dict) else {}
            if bool(autonomy_policy.get("blacklisted", False)):
                row["selection_score"] = round(float(row.get("selection_score", 0.0) or 0.0) - 12.0, 4)
                row_health["blacklisted"] = True
                row_health["blacklist_reason"] = str(autonomy_policy.get("blacklist_reason", "") or "").strip().lower()
                row_health["cooldown_hint_s"] = int(autonomy_policy.get("cooldown_hint_s", 0) or 0)
                row["health"] = dict(row_health)
                remediation_hints.append(
                    f"Template '{str(row.get('template_id', '')).strip()}' is blacklisted for autonomous execution until its launch history recovers."
                )
            final_autonomous_allowed = bool(
                row.get("ready", False)
                and not bool(row.get("suppressed", False))
                and not bool(row.get("blacklisted", False))
                and not bool(row_health.get("demoted", False))
                and not bool(row_health.get("unstable", False))
            )
            autonomy_policy["autonomous_allowed"] = final_autonomous_allowed
            row["autonomy_policy"] = dict(autonomy_policy)
            row_health["autonomous_allowed"] = final_autonomous_allowed
            row["health"] = dict(row_health)
        stable_ready_count = sum(
            1
            for row in annotated
            if bool(row.get("ready", False))
            and not bool(row.get("blacklisted", False))
            and not bool(((row.get("health", {}) if isinstance(row.get("health"), dict) else {}).get("demoted", False)))
            and not bool(((row.get("health", {}) if isinstance(row.get("health"), dict) else {}).get("unstable", False)))
            and not bool(row.get("suppressed", False))
        )
        autonomy_ready_count = sum(
            1
            for row in annotated
            if bool(
                ((row.get("autonomy_policy", {}) if isinstance(row.get("autonomy_policy"), dict) else {}).get("autonomous_allowed", False))
            )
        )
        annotated.sort(
            key=lambda row: (
                0 if not bool(row.get("blacklisted", False)) else 1,
                0 if not bool(row.get("suppressed", False)) else 1,
                0 if bool(row.get("ready", False)) else 1,
                0 if not bool(((row.get("health", {}) if isinstance(row.get("health"), dict) else {}).get("demoted", False))) else 1,
                0 if not bool(((row.get("health", {}) if isinstance(row.get("health"), dict) else {}).get("unstable", False))) else 1,
                -float(row.get("selection_score", 0.0) or 0.0),
                str(row.get("template_id", "")),
            )
        )
        recommended_template_id = str(annotated[0].get("template_id", "")).strip() if annotated else ""
        recommended_base_template_id = str(next((row.get("template_id", "") for row in annotated if bool(row.get("recommended_base", False))), "")).strip()
        for row in annotated:
            row["recommended"] = str(row.get("template_id", "")).strip() == recommended_template_id
        return annotated, {
            "template_count": len(annotated),
            "ready_count": ready_count,
            "stable_ready_count": stable_ready_count,
            "unstable_count": unstable_count,
            "demoted_count": demoted_count,
            "suppressed_count": suppressed_count,
            "blacklisted_count": blacklisted_count,
            "autonomy_ready_count": autonomy_ready_count,
            "suppressed_template_ids": [str(row.get("template_id", "")).strip() for row in annotated if bool(row.get("suppressed", False))],
            "blacklisted_template_ids": [str(row.get("template_id", "")).strip() for row in annotated if bool(row.get("blacklisted", False))],
            "recommended_template_id": recommended_template_id,
            "recommended_base_template_id": recommended_base_template_id,
            "recommended_shifted": bool(recommended_template_id and recommended_base_template_id and recommended_template_id != recommended_base_template_id),
            "recommended_retry_profile": recommended_retry_profile,
            "missing_requirements": list(dict.fromkeys(missing_requirements)),
            "remediation_hints": list(dict.fromkeys(remediation_hints)),
            "history_count": history_count,
            "strategy_outcomes": strategy_outcomes.get("items", []) if isinstance(strategy_outcomes, dict) else [],
            "retry_profile_scores": strategy_outcomes.get("profile_scores", []) if isinstance(strategy_outcomes, dict) else [],
        }

    def _record_launch_template_event(
        self,
        *,
        profile_id: str,
        template_id: str,
        requested_template_id: str = "",
        bridge_kind: str,
        launcher: str,
        requested_launcher: str = "",
        requested_transport: str = "",
        requested_manual_only: bool = False,
        requested_autostart_capable: bool = False,
        ready: bool,
        status: str,
        fallback_applied: bool = False,
        fallback_reason: str = "",
        fallback_source: str = "",
        attempt_chain_id: str = "",
        attempt_index: int = 1,
        retry_trigger: str = "",
        retry_requested_profile: str = "",
        retry_profile: str = "",
        retry_profile_adjusted: bool = False,
        retry_profile_adjustment_reason: str = "",
        retry_strategy: str = "",
        retry_strategy_score: float = 0.0,
        retry_escalation_mode: str = "",
        retry_prefer_recommended: bool = False,
        retry_delay_ms: int = 0,
        retry_jitter_ms: int = 0,
        retry_recommended_template_id: str = "",
        retry_target_template_id: str = "",
        retry_preview_schedule_ms: list[int] | None = None,
        execution_diff: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        row = {
            "event_id": len(self.launch_template_history) + 1,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "profile_id": str(profile_id or "").strip().lower(),
            "template_id": str(template_id or "").strip().lower(),
            "requested_template_id": str(requested_template_id or template_id or "").strip().lower(),
            "bridge_kind": str(bridge_kind or "").strip().lower(),
            "launcher": str(launcher or "").strip().lower(),
            "status": str(status or "unknown").strip().lower() or "unknown",
            "ready": bool(ready),
            "failure_like": str(status or "unknown").strip().lower() in {"error", "blocked", "degraded"} or not bool(ready),
            "fallback_applied": bool(fallback_applied),
            "fallback_reason": str(fallback_reason or "").strip().lower(),
            "fallback_source": str(fallback_source or "").strip().lower(),
            "fallback_from_template_id": str(requested_template_id or "").strip().lower(),
            "fallback_to_template_id": str(template_id or "").strip().lower() if fallback_applied else "",
            "requested_launcher": str(requested_launcher or launcher or "").strip().lower(),
            "requested_transport": str(requested_transport or "").strip().lower(),
            "requested_manual_only": bool(requested_manual_only),
            "requested_autostart_capable": bool(requested_autostart_capable),
            "attempt_chain_id": str(attempt_chain_id or "").strip(),
            "attempt_index": max(1, int(attempt_index or 1)),
            "retry_trigger": str(retry_trigger or "").strip().lower(),
            "retry_requested_profile": str(retry_requested_profile or "").strip().lower(),
            "retry_profile": str(retry_profile or "").strip().lower(),
            "retry_profile_adjusted": bool(retry_profile_adjusted),
            "retry_profile_adjustment_reason": str(retry_profile_adjustment_reason or "").strip().lower(),
            "retry_strategy": str(retry_strategy or "").strip().lower(),
            "retry_strategy_score": round(float(retry_strategy_score or 0.0), 4),
            "retry_escalation_mode": str(retry_escalation_mode or "").strip().lower(),
            "retry_prefer_recommended": bool(retry_prefer_recommended),
            "retry_delay_ms": max(0, int(retry_delay_ms or 0)),
            "retry_jitter_ms": max(0, int(retry_jitter_ms or 0)),
            "retry_recommended_template_id": str(retry_recommended_template_id or "").strip().lower(),
            "retry_target_template_id": str(retry_target_template_id or "").strip().lower(),
            "retry_preview_schedule_ms": [
                max(0, int(item or 0))
                for item in (retry_preview_schedule_ms or [])
            ],
            "execution_diff": dict(execution_diff) if isinstance(execution_diff, dict) else {},
        }
        self.launch_template_history.append(row)
        self.launch_template_history = self.launch_template_history[-120:]
        return row

    def model_launch_template_history(
        self,
        *,
        limit: int = 48,
        bridge_kind: str = "",
        profile_id: str = "",
        template_id: str = "",
        status: str = "",
        failure_like: bool | None = None,
        after_event_id: int = 0,
    ) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 256))
        clean_bridge_kind = str(bridge_kind or "").strip().lower()
        clean_profile_id = str(profile_id or "").strip().lower()
        clean_template_id = str(template_id or "").strip().lower()
        clean_status = str(status or "").strip().lower()
        rows = [dict(item) for item in self.launch_template_history if isinstance(item, dict)]
        if clean_bridge_kind:
            rows = [row for row in rows if str(row.get("bridge_kind", "")).strip().lower() == clean_bridge_kind]
        if clean_profile_id:
            rows = [row for row in rows if str(row.get("profile_id", "")).strip().lower() == clean_profile_id]
        if clean_template_id:
            rows = [
                row
                for row in rows
                if str(row.get("template_id", "")).strip().lower() == clean_template_id
                or str(row.get("requested_template_id", "")).strip().lower() == clean_template_id
            ]
        if clean_status:
            rows = [row for row in rows if str(row.get("status", "")).strip().lower() == clean_status]
        if failure_like is not None:
            rows = [row for row in rows if bool(row.get("failure_like", False)) is bool(failure_like)]
        if int(after_event_id or 0) > 0:
            rows = [row for row in rows if int(row.get("event_id", 0) or 0) > int(after_event_id or 0)]
        selected = rows[-bounded:]
        timeline_map: dict[str, dict[str, Any]] = {}
        bridge_kind_counts: dict[str, int] = {}
        launcher_counts: dict[str, int] = {}
        retry_profile_counts: dict[str, int] = {}
        retry_strategy_counts: dict[str, int] = {}
        retry_escalation_counts: dict[str, int] = {}
        status_counts: dict[str, int] = {}
        fallback_count = 0
        retry_chain_depths: dict[str, int] = {}
        retry_delay_total_ms = 0
        max_retry_delay_ms = 0
        for row in rows:
            current_status = str(row.get("status", "")).strip().lower() or "unknown"
            current_bridge_kind = str(row.get("bridge_kind", "")).strip().lower() or "unknown"
            current_launcher = str(row.get("launcher", "")).strip().lower() or "unknown"
            current_retry_profile = str(row.get("retry_profile", "")).strip().lower() or "none"
            current_retry_strategy = str(row.get("retry_strategy", "")).strip().lower() or "none"
            current_retry_strategy_score = float(row.get("retry_strategy_score", 0.0) or 0.0)
            current_retry_escalation = str(row.get("retry_escalation_mode", "")).strip().lower() or "none"
            current_retry_delay_ms = max(0, int(row.get("retry_delay_ms", 0) or 0))
            status_counts[current_status] = status_counts.get(current_status, 0) + 1
            bridge_kind_counts[current_bridge_kind] = bridge_kind_counts.get(current_bridge_kind, 0) + 1
            launcher_counts[current_launcher] = launcher_counts.get(current_launcher, 0) + 1
            retry_profile_counts[current_retry_profile] = retry_profile_counts.get(current_retry_profile, 0) + 1
            retry_strategy_counts[current_retry_strategy] = retry_strategy_counts.get(current_retry_strategy, 0) + 1
            retry_escalation_counts[current_retry_escalation] = retry_escalation_counts.get(current_retry_escalation, 0) + 1
            retry_delay_total_ms += current_retry_delay_ms
            max_retry_delay_ms = max(max_retry_delay_ms, current_retry_delay_ms)
            current_chain_id = str(row.get("attempt_chain_id", "") or "").strip()
            if current_chain_id:
                retry_chain_depths[current_chain_id] = max(
                    retry_chain_depths.get(current_chain_id, 0),
                    max(1, int(row.get("attempt_index", 1) or 1)),
                )
            bucket = str(row.get("occurred_at", "")).strip()[:13] + ":00:00Z"
            entry = timeline_map.setdefault(
                bucket,
                {
                    "bucket": bucket,
                    "count": 0,
                    "success_count": 0,
                    "degraded_count": 0,
                    "error_count": 0,
                    "failure_count": 0,
                    "fallback_count": 0,
                    "degradation_count": 0,
                    "retry_profile_counts": {},
                    "strategy_score_total": 0.0,
                    "strategy_score_count": 0,
                },
            )
            entry["count"] = int(entry.get("count", 0) or 0) + 1
            if current_status == "success":
                entry["success_count"] = int(entry.get("success_count", 0) or 0) + 1
            elif current_status == "degraded":
                entry["degraded_count"] = int(entry.get("degraded_count", 0) or 0) + 1
            elif current_status == "error":
                entry["error_count"] = int(entry.get("error_count", 0) or 0) + 1
            if bool(row.get("failure_like", False)):
                entry["failure_count"] = int(entry.get("failure_count", 0) or 0) + 1
            if bool(row.get("fallback_applied", False)):
                fallback_count += 1
                entry["fallback_count"] = int(entry.get("fallback_count", 0) or 0) + 1
            if current_status == "degraded" or bool(row.get("failure_like", False)) or bool(row.get("fallback_applied", False)):
                entry["degradation_count"] = int(entry.get("degradation_count", 0) or 0) + 1
            bucket_profiles = entry.get("retry_profile_counts", {}) if isinstance(entry.get("retry_profile_counts"), dict) else {}
            bucket_profiles[current_retry_profile] = int(bucket_profiles.get(current_retry_profile, 0) or 0) + 1
            entry["retry_profile_counts"] = bucket_profiles
            if current_retry_strategy_score > 0.0 or "retry_strategy_score" in row:
                entry["strategy_score_total"] = float(entry.get("strategy_score_total", 0.0) or 0.0) + current_retry_strategy_score
                entry["strategy_score_count"] = int(entry.get("strategy_score_count", 0) or 0) + 1
        strategy_outcomes = self._model_launch_retry_strategy_outcomes(
            bridge_kind=clean_bridge_kind,
            profile_id=clean_profile_id,
        )
        timeline = sorted(timeline_map.values(), key=lambda item: str(item.get("bucket", "")))[-24:]
        retry_profile_trend: list[Dict[str, Any]] = []
        strategy_score_timeline: list[Dict[str, Any]] = []
        degradation_timeline: list[Dict[str, Any]] = []
        for entry in timeline:
            bucket_profiles = entry.get("retry_profile_counts", {}) if isinstance(entry.get("retry_profile_counts"), dict) else {}
            dominant_retry_profile = ""
            if bucket_profiles:
                dominant_retry_profile = str(
                    sorted(bucket_profiles.items(), key=lambda item: (-int(item[1] or 0), str(item[0] or "")))[0][0] or ""
                ).strip().lower()
            strategy_score_count = int(entry.get("strategy_score_count", 0) or 0)
            average_score = round(
                float(entry.get("strategy_score_total", 0.0) or 0.0) / max(1, strategy_score_count),
                4,
            ) if strategy_score_count > 0 else 0.0
            degradation_count = int(entry.get("degradation_count", 0) or 0)
            total_count = max(1, int(entry.get("count", 0) or 0))
            degradation_rate = round(float(degradation_count) / float(total_count), 4)
            entry["dominant_retry_profile"] = dominant_retry_profile
            entry["strategy_score_avg"] = average_score
            entry["degradation_rate"] = degradation_rate
            retry_profile_trend.append(
                {
                    "bucket": str(entry.get("bucket", "")).strip(),
                    "dominant_retry_profile": dominant_retry_profile,
                    "total_count": int(entry.get("count", 0) or 0),
                    "counts": dict(bucket_profiles),
                }
            )
            strategy_score_timeline.append(
                {
                    "bucket": str(entry.get("bucket", "")).strip(),
                    "average_score": average_score,
                    "sample_count": strategy_score_count,
                    "fallback_count": int(entry.get("fallback_count", 0) or 0),
                    "failure_count": int(entry.get("failure_count", 0) or 0),
                }
            )
            degradation_timeline.append(
                {
                    "bucket": str(entry.get("bucket", "")).strip(),
                    "degradation_count": degradation_count,
                    "degradation_rate": degradation_rate,
                    "count": int(entry.get("count", 0) or 0),
                    "fallback_count": int(entry.get("fallback_count", 0) or 0),
                    "failure_count": int(entry.get("failure_count", 0) or 0),
                }
            )
        return {
            "status": "success",
            "count": len(selected),
            "total": len(rows),
            "limit": bounded,
            "items": selected,
            "filters": {
                "bridge_kind": clean_bridge_kind,
                "profile_id": clean_profile_id,
                "template_id": clean_template_id,
                "status": clean_status,
                "failure_like": failure_like,
                "after_event_id": int(after_event_id or 0),
            },
            "status_counts": status_counts,
            "bridge_kind_counts": bridge_kind_counts,
            "launcher_counts": launcher_counts,
            "retry_profile_counts": retry_profile_counts,
            "retry_strategy_counts": retry_strategy_counts,
            "retry_escalation_counts": retry_escalation_counts,
            "failure_count": sum(1 for row in rows if bool(row.get("failure_like", False))),
            "success_count": sum(
                1
                for row in rows
                if str(row.get("status", "")).strip().lower() == "success" and bool(row.get("ready", False))
            ),
            "fallback_count": fallback_count,
            "demoted_fallback_count": sum(
                1
                for row in rows
                if bool(row.get("fallback_applied", False))
                and "demoted" in str(row.get("fallback_reason", "")).strip().lower()
            ),
            "retry_chain_count": sum(1 for depth in retry_chain_depths.values() if depth > 1),
            "max_attempt_depth": max(retry_chain_depths.values(), default=0),
            "retry_delay_total_ms": retry_delay_total_ms,
            "max_retry_delay_ms": max_retry_delay_ms,
            "recommended_retry_profile": str(strategy_outcomes.get("recommended_retry_profile", "") or "").strip().lower(),
            "strategy_outcomes": strategy_outcomes.get("items", []),
            "retry_profile_scores": strategy_outcomes.get("profile_scores", []),
            "latest_event_id": int(rows[-1].get("event_id", 0) or 0) if rows else 0,
            "timeline": timeline,
            "retry_profile_trend": retry_profile_trend,
            "strategy_score_timeline": strategy_score_timeline,
            "degradation_timeline": degradation_timeline,
            "top_profiles": [],
            "top_templates": [],
            "history_path": "data/runtime/model_launch_history.jsonl",
        }

    def model_launch_template_event_detail(
        self,
        *,
        event_id: int,
        sibling_limit: int = 6,
    ) -> Dict[str, Any]:
        clean_event_id = max(0, int(event_id or 0))
        bounded_sibling_limit = max(2, min(int(sibling_limit or 6), 20))
        rows = [dict(item) for item in self.launch_template_history if isinstance(item, dict)]
        target_index = next(
            (index for index, row in enumerate(rows) if int(row.get("event_id", 0) or 0) == clean_event_id),
            -1,
        )
        if target_index < 0:
            return {"status": "error", "message": f"launch event not found: {clean_event_id}", "event_id": clean_event_id}
        event = dict(rows[target_index])
        chain_id = str(event.get("attempt_chain_id", "") or "").strip()
        chain_rows = [
            dict(row)
            for row in rows
            if not chain_id or str(row.get("attempt_chain_id", "") or "").strip() == chain_id
        ]
        chain_rows.sort(key=lambda row: (int(row.get("attempt_index", 0) or 0), int(row.get("event_id", 0) or 0)))
        root_event = dict(chain_rows[0]) if chain_rows else dict(event)
        final_event = dict(chain_rows[-1]) if chain_rows else dict(event)
        related_rows = [
            dict(row)
            for row in rows[max(0, target_index - bounded_sibling_limit): min(len(rows), target_index + bounded_sibling_limit + 1)]
        ]
        strategy_outcomes = self._model_launch_retry_strategy_outcomes(
            bridge_kind=str(event.get("bridge_kind", "") or "").strip().lower(),
            profile_id=str(event.get("profile_id", "") or "").strip().lower(),
        )
        return {
            "status": "success",
            "event_id": clean_event_id,
            "event": event,
            "root_event": root_event,
            "final_event": final_event,
            "attempt_chain": chain_rows,
            "chain_summary": {
                "attempt_count": len(chain_rows),
                "failure_count": sum(1 for row in chain_rows if bool(row.get("failure_like", False))),
                "fallback_count": sum(1 for row in chain_rows if bool(row.get("fallback_applied", False))),
                "delay_total_ms": sum(max(0, int(row.get("retry_delay_ms", 0) or 0)) for row in chain_rows),
                "max_delay_ms": max((max(0, int(row.get("retry_delay_ms", 0) or 0)) for row in chain_rows), default=0),
                "final_status": str(final_event.get("status", "unknown") or "unknown").strip().lower(),
                "retry_profiles": list(dict.fromkeys(str(row.get("retry_profile", "") or "").strip().lower() for row in chain_rows if str(row.get("retry_profile", "") or "").strip())),
                "retry_strategies": list(dict.fromkeys(str(row.get("retry_strategy", "") or "").strip().lower() for row in chain_rows if str(row.get("retry_strategy", "") or "").strip())),
            },
            "requested_template": {
                "template_id": str(root_event.get("requested_template_id", "") or "").strip().lower(),
                "launcher": str(root_event.get("requested_launcher", "") or "").strip().lower(),
                "transport": str(root_event.get("requested_transport", "") or "").strip().lower(),
                "manual_only": bool(root_event.get("requested_manual_only", False)),
                "autostart_capable": bool(root_event.get("requested_autostart_capable", False)),
                "title": str(root_event.get("requested_template_title", "") or "").strip(),
            },
            "executed_template": {
                "template_id": str(final_event.get("template_id", "") or "").strip().lower(),
                "launcher": str(final_event.get("launcher", "") or "").strip().lower(),
                "transport": str(final_event.get("transport", "") or "").strip().lower(),
                "manual_only": bool(final_event.get("manual_only", False)),
                "autostart_capable": bool(final_event.get("autostart_capable", False)),
                "title": str(final_event.get("template_title", "") or "").strip(),
            },
            "root_execution_diff": self._model_launch_execution_diff(
                requested_template_id=str(root_event.get("requested_template_id", "") or "").strip().lower(),
                executed_template_id=str(final_event.get("template_id", "") or "").strip().lower(),
                requested_template={
                    "launcher": str(root_event.get("requested_launcher", "") or "").strip().lower(),
                    "transport": str(root_event.get("requested_transport", "") or "").strip().lower(),
                    "manual_only": bool(root_event.get("requested_manual_only", False)),
                    "autostart_capable": bool(root_event.get("requested_autostart_capable", False)),
                    "title": str(root_event.get("requested_template_title", "") or "").strip(),
                },
                executed_template={
                    "launcher": str(final_event.get("launcher", "") or "").strip().lower(),
                    "transport": str(final_event.get("transport", "") or "").strip().lower(),
                    "manual_only": bool(final_event.get("manual_only", False)),
                    "autostart_capable": bool(final_event.get("autostart_capable", False)),
                    "title": str(final_event.get("template_title", "") or "").strip(),
                },
            ),
            "related_events": related_rows,
            "strategy_outcomes": strategy_outcomes.get("items", []),
            "recommended_retry_profile": str(strategy_outcomes.get("recommended_retry_profile", "") or "").strip().lower(),
            "retry_profile_scores": strategy_outcomes.get("profile_scores", []),
            "profile": {},
            "profile_launch_health": {},
            "template_launch_health": {},
        }

    @staticmethod
    def _model_launch_execution_diff(
        *,
        requested_template_id: str,
        executed_template_id: str,
        requested_template: Dict[str, Any] | None,
        executed_template: Dict[str, Any] | None,
    ) -> Dict[str, Any]:
        requested = requested_template if isinstance(requested_template, dict) else {}
        executed = executed_template if isinstance(executed_template, dict) else {}
        diff: Dict[str, Any] = {
            "requested_template_id": str(requested_template_id or "").strip().lower(),
            "executed_template_id": str(executed_template_id or "").strip().lower(),
            "template_changed": str(requested_template_id or "").strip().lower()
            != str(executed_template_id or "").strip().lower(),
            "changed_fields": [],
        }
        comparable_fields = (
            ("launcher", "launcher"),
            ("transport", "transport"),
            ("manual_only", "manual_only"),
            ("autostart_capable", "autostart_capable"),
            ("title", "title"),
        )
        changed_fields: list[str] = []
        for field_name, template_key in comparable_fields:
            requested_value = requested.get(template_key)
            executed_value = executed.get(template_key)
            if requested_value != executed_value:
                changed_fields.append(field_name)
                diff[field_name] = {
                    "requested": requested_value,
                    "executed": executed_value,
                }
        diff["changed_fields"] = changed_fields
        return diff

    @staticmethod
    def _model_launch_retry_delay_ms(
        *,
        strategy: str,
        base_delay_ms: int,
        max_delay_ms: int,
        jitter_ms: int,
        attempt_index: int,
    ) -> int:
        clean_strategy = str(strategy or "").strip().lower()
        bounded_base = max(0, min(int(base_delay_ms or 0), 8_000))
        bounded_max = max(bounded_base, min(int(max_delay_ms or 0), 12_000))
        bounded_jitter = max(0, min(int(jitter_ms or 0), 1_500))
        current_attempt = max(1, int(attempt_index or 1))
        if bounded_base <= 0:
            return 0
        growth = 1.55
        if clean_strategy == "stabilized_backoff":
            growth = 1.85
        elif clean_strategy == "aggressive_failover":
            growth = 1.25
        raw_delay = int(round(float(bounded_base) * (growth ** float(max(0, current_attempt - 1)))))
        deterministic_jitter = 0
        if bounded_jitter > 0:
            deterministic_jitter = min(
                bounded_jitter,
                int(((current_attempt * 37) + bounded_base + bounded_max) % (bounded_jitter + 1)),
            )
        return max(0, min(bounded_max, raw_delay + deterministic_jitter))

    def _model_launch_retry_policy(
        self,
        *,
        bridge_kind: str = "",
        profile_id: str = "",
        requested_template: Dict[str, Any] | None,
        current_template: Dict[str, Any] | None,
        recommended_template_id: str = "",
        attempt_index: int = 1,
        max_attempts: int = 1,
        failure_like: bool = False,
        retry_profile: str = "",
        retry_base_delay_ms: int | None = None,
        retry_max_delay_ms: int | None = None,
        retry_jitter_ms: int | None = None,
        retry_prefer_recommended: bool | None = None,
    ) -> Dict[str, Any]:
        requested = requested_template if isinstance(requested_template, dict) else {}
        current = current_template if isinstance(current_template, dict) else requested
        requested_launcher = str(requested.get("launcher", "") or "").strip().lower()
        current_launcher = str(current.get("launcher", "") or "").strip().lower()
        requested_manual_only = bool(requested.get("manual_only", False))
        current_manual_only = bool(current.get("manual_only", False))
        current_template_id = str(current.get("template_id", "") or "").strip().lower()
        clean_recommended_template_id = str(recommended_template_id or "").strip().lower()
        clean_profile = str(retry_profile or "").strip().lower()
        if clean_profile not in {"conservative", "adaptive", "stabilized", "aggressive"}:
            if requested_launcher in {"llama-server", "reasoning_bridge", "tts_http_bridge", "coqui_cli"}:
                clean_profile = "stabilized"
            elif requested_manual_only:
                clean_profile = "conservative"
            else:
                clean_profile = "adaptive"
        requested_profile = clean_profile
        strategy_outcomes = self._model_launch_retry_strategy_outcomes(
            bridge_kind=str(bridge_kind or "").strip().lower(),
            profile_id=str(profile_id or "").strip().lower(),
        )
        strategy_rows = strategy_outcomes.get("items", []) if isinstance(strategy_outcomes, dict) else []
        selected_strategy_health = next(
            (
                row
                for row in strategy_rows
                if isinstance(row, dict)
                and str(row.get("retry_profile", "") or "").strip().lower() == requested_profile
            ),
            None,
        )
        recommended_retry_profile = str(strategy_outcomes.get("recommended_retry_profile", "") or "").strip().lower() if isinstance(strategy_outcomes, dict) else ""
        profile_adjusted = False
        profile_adjustment_reason = ""
        if isinstance(selected_strategy_health, dict) and bool(selected_strategy_health.get("demoted", False)):
            fallback_profile = recommended_retry_profile or ("conservative" if requested_manual_only else "adaptive")
            if fallback_profile and fallback_profile != clean_profile:
                clean_profile = fallback_profile
                profile_adjusted = True
                profile_adjustment_reason = "strategy_history_demoted"
        elif isinstance(selected_strategy_health, dict) and bool(selected_strategy_health.get("unstable", False)):
            fallback_profile = recommended_retry_profile or ("adaptive" if clean_profile == "aggressive" else clean_profile)
            if fallback_profile and fallback_profile != clean_profile:
                clean_profile = fallback_profile
                profile_adjusted = True
                profile_adjustment_reason = "strategy_history_unstable"
        defaults = {
            "conservative": {"strategy": "stabilized_backoff", "base": 320, "max": 1800, "jitter": 80},
            "adaptive": {"strategy": "adaptive_backoff", "base": 180, "max": 1400, "jitter": 60},
            "stabilized": {"strategy": "stabilized_backoff", "base": 240, "max": 2200, "jitter": 90},
            "aggressive": {"strategy": "aggressive_failover", "base": 90, "max": 900, "jitter": 35},
        }
        profile_defaults = defaults.get(clean_profile, defaults["adaptive"])
        base_delay_ms = max(0, min(int(retry_base_delay_ms if retry_base_delay_ms is not None else profile_defaults["base"]), 8_000))
        max_delay_ms = max(base_delay_ms, min(int(retry_max_delay_ms if retry_max_delay_ms is not None else profile_defaults["max"]), 12_000))
        jitter_ms = max(0, min(int(retry_jitter_ms if retry_jitter_ms is not None else profile_defaults["jitter"]), 1_500))
        prefer_recommended = (
            bool(retry_prefer_recommended)
            if retry_prefer_recommended is not None
            else (
                clean_recommended_template_id != ""
                and clean_recommended_template_id != current_template_id
                and not requested_manual_only
            )
        )
        escalation_mode = "score_ranked"
        if prefer_recommended and clean_recommended_template_id and clean_recommended_template_id != current_template_id:
            escalation_mode = "recommended_first"
        elif requested_launcher in {"llama-server", "reasoning_bridge", "tts_http_bridge"}:
            escalation_mode = "managed_to_endpoint"
        elif requested_manual_only or current_manual_only:
            escalation_mode = "breadth_first"
        bounded_attempt_index = max(1, int(attempt_index or 1))
        bounded_max_attempts = max(1, min(int(max_attempts or 1), 8))
        remaining_attempts = max(0, bounded_max_attempts - bounded_attempt_index)
        strategy = str(profile_defaults["strategy"])
        delay_ms = (
            self._model_launch_retry_delay_ms(
                strategy=strategy,
                base_delay_ms=base_delay_ms,
                max_delay_ms=max_delay_ms,
                jitter_ms=jitter_ms,
                attempt_index=bounded_attempt_index,
            )
            if failure_like and remaining_attempts > 0
            else 0
        )
        preview_schedule_ms: list[int] = []
        if remaining_attempts > 0:
            for offset in range(remaining_attempts):
                preview_schedule_ms.append(
                    self._model_launch_retry_delay_ms(
                        strategy=strategy,
                        base_delay_ms=base_delay_ms,
                        max_delay_ms=max_delay_ms,
                        jitter_ms=jitter_ms,
                        attempt_index=bounded_attempt_index + offset,
                    )
                )
        return {
            "enabled": bool(failure_like and bounded_max_attempts > bounded_attempt_index),
            "requested_profile": requested_profile,
            "profile": clean_profile,
            "profile_adjusted": profile_adjusted,
            "profile_adjustment_reason": profile_adjustment_reason,
            "strategy": strategy,
            "escalation_mode": escalation_mode,
            "prefer_recommended": prefer_recommended,
            "current_launcher": requested_launcher or current_launcher,
            "current_manual_only": requested_manual_only or current_manual_only,
            "recommended_template_id": clean_recommended_template_id,
            "recommended_retry_profile": recommended_retry_profile,
            "strategy_health": dict(selected_strategy_health) if isinstance(selected_strategy_health, dict) else {},
            "attempt_index": bounded_attempt_index,
            "max_attempts": bounded_max_attempts,
            "remaining_attempts": remaining_attempts,
            "base_delay_ms": base_delay_ms,
            "max_delay_ms": max_delay_ms,
            "jitter_ms": jitter_ms,
            "delay_ms": delay_ms,
            "preview_schedule_ms": preview_schedule_ms,
        }

    def _model_launch_retry_strategy_outcomes(
        self,
        *,
        bridge_kind: str = "",
        profile_id: str = "",
        recent_window: int | None = None,
    ) -> Dict[str, Any]:
        clean_bridge_kind = str(bridge_kind or "").strip().lower()
        clean_profile_id = str(profile_id or "").strip().lower()
        bounded_recent_window = max(4, min(int(recent_window or 24), 96))
        rows = [dict(item) for item in self.launch_template_history if isinstance(item, dict)]
        if clean_bridge_kind:
            rows = [row for row in rows if str(row.get("bridge_kind", "")).strip().lower() == clean_bridge_kind]
        if clean_profile_id:
            rows = [row for row in rows if str(row.get("profile_id", "")).strip().lower() == clean_profile_id]
        rows = [
            row
            for row in rows
            if str(row.get("retry_strategy", "") or "").strip()
            or str(row.get("retry_profile", "") or "").strip()
        ]
        grouped: dict[tuple[str, str], list[Dict[str, Any]]] = {}
        for row in rows:
            retry_profile_name = str(row.get("retry_profile", "") or "").strip().lower() or "adaptive"
            retry_strategy_name = str(row.get("retry_strategy", "") or "").strip().lower() or "unknown"
            grouped.setdefault((retry_profile_name, retry_strategy_name), []).append(row)
        items: list[Dict[str, Any]] = []
        profile_scores: dict[str, Dict[str, Any]] = {}
        for (retry_profile_name, retry_strategy_name), strategy_rows in grouped.items():
            attempt_count = len(strategy_rows)
            recent_rows = strategy_rows[-bounded_recent_window:]
            failure_count = sum(1 for row in strategy_rows if bool(row.get("failure_like", False)))
            success_count = sum(
                1
                for row in strategy_rows
                if str(row.get("status", "")).strip().lower() == "success" and not bool(row.get("failure_like", False))
            )
            fallback_count = sum(1 for row in strategy_rows if bool(row.get("fallback_applied", False)))
            avg_delay_ms = sum(max(0, int(row.get("retry_delay_ms", 0) or 0)) for row in strategy_rows) / max(1, attempt_count)
            recent_failure_rate = sum(1 for row in recent_rows if bool(row.get("failure_like", False))) / max(1, len(recent_rows))
            failure_rate = failure_count / max(1, attempt_count)
            success_rate = success_count / max(1, attempt_count)
            score = round(max(4.0, min(100.0, 100.0 - (failure_rate * 46.0) - (recent_failure_rate * 24.0) - min(10.0, avg_delay_ms / 180.0) + (success_rate * 8.0))), 4)
            unstable = bool(len(recent_rows) >= 2 and recent_failure_rate >= 0.5)
            demoted = bool(attempt_count >= 4 and (recent_failure_rate >= 0.67 or failure_rate >= 0.72 or score <= 28.0))
            item = {
                "retry_profile": retry_profile_name,
                "retry_strategy": retry_strategy_name,
                "attempt_count": attempt_count,
                "success_count": success_count,
                "failure_count": failure_count,
                "failure_rate": round(failure_rate, 4),
                "recent_failure_rate": round(recent_failure_rate, 4),
                "avg_delay_ms": round(avg_delay_ms, 2),
                "score": score,
                "unstable": unstable,
                "demoted": demoted,
            }
            items.append(item)
            rollup = profile_scores.setdefault(
                retry_profile_name,
                {"retry_profile": retry_profile_name, "attempt_count": 0, "weighted_score_total": 0.0, "demoted_count": 0, "unstable_count": 0},
            )
            rollup["attempt_count"] = int(rollup.get("attempt_count", 0) or 0) + attempt_count
            rollup["weighted_score_total"] = float(rollup.get("weighted_score_total", 0.0) or 0.0) + (score * attempt_count)
            if demoted:
                rollup["demoted_count"] = int(rollup.get("demoted_count", 0) or 0) + 1
            if unstable:
                rollup["unstable_count"] = int(rollup.get("unstable_count", 0) or 0) + 1
        items.sort(key=lambda item: (0 if not bool(item.get("demoted", False)) else 1, -float(item.get("score", 0.0) or 0.0), -int(item.get("attempt_count", 0) or 0)))
        profile_items: list[Dict[str, Any]] = []
        for retry_profile_name, row in profile_scores.items():
            attempt_count = max(1, int(row.get("attempt_count", 0) or 0))
            profile_items.append(
                {
                    "retry_profile": retry_profile_name,
                    "attempt_count": attempt_count,
                    "score": round(float(row.get("weighted_score_total", 0.0) or 0.0) / attempt_count, 4),
                    "demoted_count": int(row.get("demoted_count", 0) or 0),
                    "unstable_count": int(row.get("unstable_count", 0) or 0),
                }
            )
        profile_items.sort(key=lambda item: (0 if int(item.get("demoted_count", 0) or 0) <= 0 else 1, -float(item.get("score", 0.0) or 0.0), -int(item.get("attempt_count", 0) or 0)))
        return {
            "status": "success",
            "recommended_retry_profile": str(profile_items[0].get("retry_profile", "") or "").strip().lower() if profile_items else "",
            "items": items[:8],
            "profile_scores": profile_items[:6],
        }

    def model_bridge_profiles(self, *, task: str = "", limit: int = 64) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 256))
        clean_task = str(task or "").strip().lower()
        profiles = [
            {
                "profile_id": "reasoning-bridge-local-auto-reasoning-qwen3-14b",
                "bridge_kind": "reasoning",
                "task": "reasoning",
                "name": "local-auto-reasoning-qwen3-14b",
                "title": "Reasoning Bridge: qwen3-14b-q8_0.gguf",
                "detected_model_path": "E:/J.A.R.V.I.S/all_rounder/Qwen3-14B-GGUF/qwen3-14b-q8_0.gguf",
                "family": "qwen",
                "backend": "llama_cpp",
                "format": ".gguf",
                "quality": 92,
                "latency": 188.0,
                "penalty": 0.04,
                "recommended": True,
                "apply_supported": True,
                "command_available": True,
                "command_template": '"llama-server" -m "E:/J.A.R.V.I.S/all_rounder/Qwen3-14B-GGUF/qwen3-14b-q8_0.gguf" --host 127.0.0.1 --port 8080 -c 4096',
                "recommended_endpoint": "http://127.0.0.1:8080",
                "recommended_healthcheck_url": "http://127.0.0.1:8080/health",
                "launch_templates": [
                    {
                        "template_id": "reasoning-llama-server-local-auto-reasoning-qwen3-14b",
                        "title": "Managed llama-server bridge",
                        "launcher": "llama-server",
                        "transport": "openai_chat",
                        "ready": True,
                        "recommended": True,
                        "command": '"llama-server" -m "E:/J.A.R.V.I.S/all_rounder/Qwen3-14B-GGUF/qwen3-14b-q8_0.gguf" --host 127.0.0.1 --port 8080 -c 4096',
                        "working_directory": "E:/J.A.R.V.I.S/all_rounder/Qwen3-14B-GGUF",
                        "endpoint": "http://127.0.0.1:8080",
                        "healthcheck_url": "http://127.0.0.1:8080/health",
                        "manual_only": False,
                        "autostart_capable": True,
                        "command_status": {"executable": "llama-server", "executable_available": True, "resolved_path": "llama-server"},
                        "profile_patch": {"endpoint": "http://127.0.0.1:8080"},
                        "missing_requirements": [],
                        "remediation_hints": [],
                        "notes": ["Starts a local OpenAI-compatible reasoning server for the managed bridge."],
                    },
                    {
                        "template_id": "reasoning-endpoint-local-auto-reasoning-qwen3-14b",
                        "title": "Existing OpenAI-compatible endpoint",
                        "launcher": "manual_endpoint",
                        "transport": "openai_chat",
                        "ready": True,
                        "recommended": False,
                        "command": "",
                        "working_directory": "",
                        "endpoint": "http://127.0.0.1:8080",
                        "healthcheck_url": "http://127.0.0.1:8080/health",
                        "manual_only": True,
                        "autostart_capable": False,
                        "command_status": {"executable": "", "executable_available": False, "resolved_path": ""},
                        "profile_patch": {"endpoint": "http://127.0.0.1:8080"},
                        "missing_requirements": [],
                        "remediation_hints": [],
                        "notes": ["Use this when another launcher already hosts the model behind an OpenAI-style API."],
                    },
                ],
                "recommended_launch_template_id": "reasoning-llama-server-local-auto-reasoning-qwen3-14b",
                "launch_ready_count": 3,
                "launch_missing_requirements": [],
                "launch_remediation_hints": [],
                "override_patch": {
                    "endpoint": "http://127.0.0.1:8080",
                    "api_mode": "openai_chat",
                    "model_hint": "qwen3-14b-q8_0",
                    "server_cwd": "E:/J.A.R.V.I.S/all_rounder/Qwen3-14B-GGUF",
                    "server_command": '"llama-server" -m "E:/J.A.R.V.I.S/all_rounder/Qwen3-14B-GGUF/qwen3-14b-q8_0.gguf" --host 127.0.0.1 --port 8080 -c 4096',
                    "autostart": True,
                },
                "bridge_ready": True,
                "bridge_running": True,
                "cloud_route_fallbacks": {"groq": True, "nvidia": True},
                "notes": ["Applies a live session override to the managed reasoning bridge."],
            },
            {
                "profile_id": "tts-bridge-orpheus-3b-tts-f16",
                "bridge_kind": "tts",
                "task": "tts",
                "name": "orpheus-3b-tts-f16",
                "title": "Neural TTS Bridge: Orpheus-3B-TTS.f16.gguf",
                "detected_model_path": "E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf",
                "family": "orpheus",
                "backend": "llama_cpp",
                "format": ".gguf",
                "quality": 90,
                "latency": 44.0,
                "penalty": 0.03,
                "recommended": True,
                "apply_supported": True,
                "restart_supported": True,
                "execution_backend": "openai_http",
                "command_available": True,
                "command_template": "python -m local_tts_server --model {model_path_q}",
                "recommended_endpoint": "http://127.0.0.1:5055/v1/audio/speech",
                "recommended_healthcheck_url": "http://127.0.0.1:5055/health",
                "launch_templates": [
                    {
                        "template_id": "tts-http-bridge-orpheus-3b-tts-f16",
                        "title": "Managed local HTTP speech bridge",
                        "launcher": "tts_http_bridge",
                        "transport": "openai_audio_speech",
                        "ready": True,
                        "recommended": True,
                        "command": "python -m local_tts_server --model E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf",
                        "working_directory": "E:/J.A.R.V.I.S/tts",
                        "endpoint": "http://127.0.0.1:5055/v1/audio/speech",
                        "healthcheck_url": "http://127.0.0.1:5055/health",
                        "manual_only": False,
                        "autostart_capable": True,
                        "command_status": {"executable": "python", "executable_available": True, "resolved_path": "python"},
                        "profile_patch": {"execution_backend": "openai_http"},
                        "missing_requirements": [],
                        "remediation_hints": [],
                        "notes": ["Uses the managed neural TTS bridge so the backend can supervise the local server."],
                    },
                    {
                        "template_id": "tts-existing-endpoint-orpheus-3b-tts-f16",
                        "title": "Existing neural TTS endpoint",
                        "launcher": "manual_endpoint",
                        "transport": "openai_audio_speech",
                        "ready": True,
                        "recommended": False,
                        "command": "",
                        "working_directory": "E:/J.A.R.V.I.S/tts",
                        "endpoint": "http://127.0.0.1:5055/v1/audio/speech",
                        "healthcheck_url": "http://127.0.0.1:5055/health",
                        "manual_only": True,
                        "autostart_capable": False,
                        "command_status": {"executable": "", "executable_available": False, "resolved_path": ""},
                        "profile_patch": {
                            "execution_backend": "openai_http",
                            "endpoint": "http://127.0.0.1:5055/v1/audio/speech",
                            "http_endpoint": "http://127.0.0.1:5055/v1/audio/speech",
                            "healthcheck_url": "http://127.0.0.1:5055/health",
                            "autostart": False,
                        },
                        "missing_requirements": [],
                        "remediation_hints": ["Start your local TTS server manually and keep the endpoint healthy before applying this profile."],
                        "notes": ["Use this when another process already hosts the TTS model behind an OpenAI-style speech endpoint."],
                    },
                    {
                        "template_id": "tts-command-runtime-orpheus-3b-tts-f16",
                        "title": "Direct command synthesis runtime",
                        "launcher": "command_runtime",
                        "transport": "direct_command",
                        "ready": True,
                        "recommended": False,
                        "command": "python -m local_tts_server --model {model_path_q}",
                        "working_directory": "E:/J.A.R.V.I.S/tts",
                        "endpoint": "",
                        "healthcheck_url": "",
                        "manual_only": False,
                        "autostart_capable": False,
                        "command_status": {"executable": "python", "executable_available": True, "resolved_path": "python"},
                        "profile_patch": {"execution_backend": "command"},
                        "missing_requirements": [],
                        "remediation_hints": [],
                        "notes": ["Runs a custom synthesis command without the managed HTTP bridge."],
                    },
                ],
                "recommended_launch_template_id": "tts-http-bridge-orpheus-3b-tts-f16",
                "launch_ready_count": 2,
                "launch_missing_requirements": [],
                "launch_remediation_hints": [],
                "override_patch": {
                    "enabled": True,
                    "autostart": True,
                    "model_path": "E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf",
                    "model_label": "Orpheus-3B-TTS",
                    "backend": "llama_cpp",
                    "execution_backend": "openai_http",
                    "endpoint": "http://127.0.0.1:5055/v1/audio/speech",
                    "http_endpoint": "http://127.0.0.1:5055/v1/audio/speech",
                    "healthcheck_url": "http://127.0.0.1:5055/health",
                    "server_command": "python -m local_tts_server --model E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf",
                    "server_cwd": "E:/J.A.R.V.I.S/tts",
                    "http_model": "orpheus-3b-tts",
                    "voice": "jarvis",
                    "output_format": "wav",
                    "timeout_s": 120.0,
                },
                "bridge_ready": True,
                "bridge_running": True,
                "launch_validation": {
                    "execution_backend": "openai_http",
                    "bridge_required": True,
                    "endpoint_configured": True,
                    "server_command_configured": True,
                    "command_template_configured": True,
                    "model_exists": True,
                    "issues": [],
                },
                "cloud_route_fallbacks": {"elevenlabs": True, "nvidia": True},
                "notes": ["Applies a live runtime override to the local neural TTS stack."],
            },
            {
                "profile_id": "stt-runtime-whisper-large-v3",
                "bridge_kind": "stt",
                "task": "stt",
                "name": "whisper-large-v3",
                "title": "Local STT Runtime: whisper-large-v3(Speech-To-text_model)",
                "detected_model_path": "E:/J.A.R.V.I.S/stt/whisper-large-v3(Speech-To-text_model)",
                "family": "whisper",
                "backend": "transformers",
                "format": "directory",
                "quality": 88,
                "latency": 72.0,
                "penalty": 0.05,
                "recommended": True,
                "apply_supported": True,
                "command_available": True,
                "command_template": "",
                "recommended_endpoint": "",
                "recommended_healthcheck_url": "",
                "launch_templates": [
                    {
                        "template_id": "stt-local-runtime-whisper-large-v3",
                        "title": "Managed local Whisper runtime",
                        "launcher": "stt_local_runtime",
                        "transport": "inprocess_streaming",
                        "ready": True,
                        "recommended": True,
                        "command": "",
                        "working_directory": "E:/J.A.R.V.I.S/stt",
                        "endpoint": "",
                        "healthcheck_url": "",
                        "manual_only": False,
                        "autostart_capable": False,
                        "command_status": {"executable": "", "executable_available": False, "resolved_path": ""},
                        "profile_patch": {
                            "model": "whisper-large-v3",
                            "model_label": "whisper-large-v3",
                            "local_model_path": "E:/J.A.R.V.I.S/stt/whisper-large-v3(Speech-To-text_model)",
                            "provider_preference": "local",
                        },
                        "missing_requirements": [],
                        "remediation_hints": [],
                        "notes": ["Rebuilds the in-process STT engine against this local model and preserves fallback policy."],
                    }
                ],
                "recommended_launch_template_id": "stt-local-runtime-whisper-large-v3",
                "launch_ready_count": 1,
                "launch_missing_requirements": [],
                "launch_remediation_hints": [],
                "override_patch": {
                    "model": "whisper-large-v3",
                    "model_label": "whisper-large-v3",
                    "local_model_path": "E:/J.A.R.V.I.S/stt/whisper-large-v3(Speech-To-text_model)",
                    "provider_preference": "local",
                },
                "bridge_ready": True,
                "bridge_running": False,
                "active_profile": False,
                "active_template": "",
                "selected_model_path": "E:/J.A.R.V.I.S/stt/whisper-large-v3(Speech-To-text_model)",
                "launch_validation": {
                    "mode": "inprocess_stt_runtime",
                    "model_exists": True,
                    "engine_available": True,
                    "voice_running": False,
                    "issues": [],
                },
                "cloud_route_fallbacks": {"groq": True},
                "notes": ["Applies the selected local STT model directly to the managed voice stack."],
            },
            {
                "profile_id": "vision-runtime-yolov10x",
                "bridge_kind": "vision",
                "task": "vision",
                "name": "yolov10x",
                "title": "Vision Runtime: yolov10x.pt",
                "detected_model_path": "E:/J.A.R.V.I.S/JARVIS_BACKEND/models/vision/yolov10x.pt",
                "family": "yolo",
                "backend": "torch",
                "format": ".pt",
                "quality": 86,
                "latency": 92.0,
                "penalty": 0.08,
                "recommended": True,
                "apply_supported": True,
                "command_available": True,
                "command_template": "",
                "recommended_endpoint": "",
                "recommended_healthcheck_url": "",
                "launch_templates": [
                    {
                        "template_id": "vision-warm-yolo-yolov10x",
                        "title": "Warm managed vision runtime",
                        "launcher": "vision_runtime",
                        "transport": "inprocess_vision",
                        "ready": True,
                        "recommended": True,
                        "command": "",
                        "working_directory": "E:/J.A.R.V.I.S/JARVIS_BACKEND/models/vision",
                        "endpoint": "",
                        "healthcheck_url": "",
                        "manual_only": False,
                        "autostart_capable": False,
                        "command_status": {"executable": "", "executable_available": False, "resolved_path": ""},
                        "profile_patch": {"models": ["yolo"], "force_reload": False, "clear_cache": False},
                        "missing_requirements": [],
                        "remediation_hints": [],
                        "notes": ["Loads the selected local vision runtime into the managed perception engine."],
                    },
                    {
                        "template_id": "vision-reload-yolo-yolov10x",
                        "title": "Reload managed vision runtime",
                        "launcher": "vision_reload",
                        "transport": "inprocess_vision",
                        "ready": True,
                        "recommended": False,
                        "command": "",
                        "working_directory": "E:/J.A.R.V.I.S/JARVIS_BACKEND/models/vision",
                        "endpoint": "",
                        "healthcheck_url": "",
                        "manual_only": False,
                        "autostart_capable": False,
                        "command_status": {"executable": "", "executable_available": False, "resolved_path": ""},
                        "profile_patch": {"models": ["yolo"], "force_reload": True, "clear_cache": True},
                        "missing_requirements": [],
                        "remediation_hints": ["Use reload when the runtime is stale or when switching artifacts."],
                        "notes": ["Clears caches and force reloads the selected local vision runtime."],
                    },
                ],
                "recommended_launch_template_id": "vision-warm-yolo-yolov10x",
                "launch_ready_count": 2,
                "launch_missing_requirements": [],
                "launch_remediation_hints": [],
                "override_patch": {"models": ["yolo"], "force_reload": False, "clear_cache": False},
                "bridge_ready": True,
                "bridge_running": True,
                "active_profile": False,
                "active_template": "",
                "runtime_targets": ["yolo"],
                "launch_validation": {"runtime_targets": ["yolo"], "artifact_exists": True, "supported": True, "issues": []},
                "cloud_route_fallbacks": {"nvidia": True},
                "notes": ["Routes supported local vision artifacts into the managed perception engine."],
            },
        ]
        if clean_task:
            profiles = [row for row in profiles if str(row.get("task", "")).strip().lower() == clean_task]
        launch_template_count = 0
        launch_ready_count = 0
        launch_stable_ready_count = 0
        launch_unstable_count = 0
        launch_demoted_count = 0
        launch_suppressed_count = 0
        launch_blacklisted_count = 0
        autonomous_ready_template_count = 0
        launch_history_count = 0
        recommended_shift_count = 0
        top_unstable: list[Dict[str, Any]] = []
        for profile in profiles:
            if not isinstance(profile, dict):
                continue
            templates = profile.get("launch_templates", [])
            annotated_templates, launch_summary = self._annotate_launch_templates(
                profile_id=str(profile.get("profile_id", "")),
                bridge_kind=str(profile.get("bridge_kind", "")),
                templates=[dict(item) for item in templates if isinstance(item, dict)] if isinstance(templates, list) else [],
            )
            profile["launch_templates"] = annotated_templates
            profile["recommended_launch_template_id"] = str(launch_summary.get("recommended_template_id", "")).strip()
            profile["launch_ready_count"] = int(launch_summary.get("ready_count", 0) or 0)
            profile["launch_stable_ready_count"] = int(launch_summary.get("stable_ready_count", 0) or 0)
            profile["launch_unstable_count"] = int(launch_summary.get("unstable_count", 0) or 0)
            profile["launch_demoted_count"] = int(launch_summary.get("demoted_count", 0) or 0)
            profile["launch_suppressed_count"] = int(launch_summary.get("suppressed_count", 0) or 0)
            profile["launch_blacklisted_count"] = int(launch_summary.get("blacklisted_count", 0) or 0)
            profile["launch_missing_requirements"] = list(launch_summary.get("missing_requirements", []))
            profile["launch_remediation_hints"] = list(launch_summary.get("remediation_hints", []))
            profile["launch_health"] = dict(launch_summary)
            launch_template_count += int(launch_summary.get("template_count", 0) or 0)
            launch_ready_count += int(launch_summary.get("ready_count", 0) or 0)
            launch_stable_ready_count += int(launch_summary.get("stable_ready_count", 0) or 0)
            launch_unstable_count += int(launch_summary.get("unstable_count", 0) or 0)
            launch_demoted_count += int(launch_summary.get("demoted_count", 0) or 0)
            launch_suppressed_count += int(launch_summary.get("suppressed_count", 0) or 0)
            launch_blacklisted_count += int(launch_summary.get("blacklisted_count", 0) or 0)
            autonomous_ready_template_count += int(launch_summary.get("autonomy_ready_count", 0) or 0)
            launch_history_count += int(launch_summary.get("history_count", 0) or 0)
            if bool(launch_summary.get("recommended_shifted", False)):
                recommended_shift_count += 1
            for template in annotated_templates:
                health = template.get("health", {}) if isinstance(template.get("health"), dict) else {}
                if not bool(health.get("unstable", False)) and not bool(health.get("demoted", False)):
                    continue
                top_unstable.append(
                    {
                        "profile_id": str(profile.get("profile_id", "")).strip(),
                        "template_id": str(template.get("template_id", "")).strip(),
                        "bridge_kind": str(profile.get("bridge_kind", "")).strip(),
                        "health_score": float(health.get("health_score", 0.0) or 0.0),
                        "failure_rate": float(health.get("failure_rate", 0.0) or 0.0),
                        "failure_streak": int(health.get("failure_streak", 0) or 0),
                        "demoted": bool(health.get("demoted", False)),
                    }
                )
        top_unstable.sort(
            key=lambda row: (
                0 if bool(row.get("demoted", False)) else 1,
                -float(row.get("failure_rate", 0.0) or 0.0),
                -int(row.get("failure_streak", 0) or 0),
                float(row.get("health_score", 0.0) or 0.0),
                str(row.get("template_id", "")),
            )
        )
        return {
            "status": "success",
            "task": clean_task,
            "count": len(profiles),
            "limit": bounded,
            "profiles": profiles[:bounded],
            "launch_history": {
                "count": min(len(self.launch_template_history), 18),
                "total": len(self.launch_template_history),
                "items": [dict(item) for item in self.launch_template_history[-18:]],
            },
            "launch_health_summary": {
                "profile_count": len(profiles),
                "template_count": launch_template_count,
                "ready_template_count": launch_ready_count,
                "stable_ready_template_count": launch_stable_ready_count,
                "unstable_template_count": launch_unstable_count,
                "demoted_template_count": launch_demoted_count,
                "suppressed_template_count": launch_suppressed_count,
                "blacklisted_template_count": launch_blacklisted_count,
                "autonomous_ready_template_count": autonomous_ready_template_count,
                "history_count": launch_history_count,
                "recommended_shift_count": recommended_shift_count,
                "top_unstable_templates": top_unstable[:8],
            },
            "reasoning_bridge": dict(self.local_reasoning_bridge_state),
            "tts_bridge": dict(self.tts_bridge_state),
            "stt_runtime_profile": dict(self.stt_runtime_profile_state),
            "vision_runtime_profile": dict(self.vision_runtime_profile_state),
            "stt_diagnostics": {
                "status": "success",
                "available": True,
                "provider_health": self.stt_policy_state.get("provider_health", "healthy"),
                "last_model": self.stt_runtime_profile_state.get("model", ""),
                "runtime_profile": dict(self.stt_runtime_profile_state),
            },
            "vision_runtime": dict(self.vision_runtime_state),
            "provider_credentials": {
                "status": "success",
                "providers": {
                    "groq": {"provider": "groq", "ready": True, "present": True},
                    "nvidia": {"provider": "nvidia", "ready": True, "present": True},
                    "elevenlabs": {"provider": "elevenlabs", "ready": True, "present": True},
                },
            },
            "cloud_readiness": {"groq": True, "nvidia": True, "elevenlabs": True},
            "diagnostics": {
                "llama_server_detected": True,
                "llama_server_path": "llama-server",
                "python_detected": True,
                "python_path": "python",
                "coqui_cli_detected": False,
                "coqui_cli_path": "",
            },
        }

    def model_route_bundle(
        self,
        *,
        stack_name: str = "desktop_agent",
        tasks: list[str] | None = None,
        requires_offline: bool = False,
        privacy_mode: bool = False,
        latency_sensitive: bool = False,
        mission_profile: str = "balanced",
        cost_sensitive: bool = False,
        max_cost_units: float | None = None,
    ) -> Dict[str, Any]:
        del latency_sensitive, cost_sensitive, max_cost_units
        provider_credentials = {
            "status": "success",
            "providers": {
                "groq": {"provider": "groq", "ready": True, "present": True},
                "nvidia": {"provider": "nvidia", "ready": True, "present": True},
                "elevenlabs": {"provider": "elevenlabs", "ready": True, "present": True},
                "local": {"provider": "local", "ready": True, "present": True},
            },
        }
        protected_tasks = {"wakeword", "stt", "tts", "embedding", "intent", "vision"}
        monitored_tasks = {"reasoning", "stt", "tts", "vision"}
        profile_catalog = self.model_bridge_profiles(limit=192)
        profile_rows = profile_catalog.get("profiles", []) if isinstance(profile_catalog.get("profiles"), list) else []
        requested = tasks if isinstance(tasks, list) and tasks else (
            ["wakeword", "stt", "reasoning", "tts"] if stack_name == "voice" else ["reasoning", "embedding", "intent", "vision", "wakeword", "stt", "tts"]
        )
        items = []
        selected_local_paths: Dict[str, str] = {}
        provider_counts: Dict[str, int] = {}
        warnings: list[str] = []
        launch_policy_summary: Dict[str, int] = {
            "local_provider_task_count": 0,
            "policy_monitored_task_count": 0,
            "matched_policy_task_count": 0,
            "unmatched_policy_task_count": 0,
            "local_viable_task_count": 0,
            "autonomy_safe_task_count": 0,
            "review_required_task_count": 0,
            "blacklisted_task_count": 0,
            "rerouted_task_count": 0,
            "blocked_task_count": 0,
            "protected_local_task_count": 0,
            "recovery_pending_task_count": 0,
        }
        for index, task_name in enumerate(requested, start=1):
            provider = "local"
            model = f"local-auto-{task_name}-model"
            selected_path = ""
            if task_name == "reasoning" and not requires_offline and not privacy_mode:
                provider = "groq"
                model = "groq-llm"
            elif task_name == "tts" and not privacy_mode and not requires_offline:
                provider = "elevenlabs"
                model = "elevenlabs-tts"
            if task_name == "stt":
                selected_path = "E:/J.A.R.V.I.S/stt/whisper-large-v3(Speech-To-text_model)"
                model = "local-auto-stt-whisper-large-v3"
            elif task_name == "wakeword":
                selected_path = "E:/J.A.R.V.I.S/wakeword/Jarvis_en_windows_v4_0_0.ppn"
                model = "local-auto-wakeword-jarvis-en-windows-v4-0-0"
            elif task_name == "embedding":
                selected_path = "E:/J.A.R.V.I.S/embeddings/all-mpnet-base-v2(Embeddings_model)"
                model = "local-auto-embedding-all-mpnet-base-v2"
            elif task_name == "intent":
                selected_path = "E:/J.A.R.V.I.S/custom_intents/bart-large-mnli (Custom_intent_model)"
                model = "local-auto-intent-bart-large-mnli"
            elif task_name == "reasoning" and provider == "local":
                selected_path = "E:/J.A.R.V.I.S/all_rounder/Qwen3-14B-GGUF/qwen3-14b-q8_0.gguf"
                model = "local-auto-reasoning-qwen3-14b"
            elif task_name == "tts" and provider == "local":
                selected_path = "E:/J.A.R.V.I.S/tts/Orpheus-3B-TTS.f16.gguf"
                model = "local-auto-tts-orpheus-3b-tts-f16"
            row: Dict[str, Any] = {
                "index": index,
                "status": "success",
                "task": task_name,
                "model": model,
                "provider": provider,
                "score": 2.1,
                "fallback_chain": ["local", "nvidia"] if provider != "local" else ["groq", "nvidia"],
                "alternatives": [],
                "selected_path": selected_path,
                "metadata": {"path": selected_path} if selected_path else {},
                "diagnostics": {"top_candidates": [{"provider": provider, "model": model}]},
            }
            if provider == "local":
                launch_policy_summary["local_provider_task_count"] += 1
                if task_name in monitored_tasks:
                    launch_policy_summary["policy_monitored_task_count"] += 1
                    matched_profile = next(
                        (
                            profile
                            for profile in profile_rows
                            if isinstance(profile, dict)
                            and str(profile.get("task", "")).strip().lower() == str(task_name).strip().lower()
                            and (
                                str(profile.get("detected_model_path", "")).strip().lower() == selected_path.lower()
                                or str(profile.get("name", "")).strip().lower() in str(model).strip().lower()
                            )
                        ),
                        None,
                    )
                    if isinstance(matched_profile, dict):
                        launch_policy_summary["matched_policy_task_count"] += 1
                        launch_health = matched_profile.get("launch_health", {}) if isinstance(matched_profile.get("launch_health"), dict) else {}
                        recommended_template_id = str(matched_profile.get("recommended_launch_template_id", "") or "").strip().lower()
                        recommended_template = next(
                            (
                                template
                                for template in matched_profile.get("launch_templates", [])
                                if isinstance(template, dict)
                                and str(template.get("template_id", "")).strip().lower() == recommended_template_id
                            ),
                            matched_profile.get("launch_templates", [])[0] if isinstance(matched_profile.get("launch_templates"), list) and matched_profile.get("launch_templates") else {},
                        )
                        template_health = recommended_template.get("health", {}) if isinstance(recommended_template, dict) and isinstance(recommended_template.get("health"), dict) else {}
                        autonomy_policy = template_health.get("autonomy_policy", {}) if isinstance(template_health.get("autonomy_policy"), dict) else {}
                        blacklisted = bool(recommended_template.get("blacklisted", template_health.get("blacklisted", False))) if isinstance(recommended_template, dict) else False
                        demoted = bool(recommended_template.get("demoted", template_health.get("demoted", False))) if isinstance(recommended_template, dict) else False
                        suppressed = bool(recommended_template.get("suppressed", template_health.get("suppressed", False))) if isinstance(recommended_template, dict) else False
                        launch_ready_count = int(launch_health.get("ready_count", matched_profile.get("launch_ready_count", 0)) or 0)
                        stable_ready_count = int(launch_health.get("stable_ready_count", matched_profile.get("launch_stable_ready_count", 0)) or 0)
                        autonomy_ready_count = int(launch_health.get("autonomy_ready_count", 0) or 0)
                        autonomous_allowed = bool(autonomy_policy.get("autonomous_allowed", launch_ready_count > 0 and not blacklisted and not demoted and not suppressed))
                        review_required = bool(autonomy_policy.get("review_required", blacklisted or demoted or suppressed))
                        cooldown_hint_s = int(autonomy_policy.get("cooldown_hint_s", 0) or 0)
                        local_route_viable = launch_ready_count > 0 and not blacklisted and not demoted
                        autonomy_safe = stable_ready_count > 0 and autonomy_ready_count > 0 and autonomous_allowed and not suppressed and not blacklisted
                        if local_route_viable:
                            launch_policy_summary["local_viable_task_count"] += 1
                        if autonomy_safe:
                            launch_policy_summary["autonomy_safe_task_count"] += 1
                        if review_required:
                            launch_policy_summary["review_required_task_count"] += 1
                        if blacklisted:
                            launch_policy_summary["blacklisted_task_count"] += 1
                        if cooldown_hint_s > 0:
                            launch_policy_summary["recovery_pending_task_count"] += 1
                        if privacy_mode and task_name in protected_tasks:
                            launch_policy_summary["protected_local_task_count"] += 1
                        route_reason_code = ""
                        if blacklisted:
                            route_reason_code = "local_launch_template_blacklisted"
                        elif demoted:
                            route_reason_code = "local_launch_template_demoted"
                        elif suppressed:
                            route_reason_code = "local_launch_template_suppressed"
                        elif launch_ready_count <= 0:
                            route_reason_code = "local_launch_template_unready"
                        elif review_required:
                            route_reason_code = "local_launch_review_required"
                        fallback_candidates = [
                            provider_name
                            for provider_name, ready in (matched_profile.get("cloud_route_fallbacks", {}) if isinstance(matched_profile.get("cloud_route_fallbacks"), dict) else {}).items()
                            if bool(ready)
                        ]
                        row["route_policy"] = {
                            "status": "matched",
                            "matched": True,
                            "policy_monitored": True,
                            "bridge_kind": str(matched_profile.get("bridge_kind", task_name) or task_name).strip().lower(),
                            "profile_id": str(matched_profile.get("profile_id", "") or "").strip(),
                            "recommended_template_id": str((recommended_template or {}).get("template_id", "") or "").strip(),
                            "launch_ready_count": launch_ready_count,
                            "launch_stable_ready_count": stable_ready_count,
                            "launch_blacklisted_count": int(launch_health.get("blacklisted_count", 0) or 0),
                            "autonomous_ready_count": autonomy_ready_count,
                            "local_route_viable": bool(local_route_viable),
                            "autonomy_safe": bool(autonomy_safe),
                            "autonomous_allowed": bool(autonomous_allowed),
                            "review_required": bool(review_required),
                            "blacklisted": bool(blacklisted),
                            "suppressed": bool(suppressed),
                            "demoted": bool(demoted),
                            "cooldown_hint_s": cooldown_hint_s,
                            "recovery_pending": bool(cooldown_hint_s > 0 and not autonomy_safe),
                            "cloud_fallback_candidates": fallback_candidates,
                            "reason_code": route_reason_code,
                            "reason": (
                                f"Local {task_name} route matched profile '{str(matched_profile.get('profile_id', '') or '').strip()}' "
                                f"with autonomy_allowed={autonomous_allowed} blacklisted={blacklisted}."
                            ),
                        }
                        row["local_launch_profile_id"] = str(matched_profile.get("profile_id", "") or "").strip()
                        row["local_launch_template_id"] = str((recommended_template or {}).get("template_id", "") or "").strip()
                        if route_reason_code and not requires_offline and not (privacy_mode and task_name in protected_tasks) and fallback_candidates:
                            fallback_provider = str(fallback_candidates[0]).strip().lower()
                            fallback_model = f"{fallback_provider}-{task_name}"
                            if task_name == "reasoning" and fallback_provider == "groq":
                                fallback_model = "groq-llm"
                            elif task_name == "reasoning" and fallback_provider == "nvidia":
                                fallback_model = "nvidia-nim"
                            elif task_name == "tts" and fallback_provider == "elevenlabs":
                                fallback_model = "elevenlabs-tts"
                            row["requested_provider"] = provider
                            row["requested_model"] = model
                            row["requested_selected_path"] = selected_path
                            row["provider"] = fallback_provider
                            row["model"] = fallback_model
                            row["selected_path"] = ""
                            row["metadata"] = {}
                            row["route_adjusted"] = True
                            row["route_blocked"] = False
                            row["route_adjustment_reason"] = route_reason_code
                            row["route_warning"] = f"task:{task_name}:{route_reason_code}:rerouted_to:{fallback_provider}"
                            launch_policy_summary["rerouted_task_count"] += 1
                            warnings.append(str(row["route_warning"]))
                        elif route_reason_code:
                            row["route_adjusted"] = False
                            row["route_blocked"] = True
                            row["route_adjustment_reason"] = route_reason_code
                            row["route_warning"] = f"task:{task_name}:{route_reason_code}:no_safe_reroute"
                            launch_policy_summary["blocked_task_count"] += 1
                            warnings.append(str(row["route_warning"]))
                    else:
                        launch_policy_summary["unmatched_policy_task_count"] += 1
                        row["route_policy"] = {
                            "status": "unmatched",
                            "matched": False,
                            "policy_monitored": True,
                            "bridge_kind": task_name,
                            "reason_code": "launch_profile_unmatched",
                            "reason": "No matching fake launch profile was found for the selected local route.",
                        }
                        row["route_warning"] = f"task:{task_name}:launch_profile_unmatched"
                        warnings.append(str(row["route_warning"]))
            current_provider = str(row.get("provider", "") or "").strip().lower()
            if current_provider == "local" and selected_path:
                selected_local_paths[task_name] = selected_path
            provider_counts[current_provider] = int(provider_counts.get(current_provider, 0)) + 1
            items.append(row)
        success_count = len(items)
        provider_distribution = {
            name: round(float(count) / max(1.0, float(success_count)), 6)
            for name, count in provider_counts.items()
            if success_count > 0
        }
        return {
            "status": "success",
            "stack_name": stack_name,
            "count": len(items),
            "success_count": len(items),
            "error_count": 0,
            "requires_offline": bool(requires_offline),
            "privacy_mode": bool(privacy_mode),
            "latency_sensitive": False,
            "mission_profile": mission_profile,
            "provider_counts": provider_counts,
            "provider_distribution": provider_distribution,
            "selected_local_paths": selected_local_paths,
            "items": items,
            "warnings": warnings,
            "launch_policy_summary": launch_policy_summary,
            "capabilities": self.model_capability_summary(limit_per_task=3),
            "provider_credentials": provider_credentials,
        }

    def model_connector_diagnostics(
        self,
        *,
        include_route_plan: bool = True,
        requires_offline: bool = False,
        privacy_mode: bool = False,
        mission_profile: str = "balanced",
    ) -> Dict[str, Any]:
        voice_route_policy = {
            "mission_id": "voice-mission-1",
            "policy_profile": "balanced",
            "risk_level": "medium",
            "ban_local_reasoning": not bool(requires_offline),
            "preferred_reasoning_provider": "groq" if not bool(requires_offline) else "local",
            "local_voice_pressure_score": 0.63 if not bool(requires_offline) else 0.18,
            "reason_code": "voice_route_policy_pressure",
        }
        payload = {
            "status": "success",
            "policy": dict(self.model_connector_policy),
            "provider_snapshot": {
                "groq": {"provider": "groq", "ready": True, "present": True},
                "nvidia": {"provider": "nvidia", "ready": True, "present": True},
                "local": {"provider": "local", "ready": True, "present": True, "local_candidate_count": 2},
            },
            "providers": {
                "groq": {"failure_ema": 0.08, "failure_streak": 0},
                "nvidia": {"failure_ema": 0.12, "failure_streak": 0},
                "local": {"failure_ema": 0.03, "failure_streak": 0},
            },
            "voice_route_policy": voice_route_policy,
            "route_plan": self.model_connector_route_plan(
                requires_offline=requires_offline,
                privacy_mode=privacy_mode,
                mission_profile=mission_profile,
            ) if include_route_plan else {},
        }
        return payload

    def model_connector_route_plan(
        self,
        *,
        requires_offline: bool = False,
        privacy_mode: bool = False,
        mission_profile: str = "balanced",
    ) -> Dict[str, Any]:
        preferred_provider = "local" if requires_offline or privacy_mode else "groq"
        fallback = ["local", "nvidia"] if preferred_provider == "groq" else ["groq", "nvidia"]
        return {
            "status": "success",
            "task": "reasoning",
            "mission_profile": mission_profile,
            "requires_offline": bool(requires_offline),
            "privacy_mode": bool(privacy_mode),
            "preferred_provider": preferred_provider,
            "preferred_model": "local-auto-reasoning-qwen3-14b" if preferred_provider == "local" else "groq-llm",
            "fallback_providers": fallback,
            "banned_providers": ["groq", "nvidia"] if requires_offline else [],
            "provider_affinity": {"local": 0.95, "groq": 0.3, "nvidia": 0.12},
            "voice_route_policy": {
                "mission_id": "voice-mission-1",
                "policy_profile": mission_profile,
                "risk_level": "medium",
                "ban_local_reasoning": not bool(requires_offline),
                "preferred_reasoning_provider": preferred_provider,
                "local_voice_pressure_score": 0.63 if not bool(requires_offline) else 0.18,
                "reason_code": "voice_route_policy_pressure",
            },
            "providers": [
                {"provider": preferred_provider, "score": 2.2, "blocked": False},
                {"provider": fallback[0], "score": 1.6, "blocked": False},
                {"provider": fallback[1], "score": 1.2, "blocked": False},
            ],
        }

    def probe_model_connectors(self, *, active_probe: bool = False, timeout_s: float = 4.0) -> Dict[str, Any]:
        return {
            "status": "success",
            "active_probe": bool(active_probe),
            "timeout_s": float(timeout_s),
            "count": 3,
            "probes": [
                {"provider": "groq", "ready": True, "active_probe": bool(active_probe), "active_probe_executed": False},
                {"provider": "nvidia", "ready": True, "active_probe": bool(active_probe), "active_probe_executed": False},
                {"provider": "local", "ready": True, "active_probe": bool(active_probe), "active_probe_executed": False},
            ],
            "route_plan": self.model_connector_route_plan(requires_offline=False, privacy_mode=False, mission_profile="balanced"),
        }

    def update_model_connector_policy(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        updates = payload if isinstance(payload, dict) else {}
        changed: Dict[str, float] = {}
        for key, value in updates.items():
            if key in self.model_connector_policy:
                try:
                    new_value = float(value)
                except Exception:
                    continue
                self.model_connector_policy[key] = new_value
                changed[key] = new_value
        return {
            "status": "success",
            "changed": changed,
            "count": len(changed),
            "diagnostics": self.model_connector_diagnostics(include_route_plan=False),
        }

    def list_policy_profiles(self) -> Dict[str, Any]:
        return {
            "items": [
                {
                    "name": "interactive",
                    "allow": [],
                    "deny": [],
                    "allow_high_risk": False,
                    "default_max_runtime_s": 240,
                    "default_max_steps": 28,
                },
                {
                    "name": "automation_safe",
                    "allow": ["time_now"],
                    "deny": ["terminate_process"],
                    "allow_high_risk": False,
                    "default_max_runtime_s": 120,
                    "default_max_steps": 12,
                },
            ],
            "count": 2,
            "default_profile": "interactive",
            "source_defaults": {"desktop-trigger": "automation_safe"},
        }

    @staticmethod
    def _desktop_governance_defaults(profile: str) -> Dict[str, Any]:
        clean = str(profile or "").strip().lower()
        if clean == "conservative":
            return {
                "allow_high_risk": False,
                "allow_critical_risk": False,
                "allow_admin_clearance": False,
                "allow_destructive": False,
                "allow_desktop_approval_reuse": False,
                "allow_action_confirmation_reuse": False,
                "desktop_approval_reuse_window_s": 0,
                "action_confirmation_reuse_window_s": 0,
            }
        if clean == "power":
            return {
                "allow_high_risk": True,
                "allow_critical_risk": True,
                "allow_admin_clearance": False,
                "allow_destructive": False,
                "allow_desktop_approval_reuse": True,
                "allow_action_confirmation_reuse": True,
                "desktop_approval_reuse_window_s": 240,
                "action_confirmation_reuse_window_s": 120,
            }
        return {
            "allow_high_risk": True,
            "allow_critical_risk": False,
            "allow_admin_clearance": False,
            "allow_destructive": False,
            "allow_desktop_approval_reuse": True,
            "allow_action_confirmation_reuse": True,
            "desktop_approval_reuse_window_s": 90,
            "action_confirmation_reuse_window_s": 45,
        }

    def desktop_governance_status(self) -> Dict[str, Any]:
        payload = dict(self.desktop_governance_state)
        payload["desktop_recovery_daemon"] = self.desktop_recovery_supervisor_status(history_limit=4)
        return payload

    def configure_desktop_governance(
        self,
        *,
        policy_profile: str | None = None,
        allow_high_risk: bool | None = None,
        allow_critical_risk: bool | None = None,
        allow_admin_clearance: bool | None = None,
        allow_destructive: bool | None = None,
        allow_desktop_approval_reuse: bool | None = None,
        allow_action_confirmation_reuse: bool | None = None,
        desktop_approval_reuse_window_s: int | None = None,
        action_confirmation_reuse_window_s: int | None = None,
        sync_desktop_recovery_daemon: bool = True,
    ) -> Dict[str, Any]:
        state = self.desktop_governance_state
        if policy_profile is not None:
            clean_profile = str(policy_profile or "").strip().lower() or "balanced"
            if clean_profile not in {"conservative", "balanced", "power", "custom"}:
                clean_profile = "balanced"
            state["policy_profile"] = clean_profile
            if clean_profile != "custom":
                state.update(self._desktop_governance_defaults(clean_profile))
        for key, value in {
            "allow_high_risk": allow_high_risk,
            "allow_critical_risk": allow_critical_risk,
            "allow_admin_clearance": allow_admin_clearance,
            "allow_destructive": allow_destructive,
            "allow_desktop_approval_reuse": allow_desktop_approval_reuse,
            "allow_action_confirmation_reuse": allow_action_confirmation_reuse,
        }.items():
            if value is not None:
                state[key] = bool(value)
                state["policy_profile"] = "custom"
        if desktop_approval_reuse_window_s is not None:
            state["desktop_approval_reuse_window_s"] = int(desktop_approval_reuse_window_s)
            state["policy_profile"] = "custom"
        if action_confirmation_reuse_window_s is not None:
            state["action_confirmation_reuse_window_s"] = int(action_confirmation_reuse_window_s)
            state["policy_profile"] = "custom"
        state["updated_at"] = "2026-03-15T10:35:00+00:00"
        state["source"] = "api"
        payload = self.desktop_governance_status()
        if sync_desktop_recovery_daemon:
            daemon_payload: Dict[str, Any] = {"policy_profile": state["policy_profile"], "history_limit": 4}
            if str(state.get("policy_profile", "")).strip().lower() == "custom":
                daemon_payload.update(
                    {
                        "allow_high_risk": bool(state.get("allow_high_risk", False)),
                        "allow_critical_risk": bool(state.get("allow_critical_risk", False)),
                        "allow_admin_clearance": bool(state.get("allow_admin_clearance", False)),
                        "allow_destructive": bool(state.get("allow_destructive", False)),
                    }
                )
            payload["desktop_recovery_daemon"] = self.configure_desktop_recovery_supervisor(**daemon_payload)
        return payload

    def list_recovery_profiles(self) -> Dict[str, Any]:
        items = []
        for item in self.recovery_profiles:
            row = dict(item)
            row["is_default"] = str(item.get("name", "")) == self.recovery_default_profile
            items.append(row)
        return {
            "status": "success",
            "default_profile": self.recovery_default_profile,
            "items": items,
            "count": len(items),
        }

    def set_default_recovery_profile(self, profile_name: str) -> Dict[str, Any]:
        clean = str(profile_name or "").strip().lower()
        valid = {str(item.get("name", "")).strip().lower() for item in self.recovery_profiles}
        if clean not in valid:
            return {"status": "error", "message": f"unknown profile '{profile_name}'"}
        self.recovery_default_profile = clean
        return {
            "status": "success",
            "message": "Recovery profile updated.",
            "default_profile": clean,
            "profiles": self.list_recovery_profiles(),
        }

    def external_reliability_status(self, *, provider: str = "", limit: int = 200) -> Dict[str, Any]:
        clean_provider = str(provider or "").strip().lower()
        rows = [
            {
                "provider": "google",
                "health_score": 0.41,
                "failure_ema": 0.62,
                "outage_active": True,
                "cooldown_active": True,
                "retry_after_s": 18.0,
            },
            {
                "provider": "graph",
                "health_score": 0.83,
                "failure_ema": 0.14,
                "outage_active": False,
                "cooldown_active": False,
                "retry_after_s": 0.0,
            },
        ]
        if clean_provider:
            rows = [row for row in rows if str(row.get("provider", "")).strip().lower() == clean_provider]
        return {
            "status": "success",
            "enabled": True,
            "total": len(rows),
            "count": min(len(rows), max(1, int(limit))),
            "items": rows[: max(1, int(limit))],
            "mission_outage_policy": {"mode": "worsening", "profile": "defensive", "bias": 0.22},
        }

    def external_reliability_mission_analysis(self, *, provider_limit: int = 260, history_limit: int = 40) -> Dict[str, Any]:
        return {
            "status": "success",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "provider_count": min(2, max(1, int(provider_limit))),
            "mission_policy": {"mode": "worsening", "profile": "defensive", "bias": 0.22},
            "profile_history_analysis": {
                "history_count": min(4, max(1, int(history_limit))),
                "history_limit": max(1, int(history_limit)),
                "profile_transitions": 2,
                "mode_transitions": 1,
                "avg_pressure_delta": 0.18,
                "transition_pressure": 0.45,
                "volatility_index": 0.53,
                "volatility_mode": "elevated",
            },
            "provider_risk_analysis": {
                "at_risk_count": 1,
                "at_risk_ratio": 0.5,
                "top_at_risk": [{"provider": "google", "health_score": 0.41, "failure_ema": 0.62, "outage_ema": 0.69}],
            },
            "trend": {"trend_pressure": 0.58, "mode": "worsening"},
            "recommendations": [{"priority": 1, "type": "autotune", "summary": "Prefer stability profile."}],
        }

    def external_reliability_mission_history(self, *, limit: int = 240, window: int = 36) -> Dict[str, Any]:
        bounded_limit = max(1, int(limit))
        bounded_window = max(4, int(window))
        return {
            "status": "success",
            "count": min(2, bounded_limit),
            "total": 2,
            "limit": bounded_limit,
            "window": bounded_window,
            "diagnostics": {
                "mode": "worsening",
                "drift_score": 0.63,
                "switch_pressure": 0.44,
            },
            "items": [
                {
                    "at": "2026-03-03T00:00:00+00:00",
                    "mission_mode": "worsening",
                    "mission_profile": "defensive",
                    "volatility_index": 0.57,
                    "at_risk_ratio": 0.5,
                    "trend_pressure": 0.58,
                },
                {
                    "at": "2026-03-03T00:03:00+00:00",
                    "mission_mode": "worsening",
                    "mission_profile": "defensive",
                    "volatility_index": 0.61,
                    "at_risk_ratio": 0.5,
                    "trend_pressure": 0.64,
                },
            ][:bounded_limit],
        }

    def external_reliability_mission_policy_status(
        self,
        *,
        provider_limit: int = 16,
        history_limit: int = 24,
        history_window: int = 36,
    ) -> Dict[str, Any]:
        bounded_provider_limit = max(1, int(provider_limit))
        bounded_history_limit = max(1, int(history_limit))
        bounded_history_window = max(4, int(history_window))
        analysis = self.external_reliability_mission_analysis(
            provider_limit=max(2, bounded_provider_limit),
            history_limit=max(2, bounded_history_limit),
        )
        history = self.external_reliability_mission_history(limit=bounded_history_limit, window=bounded_history_window)
        policy = dict(self.external_mission_policy_state)
        capability_bias = policy.get("capability_bias", {}) if isinstance(policy.get("capability_bias"), dict) else {}
        capability_rows = [
            {"capability": str(name), **dict(value)}
            for name, value in capability_bias.items()
            if isinstance(value, dict)
        ]
        return {
            "status": "success",
            "config": {
                **dict(self.external_mission_policy_config),
                "mission_outage_profile_history_limit": 60,
                "mission_analysis_history_limit": 240,
            },
            "policy": {
                **policy,
                "profile_history": list(policy.get("profile_history", []))[-bounded_history_limit:],
                "profile_history_count": len(policy.get("profile_history", [])),
                "capability_rows": capability_rows[:6],
            },
            "profile_history_analysis": {
                "history_count": len(list(policy.get("profile_history", []))[-bounded_history_limit:]),
                "switch_count": 1,
                "unique_profiles": ["cautious", "defensive"],
                "unique_modes": ["stable", "worsening"],
                "latest_profile": str(policy.get("profile", "balanced")),
                "latest_mode": str(policy.get("mode", "stable")),
            },
            "history": history,
            "provider_count": len(self.external_provider_bias_rows),
            "provider_biases": list(self.external_provider_bias_rows)[:bounded_provider_limit],
            "provider_policy_autotune": {
                "enabled": True,
                "interval_s": 120.0,
                "dry_run": False,
                "cooldown_remaining_s": 0.0,
                "last_run_age_s": 122.0,
            },
            "analysis": {
                "generated_at": str(analysis.get("generated_at", "")),
                "mission_policy": dict(analysis.get("mission_policy", {})),
                "profile_history_analysis": dict(analysis.get("profile_history_analysis", {})),
                "provider_risk_analysis": dict(analysis.get("provider_risk_analysis", {})),
                "trend": dict(analysis.get("trend", {})),
                "mission_history_drift": {"mode": "worsening", "drift_score": 0.63, "switch_pressure": 0.44},
                "provider_policy_tuning": {"status": "success", "changed": False, "updated_count": 0},
                "recommendations": list(analysis.get("recommendations", [])),
            },
        }

    def external_reliability_mission_policy_tune(
        self,
        *,
        dry_run: bool = False,
        reason: str = "manual",
        record_analysis: bool = True,
        tune_provider_policies: bool = True,
        provider_limit: int = 260,
        history_limit: int = 40,
    ) -> Dict[str, Any]:
        policy = dict(self.external_mission_policy_state)
        history = list(policy.get("profile_history", [])) if isinstance(policy.get("profile_history"), list) else []
        now_iso = datetime.now(timezone.utc).isoformat()
        next_profile = "defensive"
        if not dry_run:
            policy["mode"] = "worsening"
            policy["profile"] = next_profile
            policy["bias"] = 0.29
            policy["profile_confidence"] = 0.81
            policy["updated_at"] = now_iso
            policy["last_reason"] = str(reason or "manual")
            history.append(
                {
                    "at": now_iso,
                    "profile": next_profile,
                    "mode": "worsening",
                    "volatility_index": 0.66,
                    "target_pressure": 0.71,
                    "recommendation": "stability",
                    "reason": "mission_worsening_pressure",
                }
            )
            policy["profile_history"] = history[-60:]
            self.external_mission_policy_state = policy
            for row in self.external_provider_bias_rows:
                if str(row.get("provider", "")).strip().lower() == "google":
                    row["outage_policy_bias"] = 0.36
                    row["cooldown_bias"] = 1.51
                    row["mission_pressure"] = 0.78
                    row["updated_at"] = now_iso
        status_snapshot = self.external_reliability_mission_policy_status(
            provider_limit=min(12, max(1, int(provider_limit // 20) or 12)),
            history_limit=min(12, max(1, int(history_limit))),
            history_window=24,
        )
        return {
            "status": "success",
            "dry_run": bool(dry_run),
            "reason": str(reason or "manual"),
            "record_analysis": bool(record_analysis),
            "tune_provider_policies": bool(tune_provider_policies),
            "tune": {
                "status": "success",
                "changed": not dry_run,
                "dry_run": bool(dry_run),
                "enabled": True,
                "reason": f"kernel:{reason or 'manual'}",
                "mode": "worsening",
                "profile": next_profile,
                "profile_changed": True,
                "profile_reason": "mission_worsening_pressure",
                "targets": {
                    "target_pressure": 0.71,
                    "target_bias": 0.29,
                    "base_target_bias": 0.22,
                    "adaptive_bias_gain": 0.61,
                    "adaptive_bias_decay": 0.73,
                    "adaptive_quality_relief": 0.16,
                    "volatility_index": 0.66,
                    "volatility_mode": "elevated",
                    "recommendation": "stability",
                },
                "capability_targets": {
                    "email": {"target_pressure": 0.74, "target_bias": 0.32, "weight": 0.82, "top_action": "external_email_send"}
                },
                "state": dict(self.external_mission_policy_state if not dry_run else policy),
            },
            "mission_summary": {"trend": {"mode": "worsening", "pressure": 0.62}, "recommendation": "stability"},
            "report_summary": {
                "scores": {"reliability": 56.0},
                "pressures": {"failure_pressure": 0.58, "open_breaker_pressure": 0.36},
                "risk": {"avg_score": 0.61},
                "quality": {"avg_score": 0.44},
                "recovery": {"status": "active"},
                "action_hotspots": [{"action": "external_email_send", "pressure": 0.74}],
            },
            "analysis": self.external_reliability_mission_analysis(
                provider_limit=max(2, int(provider_limit)),
                history_limit=max(2, int(history_limit)),
            ),
            "history_record": {
                "status": "success" if record_analysis and not dry_run else "skip",
                "recorded": bool(record_analysis and not dry_run),
                "delta_score": 0.18,
                "elapsed_s": 52.0,
            },
            "provider_policy_tuning": {
                "status": "success" if tune_provider_policies else "skip",
                "changed": bool(tune_provider_policies and not dry_run),
                "updated_count": 1 if tune_provider_policies and not dry_run else 0,
                "mission_mode": "worsening",
                "mission_profile": next_profile,
                "dry_run": bool(dry_run),
            },
            "changed": not dry_run,
            "status_snapshot": status_snapshot,
        }

    def external_reliability_mission_policy_configure(
        self,
        *,
        config: Dict[str, Any] | None = None,
        persist_now: bool = True,
        provider_limit: int = 16,
        history_limit: int = 24,
        history_window: int = 36,
    ) -> Dict[str, Any]:
        payload = dict(config) if isinstance(config, dict) else {}
        reset_requested = bool(payload.get("reset_config", False))
        changed: Dict[str, Any] = {}
        resolved_actions: list[Dict[str, Any]] = []
        if reset_requested:
            self.external_mission_policy_config = {
                "mission_outage_autotune_enabled": True,
                "mission_outage_profile_autotune_enabled": True,
                "mission_provider_policy_autotune_enabled": True,
                "mission_outage_bias_gain": 0.48,
                "mission_outage_bias_decay": 0.8,
                "mission_outage_quality_relief": 0.18,
                "mission_outage_profile_decay": 0.78,
                "mission_outage_profile_stability_decay": 0.82,
                "mission_outage_profile_hysteresis": 0.09,
                "mission_outage_capability_bias_gain": 0.24,
                "mission_outage_capability_bias_decay": 0.84,
                "mission_outage_capability_limit": 12,
                "provider_policy_max_providers": 80,
                "outage_trip_threshold": 0.62,
                "outage_recover_threshold": 0.36,
                "outage_route_hard_block_threshold": 0.86,
                "outage_preflight_block_threshold": 0.92,
            }
            changed = dict(self.external_mission_policy_config)
        else:
            preset_id = str(payload.get("preset_id", "") or "").strip().lower()
            if bool(payload.get("apply_recommended_preset", False)) and not preset_id:
                preset_id = "balanced_adaptive"
            if preset_id == "balanced_adaptive":
                preset_changes = {
                    "mission_outage_bias_gain": 0.58,
                    "mission_outage_bias_decay": 0.84,
                    "mission_outage_profile_decay": 0.82,
                    "mission_outage_profile_stability_decay": 0.86,
                    "provider_policy_max_providers": max(
                        int(self.external_mission_policy_config.get("provider_policy_max_providers", 80) or 80),
                        12,
                    ),
                }
                self.external_mission_policy_config.update(preset_changes)
                changed.update(preset_changes)
                resolved_actions.append({"kind": "preset", "id": "balanced_adaptive"})
            remediation_action = str(payload.get("remediation_action", "") or "").strip().lower()
            if remediation_action == "widen_trip_recover_gap":
                trip_threshold = float(self.external_mission_policy_config.get("outage_trip_threshold", 0.62) or 0.62)
                recover_threshold = round(max(0.05, trip_threshold - 0.08), 6)
                self.external_mission_policy_config["outage_recover_threshold"] = recover_threshold
                changed["outage_recover_threshold"] = recover_threshold
                resolved_actions.append({"kind": "remediation", "id": remediation_action})
            for key, value in payload.items():
                if key in {"reset_config", "preset_id", "apply_recommended_preset", "remediation_action"}:
                    continue
                if self.external_mission_policy_config.get(key) != value:
                    self.external_mission_policy_config[key] = value
                    changed[key] = value
        status_snapshot = self.external_reliability_mission_policy_status(
            provider_limit=provider_limit,
            history_limit=history_limit,
            history_window=history_window,
        )
        return {
            "status": "success",
            "updated": bool(changed) or reset_requested,
            "reset": bool(reset_requested),
            "changed": changed,
            "config": dict(self.external_mission_policy_config),
            "persist_now": bool(persist_now),
            "validation": {
                "summary": {
                    "normalized_count": 0,
                    "warning_count": 0,
                    "changed_count": len(changed),
                    "reset_requested": bool(reset_requested),
                },
                "normalized": [],
                "warnings": [],
                "remediation_hints": [
                    {
                        "action": "widen_trip_recover_gap",
                        "button_label": "Widen Recovery Gap",
                        "message": "Lower recovery threshold to preserve trip/recover gap.",
                        "config_patch": {
                            "outage_recover_threshold": round(
                                max(
                                    0.05,
                                    float(self.external_mission_policy_config.get("outage_trip_threshold", 0.62) or 0.62) - 0.08,
                                ),
                                6,
                            )
                        },
                    }
                ],
                "presets": [
                    {
                        "id": "balanced_adaptive",
                        "label": "Balanced Adaptive",
                        "recommended": True,
                        "priority": 1,
                        "confidence": 0.78,
                        "reason": "Normalize mission feedback tuning while preserving adaptive routing.",
                        "changes": {
                            "mission_outage_bias_gain": float(
                                self.external_mission_policy_config.get("mission_outage_bias_gain", 0.48) or 0.48
                            ),
                            "mission_outage_bias_decay": float(
                                self.external_mission_policy_config.get("mission_outage_bias_decay", 0.8) or 0.8
                            ),
                        },
                    }
                ],
                "recommended_preset_id": "balanced_adaptive",
                "history_context": {
                    "history_count": 2,
                    "recent_count": 2,
                    "latest_mode": "worsening",
                    "latest_profile": "defensive",
                    "recent_profiles": ["defensive", "defensive"],
                    "recent_modes": ["worsening", "worsening"],
                    "recent_pressure_series": [0.58, 0.64],
                    "recent_volatility_series": [0.57, 0.61],
                    "recent_at_risk_series": [0.5, 0.5],
                    "diagnostics": {
                        "mode": "worsening",
                        "drift_score": 0.63,
                        "switch_pressure": 0.44,
                    },
                    "top_provider_biases": [
                        {"provider": "google", "outage_policy_bias": 0.21, "cooldown_bias": 1.18, "mission_pressure": 0.74}
                    ],
                    "top_capability_biases": [
                        {"capability": "document", "bias": 0.18, "pressure_ema": 0.66, "samples": 6}
                    ],
                },
                "decision_trace": {
                    "recommended_preset_id": "balanced_adaptive",
                    "trigger_codes": ["history_worsening"],
                    "trigger_band": "elevated",
                    "severity_score": 0.58,
                    "summary": "Recommended preset 'balanced_adaptive' because drift is 'worsening'.",
                    "evidence": [
                        {"label": "drift_mode", "value": "worsening"},
                        {"label": "drift_score", "value": 0.63},
                    ],
                },
                "resolved_actions": resolved_actions,
                "changed_fields": sorted(str(key) for key in changed.keys()),
                "drift_fields": sorted(str(key) for key in changed.keys()),
                "metrics": {
                    "trip_recover_gap": round(
                        float(self.external_mission_policy_config.get("outage_trip_threshold", 0.62) or 0.62)
                        - float(self.external_mission_policy_config.get("outage_recover_threshold", 0.36) or 0.36),
                        6,
                    ),
                    "route_preflight_gap": round(
                        float(self.external_mission_policy_config.get("outage_preflight_block_threshold", 0.92) or 0.92)
                        - float(self.external_mission_policy_config.get("outage_route_hard_block_threshold", 0.86) or 0.86),
                        6,
                    ),
                    "bias_gain": float(self.external_mission_policy_config.get("mission_outage_bias_gain", 0.48) or 0.48),
                    "bias_decay": float(self.external_mission_policy_config.get("mission_outage_bias_decay", 0.8) or 0.8),
                    "provider_policy_max_providers": int(
                        self.external_mission_policy_config.get("provider_policy_max_providers", 80) or 80
                    ),
                },
            },
            "status_snapshot": status_snapshot,
        }

    def reset_external_reliability_mission_policy(
        self,
        *,
        reset_history: bool = False,
        reset_provider_biases: bool = False,
    ) -> Dict[str, Any]:
        previous_profile = str(self.external_mission_policy_state.get("profile", "balanced"))
        previous_mode = str(self.external_mission_policy_state.get("mode", "stable"))
        self.external_mission_policy_state = {
            "bias": 0.0,
            "pressure_ema": 0.0,
            "risk_ema": 0.0,
            "quality_ema": 0.0,
            "failed_ratio_ema": 0.0,
            "blocked_ratio_ema": 0.0,
            "mode": "stable",
            "profile": "balanced",
            "profile_confidence": 0.0,
            "profile_pressure_ema": 0.0,
            "profile_stability_ema": 0.0,
            "profile_switch_count": 0,
            "profile_last_switch_at": "",
            "profile_last_reason": "",
            "profile_history": [] if reset_history else list(self.external_mission_policy_state.get("profile_history", [])),
            "capability_bias": {},
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "last_reason": "reset",
        }
        provider_biases_reset = 0
        operation_bias_entries_reset = 0
        if reset_provider_biases:
            for row in self.external_provider_bias_rows:
                provider_biases_reset += 1
                operation_bias_entries_reset += len(row.get("top_operation_bias", [])) if isinstance(row.get("top_operation_bias"), list) else 0
                row["outage_policy_bias"] = 0.0
                row["cooldown_bias"] = 1.0
                row["mission_pressure"] = 0.0
                row["top_operation_bias"] = []
        payload = {
            "status": "success",
            "reset_history": bool(reset_history),
            "reset_provider_biases": bool(reset_provider_biases),
            "cleared_history": 2 if reset_history else 0,
            "provider_biases_reset": provider_biases_reset,
            "operation_bias_entries_reset": operation_bias_entries_reset,
            "previous_profile": previous_profile,
            "previous_mode": previous_mode,
            "policy": {
                "mode": str(self.external_mission_policy_state.get("mode", "stable")),
                "profile": str(self.external_mission_policy_state.get("profile", "balanced")),
                "bias": float(self.external_mission_policy_state.get("bias", 0.0) or 0.0),
                "updated_at": str(self.external_mission_policy_state.get("updated_at", "")),
                "last_reason": str(self.external_mission_policy_state.get("last_reason", "")),
            },
        }
        payload["status_snapshot"] = self.external_reliability_mission_policy_status(provider_limit=12, history_limit=12, history_window=24)
        return payload

    def reset_external_reliability(self, *, provider: str = "") -> Dict[str, Any]:
        return {"status": "success", "provider": str(provider or "").strip().lower(), "removed": 1}

    def list_oauth_providers(self) -> Dict[str, Any]:
        return {
            "status": "success",
            "items": [
                {
                    "provider": "google",
                    "display_name": "Google",
                    "configured": True,
                    "client_id_env": "GOOGLE_OAUTH_CLIENT_ID",
                    "client_secret_env": "GOOGLE_OAUTH_CLIENT_SECRET",
                    "authorize_url": "https://accounts.google.com/o/oauth2/v2/auth",
                    "token_url": "https://oauth2.googleapis.com/token",
                    "default_scopes": ["openid", "email"],
                },
                {
                    "provider": "graph",
                    "display_name": "Microsoft Graph",
                    "configured": True,
                    "client_id_env": "MICROSOFT_GRAPH_CLIENT_ID",
                    "client_secret_env": "MICROSOFT_GRAPH_CLIENT_SECRET",
                    "authorize_url": "https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
                    "token_url": "https://login.microsoftonline.com/common/oauth2/v2.0/token",
                    "default_scopes": ["offline_access", "User.Read"],
                },
            ],
            "count": 2,
        }

    def start_oauth_authorization(
        self,
        *,
        provider: str,
        account_id: str = "default",
        scopes: list[str] | None = None,
        redirect_uri: str = "",
    ) -> Dict[str, Any]:
        if not provider:
            return {"status": "error", "message": "provider is required"}
        self._oauth_flow_counter += 1
        flow_id = f"oauth-flow-{self._oauth_flow_counter}"
        flow = {
            "session_id": flow_id,
            "state": f"state-{self._oauth_flow_counter}",
            "provider": provider,
            "account_id": account_id,
            "redirect_uri": redirect_uri or "http://127.0.0.1:8765/oauth/callback",
            "scopes": scopes or ["openid", "email"],
            "status": "pending",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "expires_at": datetime.now(timezone.utc).isoformat(),
            "auth_url": f"https://example.com/oauth/{provider}?session_id={flow_id}",
            "error": "",
            "error_description": "",
        }
        self.oauth_flows[flow_id] = flow
        return {"status": "success", "flow": flow, "authorize_url": flow["auth_url"]}

    def oauth_authorization_status(self, *, session_id: str = "", state: str = "") -> Dict[str, Any]:
        if session_id:
            flow = self.oauth_flows.get(session_id)
            if not flow:
                return {"status": "error", "message": "OAuth flow not found"}
            return {"status": "success", "flow": flow}
        if state:
            for flow in self.oauth_flows.values():
                if str(flow.get("state", "")) == state:
                    return {"status": "success", "flow": flow}
        return {"status": "error", "message": "OAuth flow not found"}

    def complete_oauth_authorization(
        self,
        *,
        session_id: str = "",
        state: str = "",
        code: str = "",
        redirect_uri: str = "",
        error: str = "",
        error_description: str = "",
    ) -> Dict[str, Any]:
        del redirect_uri
        flow: Dict[str, Any] | None = None
        if session_id:
            flow = self.oauth_flows.get(session_id)
        elif state:
            for item in self.oauth_flows.values():
                if str(item.get("state", "")) == state:
                    flow = item
                    break
        if not flow:
            return {"status": "error", "message": "OAuth flow not found"}
        if error:
            flow["status"] = "error"
            flow["error"] = error
            flow["error_description"] = error_description
            return {"status": "error", "message": error_description or error, "flow": flow}
        if not code:
            return {"status": "error", "message": "code is required", "flow": flow}

        provider = str(flow.get("provider", "")).strip().lower()
        account_id = str(flow.get("account_id", "default"))
        token = {
            "provider": provider,
            "account_id": account_id,
            "token_type": "Bearer",
            "scopes": flow.get("scopes", []),
            "access_token_suffix": "token-x",
            "refresh_token_suffix": "refresh-x",
            "has_access_token": True,
            "has_refresh_token": True,
        }
        self.oauth_tokens[f"{provider}:{account_id}"] = {
            "provider": provider,
            "account_id": account_id,
            "access_token": "token-x",
            "refresh_token": "refresh-x",
            "token_type": "Bearer",
            "scopes": flow.get("scopes", []),
            "expires_in_s": 3600,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "metadata": {"oauth_flow_session_id": flow.get("session_id", "")},
        }
        flow["status"] = "completed"
        flow["completed_at"] = datetime.now(timezone.utc).isoformat()
        flow["token"] = token
        return {"status": "success", "flow": flow, "token": token}

    def list_oauth_tokens(
        self,
        *,
        provider: str = "",
        account_id: str = "",
        include_secrets: bool = False,
        limit: int = 200,
    ) -> Dict[str, Any]:
        rows = [dict(item) for item in self.oauth_tokens.values()]
        if provider:
            rows = [item for item in rows if str(item.get("provider", "")).lower() == provider.lower()]
        if account_id:
            rows = [item for item in rows if str(item.get("account_id", "")).lower() == account_id.lower()]
        sliced = rows[: max(1, min(int(limit), 2000))]
        if not include_secrets:
            for row in sliced:
                row.pop("access_token", None)
                row.pop("refresh_token", None)
        return {"status": "success", "items": sliced, "count": len(sliced), "total": len(rows)}

    def upsert_oauth_token(
        self,
        *,
        provider: str,
        account_id: str = "default",
        access_token: str,
        refresh_token: str = "",
        token_type: str = "Bearer",
        scopes: list[str] | None = None,
        expires_at: str = "",
        expires_in_s: int | None = None,
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        key = f"{provider}:{account_id}"
        now_iso = datetime.now(timezone.utc).isoformat()
        token = {
            "provider": provider,
            "account_id": account_id,
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": token_type,
            "scopes": scopes or [],
            "expires_at": expires_at,
            "expires_in_s": expires_in_s or 3600,
            "created_at": now_iso,
            "updated_at": now_iso,
            "metadata": metadata or {},
        }
        self.oauth_tokens[key] = token
        return {"status": "success", "token": token}

    def refresh_oauth_token(self, *, provider: str, account_id: str = "default") -> Dict[str, Any]:
        key = f"{provider}:{account_id}"
        row = self.oauth_tokens.get(key)
        if not row:
            return {"status": "error", "message": "token not found"}
        row["access_token"] = f"refreshed-{provider}-{account_id}"
        row["updated_at"] = datetime.now(timezone.utc).isoformat()
        return {"status": "success", "token": row}

    def revoke_oauth_token(self, *, provider: str, account_id: str = "default") -> Dict[str, Any]:
        key = f"{provider}:{account_id}"
        row = self.oauth_tokens.pop(key, None)
        if not row:
            return {"status": "error", "message": "token not found"}
        return {"status": "success", "provider": provider, "account_id": account_id}

    def maintain_oauth_tokens(
        self,
        *,
        refresh_window_s: int | None = None,
        provider: str = "",
        account_id: str = "",
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        rows = list(self.oauth_tokens.values())
        if provider:
            rows = [item for item in rows if str(item.get("provider", "")).strip().lower() == provider.lower()]
        if account_id:
            rows = [item for item in rows if str(item.get("account_id", "")).strip().lower() == account_id.lower()]
        candidates = len(rows)
        refreshed = 0 if dry_run else candidates
        payload = {
            "status": "success",
            "dry_run": bool(dry_run),
            "refresh_window_s": int(refresh_window_s or 300),
            "provider_filter": provider,
            "account_filter": account_id,
            "candidate_count": candidates,
            "refreshed_count": refreshed,
            "skipped_count": 0,
            "errors": [],
            "error_count": 0,
            "candidates": [
                {
                    "provider": str(item.get("provider", "")),
                    "account_id": str(item.get("account_id", "")),
                    "expires_in_s": int(item.get("expires_in_s", 3600)),
                    "expires_at": str(item.get("expires_at", "")),
                    "has_refresh_token": bool(item.get("refresh_token", "")),
                }
                for item in rows
            ],
            "refreshed": [
                {"provider": str(item.get("provider", "")), "account_id": str(item.get("account_id", ""))}
                for item in rows
            ]
            if not dry_run
            else [],
        }
        self.oauth_maintenance = {
            "status": payload["status"],
            "last_run_at": datetime.now(timezone.utc).isoformat(),
            "candidate_count": payload["candidate_count"],
            "refreshed_count": payload["refreshed_count"],
            "error_count": payload["error_count"],
            "provider_filter": provider,
            "account_filter": account_id,
            "dry_run": bool(dry_run),
        }
        return payload

    def oauth_maintenance_status(self) -> Dict[str, Any]:
        return dict(self.oauth_maintenance)

    def list_circuit_breakers(self, *, limit: int = 200) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 2000))
        items = [
            {
                "action": "browser_read_dom",
                "consecutive_failures": 0,
                "opened_count": 2,
                "open_until": "2026-02-24T10:10:00+00:00",
                "last_failure_category": "timeout",
                "last_error": "request timed out",
                "last_updated_at": "2026-02-24T10:09:00+00:00",
            }
        ]
        return {"status": "success", "items": items[:bounded], "count": min(len(items), bounded), "total": len(items)}

    def create_browser_session(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        self._browser_session_counter += 1
        session_id = f"session-{self._browser_session_counter}"
        now_iso = datetime.now(timezone.utc).isoformat()
        row = {
            "session_id": session_id,
            "name": str((payload or {}).get("name", "browser-session")),
            "base_url": str((payload or {}).get("base_url", "")),
            "created_at": now_iso,
            "updated_at": now_iso,
            "request_count": 0,
        }
        self.browser_sessions[session_id] = row
        return {"status": "success", "session": row}

    def list_browser_sessions(self) -> Dict[str, Any]:
        rows = list(self.browser_sessions.values())
        return {"status": "success", "items": rows, "count": len(rows)}

    def close_browser_session(self, session_id: str) -> Dict[str, Any]:
        row = self.browser_sessions.pop(session_id, None)
        if not row:
            return {"status": "error", "message": "session not found"}
        return {"status": "success", "session": row}

    def browser_session_request(self, session_id: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        row = self.browser_sessions.get(session_id)
        if not row:
            return {"status": "error", "message": "session not found"}
        row["request_count"] = int(row.get("request_count", 0)) + 1
        row["updated_at"] = datetime.now(timezone.utc).isoformat()
        url = str((payload or {}).get("url", row.get("base_url", "")))
        method = str((payload or {}).get("method", "GET")).upper()
        return {
            "status": "success",
            "session_id": session_id,
            "request": {"method": method, "url": url},
            "response": {"status_code": 200, "ok": True, "content_type": "application/json", "body": "{\"ok\":true}"},
        }

    def browser_session_read_dom(self, session_id: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        row = self.browser_sessions.get(session_id)
        if not row:
            return {"status": "error", "message": "session not found"}
        url = str((payload or {}).get("url", row.get("base_url", "")))
        return {"status": "success", "session_id": session_id, "url": url, "title": "Fake Session Page", "text": "hello", "chars": 5}

    def browser_session_extract_links(self, session_id: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        row = self.browser_sessions.get(session_id)
        if not row:
            return {"status": "error", "message": "session not found"}
        url = str((payload or {}).get("url", row.get("base_url", "")))
        return {"status": "success", "session_id": session_id, "url": url, "links": [url + "/a"], "count": 1, "truncated": False}

    def list_external_tasks(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        args = dict(payload or {})
        provider = str(args.get("provider", "auto") or "auto")
        query_text = str(args.get("query", "") or "")
        max_results = max(1, min(int(args.get("max_results", 25) or 25), 200))
        include_completed = bool(args.get("include_completed", True))
        status_text = str(args.get("status", "not_started") or "not_started")
        items = [
            {
                "task_id": "task-1",
                "title": "Review roadmap draft",
                "status": status_text,
                "provider": provider,
                "due": "2026-03-05T17:00:00Z",
            }
        ]
        if query_text:
            lowered = query_text.lower()
            items = [item for item in items if lowered in str(item.get("title", "")).lower()]
        if not include_completed:
            items = [item for item in items if str(item.get("status", "")).lower() not in {"completed", "done"}]
        return {
            "status": "success",
            "provider": provider,
            "query": query_text,
            "items": items[:max_results],
            "count": len(items[:max_results]),
            "include_completed": include_completed,
        }

    def run_external_connector_preflight(
        self,
        args: Dict[str, Any] | None = None,
        *,
        source: str = "desktop-ui",
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        payload = dict(args or {})
        action_name = str(payload.get("action", "")).strip()
        if not action_name:
            return {"status": "error", "message": "action is required"}
        provider = str(payload.get("provider", "auto") or "auto").strip().lower() or "auto"
        action_args = payload.get("args") if isinstance(payload.get("args"), dict) else {}
        missing_fields = []
        if action_name == "external_email_send":
            to_value = action_args.get("to")
            recipients = to_value if isinstance(to_value, list) else []
            if not recipients:
                missing_fields.append("to")
            subject = str(action_args.get("subject", "")).strip()
            if not subject:
                missing_fields.append("subject")
        status_value = "success" if not missing_fields else "error"
        severity = "info" if not missing_fields else "high"
        code = "ready" if not missing_fields else "missing_fields"
        message = "Connector preflight passed." if not missing_fields else "Connector preflight failed contract checks."

        remediation_hints: list[Dict[str, Any]] = []
        if missing_fields:
            patch: Dict[str, Any] = {}
            if "to" in missing_fields:
                patch["to"] = ["alice@example.com"]
            if "subject" in missing_fields:
                patch["subject"] = "JARVIS Follow-up"
            remediation_hints.append(
                {
                    "code": "populate_required_fields",
                    "severity": "high",
                    "message": "Populate required fields before running the action.",
                    "args_patch": patch,
                    "tool_action": {"action": "external_connector_status", "args": {"provider": provider}},
                }
            )

        checks = [
            {
                "name": "provider_available",
                "status": "ok",
                "required": True,
                "available": True,
                "message": f"Provider {provider} is available.",
            }
        ]
        if missing_fields:
            checks.append(
                {
                    "name": "required_fields",
                    "status": "error",
                    "required": True,
                    "code": "missing_fields",
                    "message": f"Missing required fields: {', '.join(missing_fields)}",
                    "context": {"fields": missing_fields},
                }
            )

        return {
            "status": status_value,
            "action": action_name,
            "provider": provider,
            "provider_selected": provider,
            "provider_recommended": provider,
            "message": message,
            "source": source,
            "metadata": dict(metadata or {}),
            "checks": checks,
            "provider_routing": [{"provider": provider, "healthy": True, "priority": 1}],
            "contract_diagnostic": {
                "code": code,
                "severity": severity,
                "message": message,
                "requested_provider": provider,
                "allowed_providers": ["google", "graph", "smtp"],
                "fields": missing_fields,
                "checks": checks,
                "remediation_hints": remediation_hints,
                "remediation_plan": [
                    {
                        "phase": "diagnose",
                        "status": "pending" if missing_fields else "complete",
                        "summary": "Run connector diagnostics and verify credentials.",
                    },
                    {
                        "phase": "repair",
                        "status": "recommended" if missing_fields else "complete",
                        "summary": "Apply recommended argument patch and retry.",
                        "args_patch": remediation_hints[0]["args_patch"] if remediation_hints else {},
                    },
                ],
            },
            "remediation_hints": remediation_hints,
            "remediation_plan": [
                {
                    "phase": "repair",
                    "status": "recommended" if missing_fields else "complete",
                    "summary": "Apply recommended argument patch and retry.",
                    "args_patch": remediation_hints[0]["args_patch"] if remediation_hints else {},
                    "tool_action": remediation_hints[0]["tool_action"] if remediation_hints else {},
                }
            ],
            "remediation_contract": {
                "version": "1.0",
                "required": ["action"],
                "patchable_fields": missing_fields,
            },
            "execution_candidates": [
                {
                    "candidate_id": f"candidate_{provider}",
                    "label": f"Execute via {provider}",
                    "summary": f"Execution handoff synthesized for {provider}.",
                    "provider": provider,
                    "risk_band": "medium" if missing_fields else "low",
                    "risk_score": 0.58 if missing_fields else 0.18,
                    "requires_compare": bool(missing_fields),
                    "requires_confirmation": False,
                    "policy_profile": "automation_safe" if missing_fields else "automation_power",
                    "args_patch": remediation_hints[0]["args_patch"] if remediation_hints else {},
                    "metadata_patch": {
                        "__external_retry_contract_mode": "adaptive_backoff" if missing_fields else "immediate",
                        "__external_retry_contract_risk": 0.58 if missing_fields else 0.18,
                        "policy_profile": "automation_safe" if missing_fields else "automation_power",
                    },
                    "tool_action": {
                        "action": action_name,
                        "args": {
                            **action_args,
                            **(remediation_hints[0]["args_patch"] if remediation_hints else {}),
                        },
                        "metadata": {
                            "__external_retry_contract_mode": "adaptive_backoff" if missing_fields else "immediate",
                            "__external_retry_contract_risk": 0.58 if missing_fields else 0.18,
                            "policy_profile": "automation_safe" if missing_fields else "automation_power",
                        },
                    },
                    "approval_preview": {
                        "status": "success",
                        "posture": "compare_required" if missing_fields else "approval_required",
                        "summary": (
                            "Candidate should be compared before execution."
                            if missing_fields
                            else "Candidate requires approval before execution."
                        ),
                        "effective_risk_score": 0.62 if missing_fields else 0.44,
                        "requires_compare": bool(missing_fields),
                        "requires_approval_ticket": True,
                        "ready_to_stage": True,
                        "ready_to_execute": False,
                        "recommended_steps": [
                            "Run advisor simulation compare before execution.",
                            "Request and consume an approval ticket before execution.",
                        ],
                    },
                }
            ],
            "approval_summary": {
                "candidate_count": 1,
                "approval_required_count": 1,
                "compare_required_count": 1 if missing_fields else 0,
                "ready_stage_count": 1,
                "ready_execute_count": 0,
                "posture": "compare_then_approve" if missing_fields else "approval_required",
                "approval_required_ratio": 1.0,
                "compare_required_ratio": 1.0 if missing_fields else 0.0,
                "ready_execute_ratio": 0.0,
                "summary": (
                    "Execution candidates require compare validation and approval before execution."
                    if missing_fields
                    else "Execution candidates require approval before execution."
                ),
            },
            "advisor_context": {
                "source": "preflight_execution_handoff",
                "action": action_name,
                "requested_provider": provider,
                "recommended_provider": provider,
                "mission_profile": "balanced",
                "runtime_lane": "parallel",
                "retry_mode": "adaptive_backoff" if missing_fields else "immediate",
                "candidate_count": 1,
                "approval_summary": {
                    "candidate_count": 1,
                    "approval_required_count": 1,
                    "compare_required_count": 1 if missing_fields else 0,
                    "ready_execute_count": 0,
                },
                "candidate_approval_map": {
                    f"candidate_{provider}": {
                        "posture": "compare_required" if missing_fields else "approval_required",
                        "requires_compare": bool(missing_fields),
                        "requires_approval_ticket": True,
                        "ready_to_execute": False,
                    }
                },
            },
            "advisor_simulation_template": {
                "template_id": "advisor_execution_handoff",
                "name": "Advisor Execution Handoff",
                "description": "Replay advisor-selected execution candidates.",
                "payload": {
                    "action": action_name,
                    "provider": provider,
                    "providers": [provider, "graph", "smtp"],
                    "args": action_args,
                    "scenarios": [
                        {"id": "baseline"},
                        {
                            "id": f"candidate_{provider}",
                            "provider": provider,
                            "candidate_id": f"candidate_{provider}",
                            "execution_candidate": True,
                            "metadata_patch": {
                                "__external_retry_contract_mode": "adaptive_backoff" if missing_fields else "immediate",
                                "__external_retry_contract_risk": 0.58 if missing_fields else 0.18,
                            },
                        },
                    ],
                    "advisor_context": {
                        "source": "preflight_execution_handoff",
                        "candidate_count": 1,
                    },
                },
            },
            "orchestration_diagnostics": {
                "status": "success",
                "provider_requested": provider,
                "provider_count": 1,
                "provider_health": [
                    {
                        "provider": provider,
                        "health_score": 0.84 if not missing_fields else 0.48,
                        "failure_ema": 0.12 if not missing_fields else 0.52,
                        "cooldown_active": False,
                        "outage_active": False,
                        "retry_after_s": 0.0,
                        "health_state": "healthy" if not missing_fields else "degraded",
                    }
                ],
                "mission_outage_policy": {"mode": "stable", "bias": 0.0, "pressure_ema": 0.12, "profile": "balanced"},
                "reliability_pressure": 0.14 if not missing_fields else 0.58,
                "blocked_by_reliability": False,
                "recommended_provider": provider,
                "route_weight_advisor": {
                    "summary": f"Route weighting favors '{provider}' for preflight retries.",
                    "trigger_band": "watch" if missing_fields else "stable",
                    "requested_provider": provider,
                    "recommended_provider": provider,
                    "runtime_lane": "parallel",
                    "retry_mode": "probe_then_backoff" if missing_fields else "immediate",
                    "weight_rows": [
                        {
                            "provider": provider,
                            "tier": "primary",
                            "priority": 1,
                            "eligible": True,
                            "score": 0.82 if not missing_fields else 0.54,
                            "pressure": 0.18 if not missing_fields else 0.58,
                            "route_weight": 1.0,
                            "recommended_contract_mode": "probe_then_backoff" if missing_fields else "immediate",
                        }
                    ],
                    "recommended_actions": [
                        {
                            "id": f"reroute_preflight_{provider}",
                            "label": f"Rerun via {provider}",
                            "summary": f"Re-run preflight on {provider} with reliability-aware retry contract.",
                            "preflight_action": action_name,
                            "provider": provider,
                            "metadata_patch": {
                                "__external_retry_contract_mode": "probe_then_backoff" if missing_fields else "immediate",
                                "__external_retry_contract_risk": 0.58 if missing_fields else 0.14,
                            },
                        }
                    ],
                },
                "cooldown_outage_explainer": {
                    "summary": "No active cooldown or outage constraints detected." if not missing_fields else "Contract issues detected while provider remains available.",
                    "trigger_band": "stable" if not missing_fields else "watch",
                    "requested_provider": provider,
                    "recommended_provider": provider,
                    "runtime_lane": "parallel",
                    "cooldown_count": 0,
                    "outage_count": 0,
                    "affected_rows": [
                        {
                            "provider": provider,
                            "severity": "medium" if missing_fields else "low",
                            "pressure": 0.18 if not missing_fields else 0.58,
                            "retry_after_s": 0.0,
                            "cooldown_active": False,
                            "outage_active": False,
                            "blocker_codes": ["contract_missing_fields"] if missing_fields else [],
                            "summary": "Provider is healthy but payload contract is incomplete." if missing_fields else "Provider is healthy.",
                        }
                    ],
                    "recommended_actions": [
                        {
                            "id": "staged_retry_contract",
                            "label": "Use staged retry contract",
                            "summary": "Re-run preflight with a staged retry contract.",
                            "preflight_action": action_name,
                            "provider": provider,
                            "metadata_patch": {
                                "__external_retry_contract_mode": "adaptive_backoff",
                                "__external_retry_contract_risk": 0.58 if missing_fields else 0.18,
                            },
                        }
                    ],
                },
                "remediation_hints": [],
                "retry_policy": {"recommended_delay_s": 0.0, "mode": "immediate"},
            },
        }

    def simulate_external_connector_preflight(
        self,
        args: Dict[str, Any] | None = None,
        *,
        source: str = "desktop-ui",
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        payload = dict(args or {})
        action_name = str(payload.get("action", "")).strip().lower()
        if not action_name:
            return {"status": "error", "message": "action is required"}

        providers_raw = payload.get("providers", [])
        providers: list[str] = []
        if isinstance(providers_raw, list):
            for item in providers_raw:
                value = str(item or "").strip().lower()
                if value and value not in providers:
                    providers.append(value)
        elif isinstance(providers_raw, str):
            for item in providers_raw.split(","):
                value = str(item or "").strip().lower()
                if value and value not in providers:
                    providers.append(value)
        provider_default = str(payload.get("provider", "") or "").strip().lower()
        if provider_default and provider_default not in providers:
            providers.insert(0, provider_default)
        if not providers:
            providers = ["auto", "google", "graph"]

        max_runs = max(1, min(int(payload.get("max_runs", 20) or 20), 120))
        advisor_context = payload.get("advisor_context") if isinstance(payload.get("advisor_context"), dict) else {}
        compare_against_simulation_id = str(payload.get("compare_against_simulation_id", "") or "").strip()
        scenarios_raw = payload.get("scenarios", [])
        scenario_rows: list[Dict[str, Any]] = []
        if isinstance(scenarios_raw, list):
            for index, row in enumerate(scenarios_raw):
                if not isinstance(row, dict):
                    continue
                scenario_rows.append(
                    {
                        "id": str(row.get("id", f"scenario_{index + 1}")).strip() or f"scenario_{index + 1}",
                        "provider": str(row.get("provider", "")).strip().lower(),
                        "candidate_id": str(row.get("candidate_id", "")).strip(),
                        "execution_candidate": bool(row.get("execution_candidate", False)),
                        "args_patch": row.get("args_patch") if isinstance(row.get("args_patch"), dict) else {},
                        "metadata_patch": row.get("metadata_patch") if isinstance(row.get("metadata_patch"), dict) else {},
                    }
                )
        if not scenario_rows:
            scenario_rows = [{"id": "baseline", "args_patch": {}, "metadata_patch": {}}]

        rows: list[Dict[str, Any]] = []
        run_index = 0
        for scenario in scenario_rows:
            scenario_id = str(scenario.get("id", "baseline") or "baseline")
            scenario_provider = str(scenario.get("provider", "")).strip().lower()
            args_patch = scenario.get("args_patch") if isinstance(scenario.get("args_patch"), dict) else {}
            metadata_patch = scenario.get("metadata_patch") if isinstance(scenario.get("metadata_patch"), dict) else {}
            active_providers = [scenario_provider] if scenario_provider else providers
            for provider in active_providers:
                run_index += 1
                if run_index > max_runs:
                    break
                run_payload = {
                    "action": action_name,
                    "provider": provider,
                    "args": {
                        **(payload.get("args") if isinstance(payload.get("args"), dict) else {}),
                        **args_patch,
                    },
                }
                run_metadata = {**(metadata or {}), **metadata_patch}
                preflight = self.run_external_connector_preflight(run_payload, source=source, metadata=run_metadata)
                status_value = str(preflight.get("status", "")).strip().lower() or "error"
                orchestration = preflight.get("orchestration_diagnostics", {})
                pressure = float(orchestration.get("reliability_pressure", 0.0) or 0.0) if isinstance(orchestration, dict) else 0.0
                ready = status_value == "success"
                risk_score = 0.18 if ready else 0.72
                decision_score = max(
                    0.0,
                    min(1.0, (0.58 if ready else 0.22) + ((1.0 - risk_score) * 0.24) + ((1.0 - pressure) * 0.18)),
                )
                rows.append(
                    {
                        "run_index": run_index,
                        "scenario_id": scenario_id,
                        "provider_requested": provider,
                        "provider_selected": str(preflight.get("provider_selected", provider)),
                        "provider_recommended": str(preflight.get("provider_recommended", provider)),
                        "advisor_candidate_id": str(scenario.get("candidate_id", "")),
                        "execution_candidate": bool(scenario.get("execution_candidate", False)),
                        "status": status_value,
                        "preflight_ready": ready,
                        "blocked_by_reliability": False,
                        "risk_score": round(risk_score, 6),
                        "reliability_pressure": round(pressure, 6),
                        "decision_score": round(decision_score, 6),
                        "hint_count": len(preflight.get("remediation_hints", []))
                        if isinstance(preflight.get("remediation_hints"), list)
                        else 0,
                        "plan_count": len(preflight.get("remediation_plan", []))
                        if isinstance(preflight.get("remediation_plan"), list)
                        else 0,
                        "message": str(preflight.get("message", "")),
                        "approval_preview": {
                            "posture": "compare_required" if bool(scenario.get("execution_candidate", False)) else "ready",
                            "requires_compare": bool(scenario.get("execution_candidate", False)),
                            "requires_approval_ticket": bool(scenario.get("execution_candidate", False)),
                            "ready_to_execute": False,
                        }
                        if bool(scenario.get("execution_candidate", False))
                        else {},
                    }
                )
            if run_index > max_runs:
                break

        rows.sort(
            key=lambda item: (
                0 if bool(item.get("preflight_ready", False)) else 1,
                float(item.get("risk_score", 1.0) or 1.0),
                -float(item.get("decision_score", 0.0) or 0.0),
                str(item.get("provider_selected", "")),
            )
        )
        best = rows[0] if rows else {}
        recommended_provider = str(best.get("provider_selected", provider_default or "auto")).strip().lower() or "auto"
        fallback_chain: list[str] = []
        for row in rows:
            provider_name = str(row.get("provider_selected", "")).strip().lower()
            if provider_name and provider_name != recommended_provider and provider_name not in fallback_chain:
                fallback_chain.append(provider_name)
        confidence = float(best.get("decision_score", 0.0) or 0.0)
        created_at = datetime.now(timezone.utc).isoformat()
        self._connector_simulation_counter += 1
        simulation_id = f"sim_fake_{self._connector_simulation_counter:02d}"
        payload = {
            "status": "success",
            "simulation_id": simulation_id,
            "created_at": created_at,
            "action": action_name,
            "source": source,
            "requested_provider": provider_default or "auto",
            "providers": providers,
            "scenario_count": len(scenario_rows),
            "provider_count": len(providers),
            "total_runs": len(rows),
            "ready_count": sum(1 for row in rows if bool(row.get("preflight_ready", False))),
            "blocked_count": 0,
            "error_count": sum(1 for row in rows if str(row.get("status", "")) == "error"),
            "recommended_provider": recommended_provider,
            "fallback_chain": fallback_chain[:4],
            "recommendation_confidence": round(confidence, 6),
            "approval_summary": {
                "candidate_count": sum(1 for row in rows if bool(row.get("execution_candidate", False))),
                "approval_required_count": sum(1 for row in rows if bool(row.get("execution_candidate", False))),
                "compare_required_count": sum(
                    1
                    for row in rows
                    if bool(row.get("execution_candidate", False))
                    and float(row.get("risk_score", 0.0) or 0.0) >= 0.5
                ),
                "ready_stage_count": sum(1 for row in rows if bool(row.get("execution_candidate", False))),
                "ready_execute_count": 0,
                "posture": "compare_then_approve",
                "approval_required_ratio": 1.0 if any(bool(row.get("execution_candidate", False)) for row in rows) else 0.0,
                "compare_required_ratio": (
                    1.0
                    if any(
                        bool(row.get("execution_candidate", False))
                        and float(row.get("risk_score", 0.0) or 0.0) >= 0.5
                        for row in rows
                    )
                    else 0.0
                ),
                "ready_execute_ratio": 0.0,
                "summary": "Execution candidates require compare validation and approval before execution.",
            },
            "advisor_context": dict(advisor_context),
            "compare_against_simulation_id": compare_against_simulation_id,
            "promotion_preview": {
                "status": "success",
                "simulation_id": simulation_id,
                "eligible": True,
                "recommended_apply": False,
                "require_compare": True,
                "compare_status": "available" if compare_against_simulation_id else "missing",
                "compare_improved": bool(compare_against_simulation_id),
                "recommended_profile": "balanced",
                "recommended_controls": {
                    "allow_high_risk": False,
                    "max_steps": 6,
                    "require_compare": True,
                    "stop_on_blocked": True,
                },
                "promotion_score": 0.64 if compare_against_simulation_id else 0.52,
                "summary": (
                    "Simulation is promotion-ready and can be persisted as the active remediation policy."
                    if compare_against_simulation_id
                    else "Promotion is gated on compare evidence proving the new simulation path beats the baseline."
                ),
            },
            "results": rows,
            "count": len(rows),
        }
        payload["advisor_candidate_count"] = len(
            {
                str(row.get("advisor_candidate_id", "")).strip()
                for row in rows
                if str(row.get("advisor_candidate_id", "")).strip()
            }
        )
        payload["execution_candidate_count"] = sum(1 for row in rows if bool(row.get("execution_candidate", False)))
        history_row = dict(payload)
        self.connector_simulation_history.append(history_row)
        if len(self.connector_simulation_history) > 80:
            self.connector_simulation_history = self.connector_simulation_history[-80:]
        return payload

    def external_connector_preflight_simulation_templates(self, *, action: str, provider: str = "auto") -> Dict[str, Any]:
        action_name = str(action or "").strip().lower()
        if not action_name:
            return {"status": "error", "message": "action is required"}
        clean_provider = str(provider or "auto").strip().lower() or "auto"
        provider_candidates = [clean_provider, "google", "graph", "smtp"]
        dedup: list[str] = []
        for item in provider_candidates:
            if item not in dedup:
                dedup.append(item)
        templates = [
            {
                "template_id": "baseline_matrix",
                "name": "Baseline Provider Matrix",
                "description": "Baseline preflight sweep across providers.",
                "provider_candidates": dedup,
                "payload": {
                    "action": action_name,
                    "provider": clean_provider,
                    "providers": dedup,
                    "args": {"to": ["alice@example.com"], "subject": "Status update"},
                    "scenarios": [{"id": "baseline"}],
                },
            },
            {
                "template_id": "contract_gap_drill",
                "name": "Contract Gap Drill",
                "description": "Inject missing required fields to validate remediation quality.",
                "provider_candidates": dedup,
                "payload": {
                    "action": action_name,
                    "provider": clean_provider,
                    "providers": dedup,
                    "args": {"to": ["alice@example.com"], "subject": "Status update"},
                    "scenarios": [{"id": "baseline"}, {"id": "missing_required_contract", "args_patch": {"subject": ""}}],
                },
            },
            {
                "template_id": "advisor_execution_handoff",
                "name": "Advisor Execution Handoff",
                "description": "Replay execution-grade advisor candidates against baseline.",
                "provider_candidates": dedup,
                "payload": {
                    "action": action_name,
                    "provider": clean_provider,
                    "providers": dedup,
                    "args": {"to": ["alice@example.com"], "subject": "Status update"},
                    "advisor_context": {"source": "preflight_execution_handoff", "candidate_count": 1},
                    "scenarios": [
                        {"id": "baseline"},
                        {
                            "id": f"candidate_{clean_provider}",
                            "provider": clean_provider,
                            "candidate_id": f"candidate_{clean_provider}",
                            "execution_candidate": True,
                            "metadata_patch": {"policy_profile": "automation_safe"},
                        },
                    ],
                },
            },
        ]
        return {
            "status": "success",
            "action": action_name,
            "provider": clean_provider,
            "recommended_template_id": "advisor_execution_handoff",
            "provider_candidates": dedup,
            "reliability_pressure": 0.18,
            "blocked_by_reliability": False,
            "execution_handoff": {
                "candidate_count": 1,
                "candidates": [
                    {
                        "candidate_id": f"candidate_{clean_provider}",
                        "provider": clean_provider,
                    }
                ],
            },
            "templates": templates,
            "count": len(templates),
        }

    def external_connector_preflight_simulation_history(
        self,
        *,
        limit: int = 60,
        action: str = "",
        provider: str = "",
        status: str = "",
        include_results: bool = False,
    ) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 500))
        action_filter = str(action or "").strip().lower()
        provider_filter = str(provider or "").strip().lower()
        status_filter = str(status or "").strip().lower()
        rows = list(reversed(self.connector_simulation_history))
        filtered: list[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            if action_filter and str(row.get("action", "")).strip().lower() != action_filter:
                continue
            if provider_filter and str(row.get("recommended_provider", "")).strip().lower() != provider_filter:
                continue
            if status_filter:
                inferred = "error" if int(row.get("error_count", 0) or 0) > 0 else "success"
                if inferred != status_filter:
                    continue
            item = dict(row)
            if not include_results:
                item.pop("results", None)
            filtered.append(item)
            if len(filtered) >= bounded:
                break
        return {
            "status": "success",
            "items": filtered,
            "count": len(filtered),
            "total": len(rows),
            "limit": bounded,
            "filters": {
                "action": action_filter,
                "provider": provider_filter,
                "status": status_filter,
                "include_results": bool(include_results),
            },
        }

    def external_connector_preflight_simulation_trends(
        self,
        *,
        limit: int = 260,
        action: str = "",
        provider: str = "",
        status: str = "",
        recent_window: int = 16,
        baseline_window: int = 72,
    ) -> Dict[str, Any]:
        bounded_limit = max(20, min(int(limit), 500))
        bounded_recent = max(4, min(int(recent_window), 200))
        bounded_baseline = max(bounded_recent + 4, min(int(baseline_window), 500))
        action_filter = str(action or "").strip().lower()
        provider_filter = str(provider or "").strip().lower()
        status_filter = str(status or "").strip().lower()

        def _status_value(row: Dict[str, Any]) -> str:
            if int(row.get("error_count", 0) or 0) > 0:
                return "error"
            if int(row.get("blocked_count", 0) or 0) > 0:
                return "blocked"
            if int(row.get("ready_count", 0) or 0) <= 0:
                return "warning"
            return "success"

        rows = list(self.connector_simulation_history)[-bounded_limit:]
        snapshots: list[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_action = str(row.get("action", "")).strip().lower()
            row_provider = str(row.get("recommended_provider", row.get("requested_provider", ""))).strip().lower()
            row_status = _status_value(row)
            if action_filter and row_action != action_filter:
                continue
            if provider_filter and row_provider != provider_filter:
                continue
            if status_filter and row_status != status_filter:
                continue
            total = max(1, int(row.get("total_runs", 0) or 0))
            ready = int(row.get("ready_count", 0) or 0)
            errors = int(row.get("error_count", 0) or 0)
            blocked = int(row.get("blocked_count", 0) or 0)
            confidence = max(0.0, min(float(row.get("recommendation_confidence", 0.0) or 0.0), 1.0))
            ready_ratio = max(0.0, min(float(ready) / float(total), 1.0))
            error_ratio = max(0.0, min(float(errors) / float(total), 1.0))
            blocked_ratio = max(0.0, min(float(blocked) / float(total), 1.0))
            composite = max(
                0.0,
                min(1.0, (confidence * 0.42) + (ready_ratio * 0.34) + ((1.0 - error_ratio) * 0.16) + ((1.0 - blocked_ratio) * 0.08)),
            )
            snapshots.append(
                {
                    "simulation_id": str(row.get("simulation_id", "")),
                    "created_at": str(row.get("created_at", "")),
                    "recommended_provider": row_provider,
                    "status": row_status,
                    "ready_ratio": round(ready_ratio, 6),
                    "error_ratio": round(error_ratio, 6),
                    "confidence": round(confidence, 6),
                    "composite_score": round(composite, 6),
                }
            )

        recent = snapshots[-bounded_recent:]
        baseline = snapshots[-(bounded_recent + bounded_baseline) : -bounded_recent] if len(snapshots) > bounded_recent else []

        def _avg(rows_in: list[Dict[str, Any]], key: str) -> float:
            if not rows_in:
                return 0.0
            return sum(float(item.get(key, 0.0) or 0.0) for item in rows_in) / float(len(rows_in))

        recent_ready = _avg(recent, "ready_ratio")
        recent_error = _avg(recent, "error_ratio")
        recent_confidence = _avg(recent, "confidence")
        recent_composite = _avg(recent, "composite_score")
        baseline_ready = _avg(baseline, "ready_ratio") if baseline else recent_ready
        baseline_error = _avg(baseline, "error_ratio") if baseline else recent_error
        baseline_confidence = _avg(baseline, "confidence") if baseline else recent_confidence
        baseline_composite = _avg(baseline, "composite_score") if baseline else recent_composite
        provider_mix: Dict[str, int] = {}
        for item in recent:
            name = str(item.get("recommended_provider", "auto") or "auto")
            provider_mix[name] = provider_mix.get(name, 0) + 1

        recommended_profile = "balanced"
        if recent_error >= 0.3 or (recent_error - baseline_error) >= 0.1:
            recommended_profile = "strict"
        elif recent_ready >= 0.75 and recent_confidence >= 0.75 and recent_error <= 0.1:
            recommended_profile = "aggressive"

        profile_controls = {
            "strict": {"allow_high_risk": False, "max_steps": 4, "require_compare": True, "stop_on_blocked": True},
            "balanced": {"allow_high_risk": False, "max_steps": 8, "require_compare": False, "stop_on_blocked": True},
            "aggressive": {"allow_high_risk": True, "max_steps": 12, "require_compare": False, "stop_on_blocked": False},
        }

        return {
            "status": "success",
            "count": len(snapshots),
            "limit": bounded_limit,
            "recent_window": bounded_recent,
            "baseline_window": bounded_baseline,
            "filters": {"action": action_filter, "provider": provider_filter, "status": status_filter},
            "recent": {
                "count": len(recent),
                "ready_ratio": round(recent_ready, 6),
                "error_ratio": round(recent_error, 6),
                "confidence": round(recent_confidence, 6),
                "composite_score": round(recent_composite, 6),
            },
            "baseline": {
                "count": len(baseline),
                "ready_ratio": round(baseline_ready, 6),
                "error_ratio": round(baseline_error, 6),
                "confidence": round(baseline_confidence, 6),
                "composite_score": round(baseline_composite, 6),
            },
            "deltas": {
                "ready_ratio": round(recent_ready - baseline_ready, 6),
                "error_ratio": round(recent_error - baseline_error, 6),
                "confidence": round(recent_confidence - baseline_confidence, 6),
                "composite_score": round(recent_composite - baseline_composite, 6),
            },
            "stability": {
                "score": round(max(0.0, min(1.0, 1.0 - recent_error)), 6),
                "drift_pressure": round(max(0.0, (recent_error - baseline_error) + (baseline_ready - recent_ready)), 6),
                "switch_rate": 0.0,
                "confidence_volatility": 0.0,
            },
            "advisor_usage": {
                "recent": {
                    "replay_ratio": 1.0 if recent else 0.0,
                    "candidate_density": 1.0 if recent else 0.0,
                    "execution_density": 1.0 if recent else 0.0,
                    "compare_required_ratio": 0.5 if recent else 0.0,
                    "approval_required_ratio": 1.0 if recent else 0.0,
                },
                "baseline": {
                    "replay_ratio": 0.5 if baseline else 0.0,
                    "candidate_density": 0.5 if baseline else 0.0,
                    "execution_density": 0.5 if baseline else 0.0,
                    "compare_required_ratio": 0.25 if baseline else 0.0,
                    "approval_required_ratio": 0.5 if baseline else 0.0,
                },
                "deltas": {
                    "replay_ratio": 0.5 if recent and baseline else 0.0,
                    "candidate_density": 0.5 if recent and baseline else 0.0,
                    "execution_density": 0.5 if recent and baseline else 0.0,
                    "compare_required_ratio": 0.25 if recent and baseline else 0.0,
                    "approval_required_ratio": 0.5 if recent and baseline else 0.0,
                },
                "source_mix": [{"source": "preflight_execution_handoff", "count": len(recent), "share": 1.0 if recent else 0.0}],
            },
            "approval_pressure": {
                "status": "guarded" if recent else "ready",
                "recent": {
                    "approval_required_ratio": 1.0 if recent else 0.0,
                    "compare_required_ratio": 0.5 if recent else 0.0,
                    "manual_review_ratio": 1.0 if recent else 0.0,
                    "ready_execute_ratio": 0.0,
                },
                "baseline": {
                    "approval_required_ratio": 0.5 if baseline else 0.0,
                    "compare_required_ratio": 0.25 if baseline else 0.0,
                    "manual_review_ratio": 0.5 if baseline else 0.0,
                    "ready_execute_ratio": 0.0,
                },
                "deltas": {
                    "approval_required_ratio": 0.5 if recent and baseline else 0.0,
                    "compare_required_ratio": 0.25 if recent and baseline else 0.0,
                    "manual_review_ratio": 0.5 if recent and baseline else 0.0,
                    "ready_execute_ratio": 0.0,
                },
            },
            "execution_readiness": {
                "mode": "stage_then_compare" if recent else "manual_gate",
                "recent": {
                    "ready_execute_ratio": 0.0,
                    "execution_density": 1.0 if recent else 0.0,
                    "promotion_score": 0.62 if recent else 0.0,
                },
                "baseline": {
                    "ready_execute_ratio": 0.0,
                    "execution_density": 0.5 if baseline else 0.0,
                    "promotion_score": 0.44 if baseline else 0.0,
                },
                "deltas": {
                    "ready_execute_ratio": 0.0,
                    "execution_density": 0.5 if recent and baseline else 0.0,
                    "promotion_score": 0.18 if recent and baseline else 0.0,
                },
            },
            "promotion_readiness": {
                "status": "success",
                "simulation_id": str(snapshots[-1]["simulation_id"]) if snapshots else "",
                "eligible": True,
                "recommended_apply": False,
                "promotion_score": 0.62 if recent else 0.0,
                "summary": (
                    "Promotion is gated on compare evidence proving the new simulation path beats the baseline."
                    if recent
                    else "No recent simulation is available for promotion analysis."
                ),
                "recent_apply_ratio": 0.0,
                "baseline_apply_ratio": 0.0,
                "apply_ratio_delta": 0.0,
            },
            "provider_mix": [
                {"provider": name, "count": count, "share": round(float(count) / float(max(1, len(recent))), 6)}
                for name, count in provider_mix.items()
            ],
            "recommended_profile": recommended_profile,
            "recommended_controls": dict(profile_controls[recommended_profile]),
            "profile_controls": profile_controls,
            "snapshots": snapshots[-40:],
        }

    def external_connector_preflight_simulation_promote(
        self,
        *,
        simulation_id: str,
        compare_against_simulation_id: str = "",
        source: str = "desktop-ui",
        reason: str = "",
        dry_run: bool = True,
        force: bool = False,
        require_compare: bool = True,
        require_improvement: bool = True,
        mission_mode: str = "",
        limit: int = 320,
        recent_window: int = 16,
        baseline_window: int = 72,
        status: str = "",
    ) -> Dict[str, Any]:
        clean_id = str(simulation_id or "").strip()
        if not clean_id:
            return {"status": "error", "message": "simulation_id is required"}
        row = next(
            (
                dict(item)
                for item in reversed(self.connector_simulation_history)
                if isinstance(item, dict) and str(item.get("simulation_id", "")).strip() == clean_id
            ),
            None,
        )
        if not isinstance(row, dict):
            return {"status": "error", "message": f"simulation '{clean_id}' was not found"}
        row_index = next(
            (
                index
                for index in range(len(self.connector_simulation_history) - 1, -1, -1)
                if isinstance(self.connector_simulation_history[index], dict)
                and str(self.connector_simulation_history[index].get("simulation_id", "")).strip() == clean_id
            ),
            -1,
        )
        compare_target = str(compare_against_simulation_id or row.get("compare_against_simulation_id", "") or "").strip()
        promotion = {
            "status": "success",
            "simulation_id": clean_id,
            "eligible": True,
            "recommended_apply": not bool(dry_run),
            "require_compare": bool(require_compare),
            "compare_improved": bool(compare_target) or not bool(require_compare),
            "recommended_profile": "balanced",
            "recommended_controls": {
                "allow_high_risk": False,
                "max_steps": 6,
                "require_compare": bool(require_compare),
                "stop_on_blocked": True,
            },
            "promotion_score": 0.71 if compare_target or not require_compare else 0.55,
            "summary": (
                "Simulation is promotion-ready and can be persisted as the active remediation policy."
                if compare_target or not require_compare
                else "Promotion is gated on compare evidence proving the new simulation path beats the baseline."
            ),
        }
        payload: Dict[str, Any] = {
            "status": "dry_run" if dry_run else "applied",
            "simulation_id": clean_id,
            "compare_against_simulation_id": compare_target,
            "mission_mode": str(mission_mode or "stable").strip().lower() or "stable",
            "applied": not bool(dry_run),
            "promotion": promotion,
            "trend_summary": {
                "recent": {"ready_ratio": 1.0, "error_ratio": 0.0, "confidence": 0.82},
                "deltas": {"ready_ratio": 0.2, "error_ratio": -0.1, "confidence": 0.08},
                "stability": {"score": 0.88, "drift_pressure": 0.12},
            },
            "message": "Simulation promotion evaluated.",
        }
        if not dry_run:
            apply_payload = self.external_connector_remediation_policy_apply(
                action=str(row.get("action", "")).strip().lower(),
                provider=str(row.get("recommended_provider", row.get("requested_provider", "auto"))).strip().lower() or "auto",
                mission_mode=str(mission_mode or "stable").strip().lower() or "stable",
                profile="balanced",
                controls={"allow_high_risk": False, "max_steps": 6, "require_compare": bool(require_compare), "stop_on_blocked": True},
                source=source,
                reason=reason or "fake_promotion",
                metadata={"simulation_id": clean_id, "compare_against_simulation_id": compare_target},
                use_recommendation=False,
                limit=limit,
                recent_window=recent_window,
                baseline_window=baseline_window,
                status=status,
            )
            payload["apply"] = apply_payload
            payload["entry"] = apply_payload.get("entry", {})
            contract_state = (
                self.connector_execution_contract_state
                if isinstance(self.connector_execution_contract_state, dict)
                else {"version": "1.0", "updated_at": "", "contracts": {}, "history": []}
            )
            contracts = contract_state.get("contracts", {}) if isinstance(contract_state.get("contracts"), dict) else {}
            history = contract_state.get("history", []) if isinstance(contract_state.get("history"), list) else []
            scope_key = f"{str(row.get('action', '')).strip().lower()}|{str(row.get('recommended_provider', row.get('requested_provider', 'auto'))).strip().lower() or 'auto'}|{str(mission_mode or 'stable').strip().lower() or 'stable'}"
            previous = contracts.get(scope_key, {}) if isinstance(contracts.get(scope_key), dict) else {}
            contract_event_id = int(history[-1].get("event_id", 0) or 0) + 1 if history and isinstance(history[-1], dict) else 1
            now_iso = datetime.now(timezone.utc).isoformat()
            contract_entry = {
                "scope_key": scope_key,
                "legacy_scope_key": "|".join(scope_key.split("|")[:2]),
                "action": str(row.get("action", "")).strip().lower(),
                "provider": str(row.get("recommended_provider", row.get("requested_provider", "auto"))).strip().lower() or "auto",
                "mission_mode": str(mission_mode or "stable").strip().lower() or "stable",
                "simulation_id": clean_id,
                "compare_against_simulation_id": compare_target,
                "selected_provider": str(row.get("recommended_provider", row.get("requested_provider", "auto"))).strip().lower() or "auto",
                "recommended_provider": str(row.get("recommended_provider", row.get("requested_provider", "auto"))).strip().lower() or "auto",
                "profile": "balanced",
                "controls": {
                    "allow_high_risk": False,
                    "max_steps": 6,
                    "require_compare": bool(require_compare),
                    "stop_on_blocked": True,
                },
                "candidate_count": int(row.get("execution_candidate_count", 1) or 1),
                "ready_execute_count": 0,
                "candidate_summary": [
                    {
                        "candidate_id": "candidate_google",
                        "provider_selected": str(row.get("recommended_provider", "google")).strip().lower() or "google",
                        "provider_requested": str(row.get("requested_provider", "google")).strip().lower() or "google",
                        "status": "success",
                        "decision_score": 0.82,
                        "risk_score": 0.12,
                        "reliability_pressure": 0.18,
                        "approval_posture": "compare_then_approve" if require_compare else "review_required",
                        "approval_required": True,
                        "requires_compare": bool(require_compare),
                        "ready_to_execute": False,
                        "route_signature": f"{str(row.get('recommended_provider', 'google')).strip().lower() or 'google'}:candidate_google",
                    }
                ],
                "approval_summary": dict(row.get("approval_summary", {})) if isinstance(row.get("approval_summary"), dict) else {},
                "recommended_tool_action": dict(row.get("recommended_tool_action", {}))
                if isinstance(row.get("recommended_tool_action"), dict)
                else {},
                "recommended_args_patch": dict(row.get("recommended_args_patch", {}))
                if isinstance(row.get("recommended_args_patch"), dict)
                else {},
                "fallback_chain": list(row.get("fallback_chain", [])) if isinstance(row.get("fallback_chain"), list) else [],
                "promotion_score": float(promotion.get("promotion_score", 0.0) or 0.0),
                "promotion": dict(promotion),
                "route_signature": f"{str(row.get('recommended_provider', 'google')).strip().lower() or 'google'}:candidate_google",
                "updated_at": now_iso,
                "updated_by_source": str(source or "desktop-ui"),
                "reason": str(reason or "fake_promotion"),
                "version": int(previous.get("version", 0) or 0) + 1,
            }
            contracts[scope_key] = contract_entry
            contract_history_event = {
                "event_id": contract_event_id,
                "created_at": now_iso,
                "event_type": "promotion",
                **contract_entry,
            }
            history.append(contract_history_event)
            contract_state["contracts"] = contracts
            contract_state["history"] = history[-1200:]
            contract_state["updated_at"] = now_iso
            self.connector_execution_contract_state = contract_state
            payload["execution_contract"] = {
                "status": "success",
                "event_id": contract_event_id,
                "scope_key": scope_key,
                "entry": contract_entry,
                "history_event": contract_history_event,
                "message": f"Execution contract updated for {scope_key}.",
            }
            payload["execution_contract_entry"] = contract_entry
            if row_index >= 0:
                updated_row = dict(self.connector_simulation_history[row_index])
                updated_row["promotion_preview"] = dict(promotion)
                updated_row["policy_promotion"] = {
                    "status": payload["status"],
                    "applied": True,
                    "message": str(payload.get("message", "Simulation promotion evaluated.")),
                    "promotion": dict(promotion),
                    "apply": dict(apply_payload),
                    "entry": dict(apply_payload.get("entry", {}))
                    if isinstance(apply_payload.get("entry"), dict)
                    else {},
                    "execution_contract": dict(payload.get("execution_contract", {}))
                    if isinstance(payload.get("execution_contract"), dict)
                    else {},
                    "execution_contract_entry": dict(payload.get("execution_contract_entry", {}))
                    if isinstance(payload.get("execution_contract_entry"), dict)
                    else {},
                    "mission_mode": str(payload.get("mission_mode", "stable")),
                    "compare_against_simulation_id": compare_target,
                }
                self.connector_simulation_history[row_index] = updated_row
        return payload

    def external_connector_preflight_simulation_promotions(
        self,
        *,
        limit: int = 80,
        action: str = "",
        provider: str = "",
        mission_mode: str = "",
        status: str = "",
        applied_only: bool = False,
    ) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 5000))
        action_filter = str(action or "").strip().lower()
        provider_filter = str(provider or "").strip().lower()
        mission_mode_filter = str(mission_mode or "").strip().lower()
        status_filter = str(status or "").strip().lower()
        items: list[Dict[str, Any]] = []
        for row in reversed(self.connector_simulation_history):
            if not isinstance(row, dict):
                continue
            if action_filter and str(row.get("action", "")).strip().lower() != action_filter:
                continue
            provider_name = str(row.get("recommended_provider", row.get("requested_provider", "auto"))).strip().lower() or "auto"
            if provider_filter and provider_name != provider_filter:
                continue
            promotion_payload = row.get("policy_promotion", {}) if isinstance(row.get("policy_promotion"), dict) else {}
            if not promotion_payload:
                continue
            promotion_status = str(promotion_payload.get("status", "")).strip().lower() or "unknown"
            if status_filter and promotion_status != status_filter:
                continue
            if applied_only and not bool(promotion_payload.get("applied", False)):
                continue
            entry = promotion_payload.get("entry", {}) if isinstance(promotion_payload.get("entry"), dict) else {}
            apply_payload = promotion_payload.get("apply", {}) if isinstance(promotion_payload.get("apply"), dict) else {}
            history_event = apply_payload.get("history_event", {}) if isinstance(apply_payload.get("history_event"), dict) else {}
            mission_mode_name = (
                str(promotion_payload.get("mission_mode", entry.get("mission_mode", history_event.get("mission_mode", ""))) or "")
                .strip()
                .lower()
                or "*"
            )
            if mission_mode_filter and mission_mode_name != mission_mode_filter:
                continue
            promotion = promotion_payload.get("promotion", {}) if isinstance(promotion_payload.get("promotion"), dict) else {}
            items.append(
                {
                    "simulation_id": str(row.get("simulation_id", "")).strip(),
                    "created_at": str(row.get("created_at", "")).strip(),
                    "action": str(row.get("action", "")).strip().lower(),
                    "requested_provider": str(row.get("requested_provider", "auto")).strip().lower() or "auto",
                    "recommended_provider": provider_name,
                    "compare_against_simulation_id": str(
                        promotion_payload.get("compare_against_simulation_id", row.get("compare_against_simulation_id", ""))
                    ).strip(),
                    "promotion_status": promotion_status,
                    "applied": bool(promotion_payload.get("applied", False)),
                    "mission_mode": mission_mode_name,
                    "promotion_score": float(promotion.get("promotion_score", 0.0) or 0.0),
                    "recommended_profile": str(promotion.get("recommended_profile", entry.get("profile", "balanced"))).strip().lower()
                    or "balanced",
                    "gate_reasons": [
                        str(item).strip().lower()
                        for item in (promotion.get("gate_reasons", []) if isinstance(promotion.get("gate_reasons"), list) else [])
                        if str(item).strip()
                    ],
                    "summary": str(
                        promotion_payload.get("message", promotion.get("summary", "Connector promotion event recorded."))
                    ).strip()
                    or "Connector promotion event recorded.",
                    "scope_key": str(entry.get("scope_key", history_event.get("scope_key", ""))).strip().lower(),
                    "event_id": int(apply_payload.get("event_id", history_event.get("event_id", 0)) or 0),
                    "entry_version": int(entry.get("version", 0) or 0),
                    "policy_entry": dict(entry),
                    "history_event": dict(history_event),
                    "policy_promotion": dict(promotion_payload),
                    "promotion_preview": dict(row.get("promotion_preview", {}))
                    if isinstance(row.get("promotion_preview"), dict)
                    else {},
                }
            )
            if len(items) >= bounded:
                break
        return {
            "status": "success",
            "items": items,
            "count": len(items),
            "limit": bounded,
            "filters": {
                "action": action_filter,
                "provider": provider_filter,
                "mission_mode": mission_mode_filter,
                "status": status_filter,
                "applied_only": bool(applied_only),
            },
        }

    def external_connector_remediation_policies(
        self,
        *,
        limit: int = 180,
        action: str = "",
        provider: str = "",
        mission_mode: str = "",
        include_history: bool = False,
        history_limit: int = 40,
    ) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 500))
        bounded_history = max(1, min(int(history_limit), 1000))
        action_filter = str(action or "").strip().lower()
        provider_filter = str(provider or "").strip().lower()
        mission_mode_filter = str(mission_mode or "").strip().lower()
        state = self.connector_remediation_policy_state if isinstance(self.connector_remediation_policy_state, dict) else {}
        profiles = state.get("profiles", {}) if isinstance(state.get("profiles"), dict) else {}
        rows = [dict(item) for item in profiles.values() if isinstance(item, dict)]
        filtered: list[Dict[str, Any]] = []
        for row in rows:
            scope_key = str(row.get("scope_key", "")).strip().lower()
            scope_parts = [part for part in scope_key.split("|") if part]
            scope_mode = scope_parts[2] if len(scope_parts) >= 3 else "*"
            row_action = str(row.get("action", scope_parts[0] if len(scope_parts) >= 1 else "")).strip().lower()
            row_provider = str(row.get("provider", scope_parts[1] if len(scope_parts) >= 2 else "auto")).strip().lower() or "auto"
            row_mode = str(row.get("mission_mode", scope_mode)).strip().lower() or "*"
            if action_filter and row_action != action_filter:
                continue
            if provider_filter and row_provider != provider_filter:
                continue
            if mission_mode_filter and row_mode != mission_mode_filter:
                continue
            row_payload = dict(row)
            row_payload["action"] = row_action
            row_payload["provider"] = row_provider
            row_payload["mission_mode"] = row_mode
            filtered.append(row_payload)
        filtered.sort(key=lambda item: str(item.get("updated_at", "")), reverse=True)
        payload: Dict[str, Any] = {
            "status": "success",
            "items": filtered[:bounded],
            "count": min(len(filtered), bounded),
            "total": len(filtered),
            "state_updated_at": str(state.get("updated_at", "")),
            "filters": {"action": action_filter, "provider": provider_filter, "mission_mode": mission_mode_filter},
        }
        if include_history:
            history_rows = state.get("history", []) if isinstance(state.get("history"), list) else []
            history_items: list[Dict[str, Any]] = []
            for row in history_rows:
                if not isinstance(row, dict):
                    continue
                scope_key = str(row.get("scope_key", "")).strip().lower()
                scope_parts = [part for part in scope_key.split("|") if part]
                scope_mode = scope_parts[2] if len(scope_parts) >= 3 else "*"
                row_action = str(row.get("action", scope_parts[0] if len(scope_parts) >= 1 else "")).strip().lower()
                row_provider = str(row.get("provider", scope_parts[1] if len(scope_parts) >= 2 else "auto")).strip().lower() or "auto"
                row_mode = str(row.get("mission_mode", scope_mode)).strip().lower() or "*"
                if action_filter and row_action != action_filter:
                    continue
                if provider_filter and row_provider != provider_filter:
                    continue
                if mission_mode_filter and row_mode != mission_mode_filter:
                    continue
                row_payload = dict(row)
                row_payload["action"] = row_action
                row_payload["provider"] = row_provider
                row_payload["mission_mode"] = row_mode
                history_items.append(row_payload)
            history_items.sort(key=lambda item: int(item.get("event_id", 0) or 0), reverse=True)
            payload["history"] = history_items[:bounded_history]
            payload["history_count"] = min(len(history_items), bounded_history)
            payload["history_total"] = len(history_items)
        return payload

    def external_connector_remediation_policy_status(
        self,
        *,
        action: str,
        provider: str = "auto",
        mission_mode: str = "",
        include_history: bool = False,
        history_limit: int = 30,
        include_alerts: bool = True,
    ) -> Dict[str, Any]:
        action_name = str(action or "").strip().lower()
        if not action_name:
            return {"status": "error", "message": "action is required"}
        provider_name = str(provider or "auto").strip().lower() or "auto"
        mission_mode_name = str(mission_mode or "").strip().lower() or "stable"
        state = self.connector_remediation_policy_state if isinstance(self.connector_remediation_policy_state, dict) else {}
        profiles = state.get("profiles", {}) if isinstance(state.get("profiles"), dict) else {}
        scope_candidates = [
            f"{action_name}|{provider_name}|{mission_mode_name}",
            f"{action_name}|{provider_name}|*",
            f"{action_name}|auto|{mission_mode_name}",
            f"{action_name}|auto|*",
            f"*|{provider_name}|{mission_mode_name}",
            f"*|{provider_name}|*",
            f"*|auto|{mission_mode_name}",
            "*|auto|*",
            f"{action_name}|{provider_name}",
            f"{action_name}|auto",
            f"*|{provider_name}",
            "*|auto",
        ]
        entry: Dict[str, Any] = {}
        matched_scope = ""
        for scope_key in scope_candidates:
            row = profiles.get(scope_key)
            if isinstance(row, dict):
                entry = dict(row)
                matched_scope = scope_key
                break
        profile = str(entry.get("profile", "balanced") or "balanced").strip().lower() or "balanced"
        controls = entry.get("controls") if isinstance(entry.get("controls"), dict) else {}
        payload: Dict[str, Any] = {
            "status": "success",
            "action": action_name,
            "provider": provider_name,
            "mission_mode": mission_mode_name,
            "scope_key": f"{action_name}|{provider_name}|{mission_mode_name}",
            "scope_candidates": scope_candidates,
            "matched_scope_key": matched_scope,
            "profile": profile,
            "controls": controls,
            "entry": entry,
            "state_updated_at": str(state.get("updated_at", "")),
            "defaults": {
                "strict": {"allow_high_risk": False, "max_steps": 4, "require_compare": True, "stop_on_blocked": True},
                "balanced": {"allow_high_risk": False, "max_steps": 8, "require_compare": False, "stop_on_blocked": True},
                "aggressive": {"allow_high_risk": True, "max_steps": 12, "require_compare": False, "stop_on_blocked": False},
            },
        }
        if include_history:
            bounded_history = max(1, min(int(history_limit), 1000))
            history_rows = state.get("history", []) if isinstance(state.get("history"), list) else []
            scoped = [
                dict(item)
                for item in history_rows
                if isinstance(item, dict) and str(item.get("scope_key", "")).strip().lower() in set(scope_candidates)
            ]
            scoped.sort(key=lambda item: int(item.get("event_id", 0) or 0), reverse=True)
            payload["history"] = scoped[:bounded_history]
            payload["history_count"] = min(len(scoped), bounded_history)
            payload["history_total"] = len(scoped)
        if include_alerts:
            recommendation = self.external_connector_remediation_policy_recommendation(
                action=action_name,
                provider=provider_name,
                mission_mode=mission_mode_name,
            )
            recommendation_row = (
                recommendation.get("recommendation", {})
                if isinstance(recommendation.get("recommendation"), dict)
                else {}
            )
            alerts: list[Dict[str, Any]] = []
            recommended_profile = str(recommendation_row.get("profile", profile)).strip().lower() or profile
            confidence = float(recommendation_row.get("confidence", 0.0) or 0.0)
            if recommended_profile != profile and confidence >= 0.55:
                alerts.append(
                    {
                        "id": "policy_profile_drift_detected",
                        "severity": "medium",
                        "message": "Fake alert: profile drift detected for connector remediation policy.",
                        "recommended_profile": recommended_profile,
                        "confidence": confidence,
                    }
                )
            payload["alerts"] = alerts
            payload["alert_count"] = len(alerts)
            payload["highest_alert_severity"] = "medium" if alerts else "none"
        return payload

    def external_connector_remediation_policy_recommendation(
        self,
        *,
        action: str,
        provider: str = "auto",
        mission_mode: str = "",
        limit: int = 320,
        recent_window: int = 16,
        baseline_window: int = 72,
        status: str = "",
    ) -> Dict[str, Any]:
        action_name = str(action or "").strip().lower()
        if not action_name:
            return {"status": "error", "message": "action is required"}
        provider_name = str(provider or "auto").strip().lower() or "auto"
        mission_mode_name = str(mission_mode or "").strip().lower() or "stable"
        trend = self.external_connector_preflight_simulation_trends(
            limit=limit,
            action=action_name,
            provider=provider_name,
            status=status,
            recent_window=recent_window,
            baseline_window=baseline_window,
        )
        if trend.get("status") != "success":
            return {"status": "error", "message": "trend diagnostics failed"}
        recommended_profile = str(trend.get("recommended_profile", "balanced") or "balanced").strip().lower() or "balanced"
        recommended_controls = trend.get("recommended_controls", {}) if isinstance(trend.get("recommended_controls"), dict) else {}
        current = self.external_connector_remediation_policy_status(
            action=action_name,
            provider=provider_name,
            mission_mode=mission_mode_name,
            include_alerts=False,
        )
        return {
            "status": "success",
            "action": action_name,
            "provider": provider_name,
            "mission_mode": mission_mode_name,
            "current": {
                "profile": str(current.get("profile", "balanced")),
                "controls": current.get("controls", {}),
                "matched_scope_key": str(current.get("matched_scope_key", "")),
            },
            "recommendation": {
                "profile": recommended_profile,
                "controls": recommended_controls,
                "confidence": 0.72,
                "reasons": ["Trend diagnostics indicate profile adjustment opportunity."],
                "would_change_profile": str(current.get("profile", "balanced")) != recommended_profile,
            },
            "trend_summary": {
                "count": int(trend.get("count", 0) or 0),
                "recent": trend.get("recent", {}),
                "deltas": trend.get("deltas", {}),
                "stability": trend.get("stability", {}),
                "provider_mix": trend.get("provider_mix", []),
            },
        }

    def external_connector_remediation_policy_apply(
        self,
        *,
        action: str,
        provider: str = "auto",
        mission_mode: str = "",
        profile: str = "",
        controls: Any = None,
        source: str = "desktop-ui",
        reason: str = "",
        metadata: Dict[str, Any] | None = None,
        use_recommendation: bool = True,
        limit: int = 320,
        recent_window: int = 16,
        baseline_window: int = 72,
        status: str = "",
    ) -> Dict[str, Any]:
        action_name = str(action or "").strip().lower()
        if not action_name:
            return {"status": "error", "message": "action is required"}
        provider_name = str(provider or "auto").strip().lower() or "auto"
        metadata_payload = metadata if isinstance(metadata, dict) else {}
        mission_mode_name = str(mission_mode or metadata_payload.get("mission_mode") or "").strip().lower() or "*"
        recommendation_payload: Dict[str, Any] = {}
        if use_recommendation or not str(profile or "").strip():
            recommendation_payload = self.external_connector_remediation_policy_recommendation(
                action=action_name,
                provider=provider_name,
                mission_mode=mission_mode_name,
                limit=limit,
                recent_window=recent_window,
                baseline_window=baseline_window,
                status=status,
            )
            if recommendation_payload.get("status") != "success":
                return recommendation_payload
        recommendation = recommendation_payload.get("recommendation", {}) if isinstance(recommendation_payload.get("recommendation"), dict) else {}
        effective_profile = str(profile or recommendation.get("profile", "balanced") or "balanced").strip().lower() or "balanced"
        defaults = {
            "strict": {"allow_high_risk": False, "max_steps": 4, "require_compare": True, "stop_on_blocked": True},
            "balanced": {"allow_high_risk": False, "max_steps": 8, "require_compare": False, "stop_on_blocked": True},
            "aggressive": {"allow_high_risk": True, "max_steps": 12, "require_compare": False, "stop_on_blocked": False},
        }
        effective_controls = dict(defaults.get(effective_profile, defaults["balanced"]))
        if use_recommendation and isinstance(recommendation.get("controls"), dict):
            effective_controls.update(recommendation.get("controls", {}))
        if isinstance(controls, dict):
            effective_controls.update(controls)
        state = self.connector_remediation_policy_state if isinstance(self.connector_remediation_policy_state, dict) else {}
        profiles = state.get("profiles", {}) if isinstance(state.get("profiles"), dict) else {}
        history_rows = state.get("history", []) if isinstance(state.get("history"), list) else []
        scope_key = f"{action_name}|{provider_name}|{mission_mode_name}"
        legacy_scope_key = f"{action_name}|{provider_name}"
        previous = profiles.get(scope_key, {}) if isinstance(profiles.get(scope_key), dict) else {}
        now_iso = datetime.now(timezone.utc).isoformat()
        event_id = int(history_rows[-1].get("event_id", 0) or 0) + 1 if history_rows and isinstance(history_rows[-1], dict) else 1
        entry = {
            "scope_key": scope_key,
            "legacy_scope_key": legacy_scope_key,
            "action": action_name,
            "provider": provider_name,
            "mission_mode": mission_mode_name,
            "profile": effective_profile,
            "controls": effective_controls,
            "version": int(previous.get("version", 0) or 0) + 1,
            "updated_at": now_iso,
            "updated_by_source": str(source or "desktop-ui"),
            "reason": str(reason or ""),
            "recommendation_confidence": float(recommendation.get("confidence", 0.0) or 0.0),
            "metadata": dict(metadata_payload),
        }
        profiles[scope_key] = entry
        history_rows.append(
            {
                "event_id": event_id,
                "created_at": now_iso,
                "scope_key": scope_key,
                "legacy_scope_key": legacy_scope_key,
                "action": action_name,
                "provider": provider_name,
                "mission_mode": mission_mode_name,
                "profile": effective_profile,
                "controls": effective_controls,
                "source": str(source or "desktop-ui"),
                "reason": str(reason or ""),
                "recommendation_confidence": float(recommendation.get("confidence", 0.0) or 0.0),
                "used_recommendation": bool(use_recommendation),
            }
        )
        state["profiles"] = profiles
        state["history"] = history_rows[-1200:]
        state["updated_at"] = now_iso
        self.connector_remediation_policy_state = state
        history_event = {
            "event_id": event_id,
            "created_at": now_iso,
            "scope_key": scope_key,
            "legacy_scope_key": legacy_scope_key,
            "action": action_name,
            "provider": provider_name,
            "mission_mode": mission_mode_name,
            "profile": effective_profile,
            "controls": effective_controls,
            "source": str(source or "desktop-ui"),
            "reason": str(reason or ""),
            "recommendation_confidence": float(recommendation.get("confidence", 0.0) or 0.0),
            "used_recommendation": bool(use_recommendation),
            "metadata": dict(metadata_payload),
        }
        return {
            "status": "success",
            "action": action_name,
            "provider": provider_name,
            "mission_mode": mission_mode_name,
            "scope_key": scope_key,
            "event_id": event_id,
            "entry": entry,
            "history_event": history_event,
            "recommendation": recommendation,
            "message": f"Remediation policy updated for {scope_key}.",
        }

    def external_connector_remediation_policy_restore(
        self,
        *,
        event_id: int = 0,
        action: str = "",
        provider: str = "",
        mission_mode: str = "",
        source: str = "desktop-ui",
        reason: str = "",
        dry_run: bool = True,
        force: bool = False,
    ) -> Dict[str, Any]:
        requested_event_id = int(event_id or 0)
        action_filter = str(action or "").strip().lower()
        provider_filter = str(provider or "").strip().lower()
        mission_mode_filter = str(mission_mode or "").strip().lower()
        state = self.connector_remediation_policy_state if isinstance(self.connector_remediation_policy_state, dict) else {}
        history_rows = state.get("history", []) if isinstance(state.get("history"), list) else []

        target: Dict[str, Any] = {}
        for row in reversed(history_rows):
            if not isinstance(row, dict):
                continue
            if requested_event_id and int(row.get("event_id", 0) or 0) != requested_event_id:
                continue
            row_action = str(row.get("action", "")).strip().lower()
            row_provider = str(row.get("provider", "auto")).strip().lower() or "auto"
            row_mode = str(row.get("mission_mode", "*")).strip().lower() or "*"
            if action_filter and row_action != action_filter:
                continue
            if provider_filter and row_provider != provider_filter:
                continue
            if mission_mode_filter and row_mode != mission_mode_filter:
                continue
            target = dict(row)
            break
        if not target:
            return {"status": "error", "message": "matching remediation policy history event was not found"}

        current = self.external_connector_remediation_policy_status(
            action=str(target.get("action", "")).strip().lower(),
            provider=str(target.get("provider", "auto")).strip().lower() or "auto",
            mission_mode=str(target.get("mission_mode", "*")).strip().lower() or "*",
            include_history=False,
            include_alerts=False,
        )
        current_entry = current.get("entry", {}) if isinstance(current.get("entry"), dict) else {}
        current_profile = str(current_entry.get("profile", "balanced")).strip().lower() or "balanced"
        target_profile = str(target.get("profile", "balanced")).strip().lower() or "balanced"
        current_controls = current_entry.get("controls", {}) if isinstance(current_entry.get("controls"), dict) else {}
        target_controls = target.get("controls", {}) if isinstance(target.get("controls"), dict) else {}
        changed_controls = [
            {
                "key": str(key),
                "before": current_controls.get(key),
                "after": target_controls.get(key),
            }
            for key in sorted(set(current_controls.keys()) | set(target_controls.keys()))
            if current_controls.get(key) != target_controls.get(key)
        ]
        diff = {
            "profile_changed": current_profile != target_profile,
            "before_profile": current_profile,
            "after_profile": target_profile,
            "changed_control_count": len(changed_controls),
            "changed_controls": changed_controls[:12],
            "summary": (
                "Selected policy event matches the current persisted configuration."
                if current_profile == target_profile and not changed_controls
                else (
                    f"Restoring this event will switch profile from '{current_profile}' to '{target_profile}' "
                    f"and change {len(changed_controls)} control field(s)."
                )
            ),
        }
        if dry_run:
            return {
                "status": "dry_run",
                "applied": False,
                "event_id": int(target.get("event_id", 0) or 0),
                "target": target,
                "current": current_entry,
                "diff": diff,
                "message": str(diff.get("summary", "Policy restore preview completed.")),
            }
        if not force and not diff["profile_changed"] and int(diff["changed_control_count"]) <= 0:
            return {
                "status": "skip",
                "applied": False,
                "event_id": int(target.get("event_id", 0) or 0),
                "target": target,
                "current": current_entry,
                "diff": diff,
                "message": "Selected policy event already matches the current persisted configuration.",
            }
        apply_payload = self.external_connector_remediation_policy_apply(
            action=str(target.get("action", "")).strip().lower(),
            provider=str(target.get("provider", "auto")).strip().lower() or "auto",
            mission_mode=str(target.get("mission_mode", "*")).strip().lower() or "*",
            profile=str(target.get("profile", "balanced")).strip().lower() or "balanced",
            controls=target_controls,
            source=str(source or "desktop-ui").strip() or "desktop-ui",
            reason=str(reason or "restore_history_event").strip() or "restore_history_event",
            metadata={"restore_event_id": int(target.get("event_id", 0) or 0), "restore_diff": diff},
            use_recommendation=False,
        )
        return {
            "status": "applied",
            "applied": True,
            "event_id": int(target.get("event_id", 0) or 0),
            "target": target,
            "current": current_entry,
            "diff": diff,
            "apply": apply_payload,
            "entry": apply_payload.get("entry", {}),
            "message": str(apply_payload.get("message", "Policy restore applied from history event.")),
        }

    def external_connector_execution_contract_status(
        self,
        *,
        action: str,
        provider: str = "auto",
        mission_mode: str = "",
        include_history: bool = False,
        history_limit: int = 30,
    ) -> Dict[str, Any]:
        action_name = str(action or "").strip().lower()
        if not action_name:
            return {"status": "error", "message": "action is required"}
        provider_name = str(provider or "auto").strip().lower() or "auto"
        mission_mode_name = str(mission_mode or "").strip().lower() or "stable"
        state = self.connector_execution_contract_state if isinstance(self.connector_execution_contract_state, dict) else {}
        contracts = state.get("contracts", {}) if isinstance(state.get("contracts"), dict) else {}
        scope_candidates = [
            f"{action_name}|{provider_name}|{mission_mode_name}",
            f"{action_name}|{provider_name}|*",
            f"{action_name}|auto|{mission_mode_name}",
            f"{action_name}|auto|*",
            f"*|{provider_name}|{mission_mode_name}",
            f"*|{provider_name}|*",
            f"*|auto|{mission_mode_name}",
            "*|auto|*",
        ]
        entry: Dict[str, Any] = {}
        matched_scope = ""
        for scope_key in scope_candidates:
            row = contracts.get(scope_key)
            if isinstance(row, dict):
                entry = dict(row)
                matched_scope = scope_key
                break
        payload: Dict[str, Any] = {
            "status": "success",
            "action": action_name,
            "provider": provider_name,
            "mission_mode": mission_mode_name,
            "scope_key": f"{action_name}|{provider_name}|{mission_mode_name}",
            "scope_candidates": scope_candidates,
            "matched_scope_key": matched_scope,
            "entry": entry,
            "selected_provider": str(entry.get("selected_provider", provider_name)).strip().lower() or provider_name,
            "profile": str(entry.get("profile", "balanced")).strip().lower() or "balanced",
            "candidate_count": int(entry.get("candidate_count", 0) or 0),
            "ready_execute_count": int(entry.get("ready_execute_count", 0) or 0),
            "state_updated_at": str(state.get("updated_at", "")),
        }
        if include_history:
            bounded = max(1, min(int(history_limit), 1000))
            history_rows = state.get("history", []) if isinstance(state.get("history"), list) else []
            scoped = [
                dict(item)
                for item in history_rows
                if isinstance(item, dict) and str(item.get("scope_key", "")).strip().lower() in set(scope_candidates)
            ]
            scoped.sort(key=lambda item: int(item.get("event_id", 0) or 0), reverse=True)
            payload["history"] = scoped[:bounded]
            payload["history_count"] = min(len(scoped), bounded)
            payload["history_total"] = len(scoped)
        return payload

    def external_connector_execution_contract_restore(
        self,
        *,
        event_id: int = 0,
        action: str = "",
        provider: str = "",
        mission_mode: str = "",
        source: str = "desktop-ui",
        reason: str = "",
        dry_run: bool = True,
        force: bool = False,
    ) -> Dict[str, Any]:
        requested_event_id = int(event_id or 0)
        action_filter = str(action or "").strip().lower()
        provider_filter = str(provider or "").strip().lower()
        mission_mode_filter = str(mission_mode or "").strip().lower()
        state = self.connector_execution_contract_state if isinstance(self.connector_execution_contract_state, dict) else {}
        history_rows = state.get("history", []) if isinstance(state.get("history"), list) else []
        target: Dict[str, Any] = {}
        for row in reversed(history_rows):
            if not isinstance(row, dict):
                continue
            if requested_event_id and int(row.get("event_id", 0) or 0) != requested_event_id:
                continue
            row_action = str(row.get("action", "")).strip().lower()
            row_provider = str(row.get("provider", "auto")).strip().lower() or "auto"
            row_mode = str(row.get("mission_mode", "*")).strip().lower() or "*"
            if action_filter and row_action != action_filter:
                continue
            if provider_filter and row_provider != provider_filter:
                continue
            if mission_mode_filter and row_mode != mission_mode_filter:
                continue
            target = dict(row)
            break
        if not target:
            return {"status": "error", "message": "matching execution contract history event was not found"}
        current = self.external_connector_execution_contract_status(
            action=str(target.get("action", "")).strip().lower(),
            provider=str(target.get("provider", "auto")).strip().lower() or "auto",
            mission_mode=str(target.get("mission_mode", "*")).strip().lower() or "*",
            include_history=False,
        )
        current_entry = current.get("entry", {}) if isinstance(current.get("entry"), dict) else {}
        changed_fields = [
            key
            for key in (
                "selected_provider",
                "route_signature",
                "recommended_tool_action",
                "recommended_args_patch",
                "candidate_summary",
            )
            if current_entry.get(key) != target.get(key)
        ]
        diff = {
            "changed_field_count": len(changed_fields),
            "changed_fields": changed_fields,
            "summary": (
                "Selected execution contract event matches the current persisted contract."
                if not changed_fields
                else f"Restoring this contract will change {len(changed_fields)} persisted execution field(s)."
            ),
        }
        if dry_run:
            return {
                "status": "dry_run",
                "applied": False,
                "event_id": int(target.get("event_id", 0) or 0),
                "target": target,
                "current": current_entry,
                "diff": diff,
                "message": str(diff.get("summary", "Execution contract restore preview completed.")),
            }
        if not force and not changed_fields:
            return {
                "status": "skip",
                "applied": False,
                "event_id": int(target.get("event_id", 0) or 0),
                "target": target,
                "current": current_entry,
                "diff": diff,
                "message": "Selected execution contract event already matches the current persisted contract.",
            }
        contracts = state.get("contracts", {}) if isinstance(state.get("contracts"), dict) else {}
        history = state.get("history", []) if isinstance(state.get("history"), list) else []
        scope_key = str(target.get("scope_key", "")).strip().lower()
        previous = contracts.get(scope_key, {}) if isinstance(contracts.get(scope_key), dict) else {}
        now_iso = datetime.now(timezone.utc).isoformat()
        next_event_id = int(history[-1].get("event_id", 0) or 0) + 1 if history and isinstance(history[-1], dict) else 1
        entry = {
            key: value
            for key, value in target.items()
            if key
            not in {
                "event_id",
                "created_at",
                "event_type",
                "updated_at",
                "updated_by_source",
                "reason",
                "metadata",
                "version",
            }
        }
        entry["updated_at"] = now_iso
        entry["updated_by_source"] = str(source or "desktop-ui")
        entry["reason"] = str(reason or "restore_execution_contract_history")
        entry["version"] = int(previous.get("version", 0) or 0) + 1
        contracts[scope_key] = entry
        history_event = {
            "event_id": next_event_id,
            "created_at": now_iso,
            "event_type": "restore",
            **entry,
        }
        history.append(history_event)
        state["contracts"] = contracts
        state["history"] = history[-1200:]
        state["updated_at"] = now_iso
        self.connector_execution_contract_state = state
        return {
            "status": "applied",
            "applied": True,
            "event_id": int(target.get("event_id", 0) or 0),
            "target": target,
            "current": current_entry,
            "diff": diff,
            "apply": {"status": "success", "entry": entry, "event_id": next_event_id, "history_event": history_event},
            "entry": entry,
            "message": "Execution contract restore applied from history event.",
        }

    def _connector_remediation_autotune_state(self) -> Dict[str, Any]:
        state = self.connector_remediation_policy_state if isinstance(self.connector_remediation_policy_state, dict) else {}
        raw = state.get("autotune", {}) if isinstance(state.get("autotune"), dict) else {}
        autotune = {
            "runs": int(raw.get("runs", 0) or 0),
            "applied": int(raw.get("applied", 0) or 0),
            "last_status": str(raw.get("last_status", "") or "").strip().lower(),
            "last_reason": str(raw.get("last_reason", "") or "").strip(),
            "last_run_at": str(raw.get("last_run_at", "") or "").strip(),
            "last_apply_at": str(raw.get("last_apply_at", "") or "").strip(),
            "last_scope_key": str(raw.get("last_scope_key", "") or "").strip().lower(),
            "last_run_monotonic": float(raw.get("last_run_monotonic", 0.0) or 0.0),
            "last_apply_by_scope": dict(raw.get("last_apply_by_scope", {}))
            if isinstance(raw.get("last_apply_by_scope"), dict)
            else {},
            "history": [dict(item) for item in raw.get("history", []) if isinstance(item, dict)]
            if isinstance(raw.get("history"), list)
            else [],
        }
        state["autotune"] = autotune
        self.connector_remediation_policy_state = state
        return autotune

    def external_connector_remediation_policy_autotune_status(self, *, limit: int = 40) -> Dict[str, Any]:
        bounded_limit = max(1, min(int(limit), 1000))
        state = self.connector_remediation_policy_state if isinstance(self.connector_remediation_policy_state, dict) else {}
        autotune = self._connector_remediation_autotune_state()
        history = list(autotune.get("history", []))
        items = history[-bounded_limit:]
        items.reverse()
        return {
            "status": "success",
            "enabled": True,
            "config": {
                "interval_s": 90.0,
                "apply_cooldown_s": 220.0,
                "min_confidence": 0.66,
                "min_samples": 1,
                "min_drift_pressure": 0.0,
            },
            "state_updated_at": str(state.get("updated_at", "")),
            "runs": int(autotune.get("runs", 0) or 0),
            "applied": int(autotune.get("applied", 0) or 0),
            "last_status": str(autotune.get("last_status", "") or ""),
            "last_reason": str(autotune.get("last_reason", "") or ""),
            "last_run_at": str(autotune.get("last_run_at", "") or ""),
            "last_apply_at": str(autotune.get("last_apply_at", "") or ""),
            "last_scope_key": str(autotune.get("last_scope_key", "") or ""),
            "cooldown_remaining_s": 0.0,
            "history": items,
            "history_count": len(items),
            "history_total": len(history),
        }

    def external_connector_remediation_policy_autotune(
        self,
        *,
        action: str,
        provider: str = "auto",
        mission_mode: str = "",
        source: str = "desktop-ui",
        reason: str = "",
        force: bool = False,
        dry_run: bool = False,
        limit: int = 320,
        recent_window: int = 16,
        baseline_window: int = 72,
        status: str = "",
    ) -> Dict[str, Any]:
        action_name = str(action or "").strip().lower()
        if not action_name:
            return {"status": "error", "message": "action is required"}
        provider_name = str(provider or "auto").strip().lower() or "auto"
        mission_mode_name = str(mission_mode or "").strip().lower() or "stable"
        recommendation_payload = self.external_connector_remediation_policy_recommendation(
            action=action_name,
            provider=provider_name,
            mission_mode=mission_mode_name,
            limit=limit,
            recent_window=recent_window,
            baseline_window=baseline_window,
            status=status,
        )
        if recommendation_payload.get("status") != "success":
            return {"status": "error", "message": "recommendation failed"}

        recommendation = (
            recommendation_payload.get("recommendation", {})
            if isinstance(recommendation_payload.get("recommendation"), dict)
            else {}
        )
        trend_summary = (
            recommendation_payload.get("trend_summary", {})
            if isinstance(recommendation_payload.get("trend_summary"), dict)
            else {}
        )
        current = recommendation_payload.get("current", {}) if isinstance(recommendation_payload.get("current"), dict) else {}
        current_profile = str(current.get("profile", "balanced") or "balanced").strip().lower() or "balanced"
        current_controls = current.get("controls", {}) if isinstance(current.get("controls"), dict) else {}
        recommended_profile = str(recommendation.get("profile", current_profile) or current_profile).strip().lower() or current_profile
        recommended_controls = recommendation.get("controls", {}) if isinstance(recommendation.get("controls"), dict) else {}
        confidence = float(recommendation.get("confidence", 0.0) or 0.0)
        sample_count = int(trend_summary.get("count", 0) or 0)
        scope_key = f"{action_name}|{provider_name}|{mission_mode_name}"
        profile_change = recommended_profile != current_profile
        controls_change = current_controls != recommended_controls
        should_apply = bool(force) or (sample_count >= 1 and confidence >= 0.55 and (profile_change or controls_change))
        result_status = "skip"
        decision_reason = "no_policy_change"
        apply_payload: Dict[str, Any] = {}
        applied = False
        if should_apply and dry_run:
            result_status = "dry_run"
            decision_reason = "apply"
        elif should_apply:
            apply_payload = self.external_connector_remediation_policy_apply(
                action=action_name,
                provider=provider_name,
                mission_mode=mission_mode_name,
                source=source,
                reason=f"autotune:{reason or 'manual'}",
                metadata={"autotune": {"confidence": confidence, "sample_count": sample_count}},
                use_recommendation=True,
                limit=limit,
                recent_window=recent_window,
                baseline_window=baseline_window,
                status=status,
            )
            applied = apply_payload.get("status") == "success"
            result_status = "applied" if applied else "error"
            decision_reason = "apply" if applied else "apply_failed"
        elif sample_count < 1 and not force:
            decision_reason = "insufficient_samples"

        autotune = self._connector_remediation_autotune_state()
        now_iso = datetime.now(timezone.utc).isoformat()
        history = list(autotune.get("history", []))
        history.append(
            {
                "run_at": now_iso,
                "status": result_status,
                "reason": decision_reason,
                "action": action_name,
                "provider": provider_name,
                "mission_mode": mission_mode_name,
                "scope_key": scope_key,
                "source": source,
                "dry_run": bool(dry_run),
                "force": bool(force),
                "confidence": round(confidence, 6),
                "sample_count": int(sample_count),
                "drift_pressure": float(
                    (
                        trend_summary.get("stability", {})
                        if isinstance(trend_summary.get("stability", {}), dict)
                        else {}
                    ).get("drift_pressure", 0.0)
                    or 0.0
                ),
                "profile_from": current_profile,
                "profile_to": recommended_profile,
            }
        )
        autotune["runs"] = int(autotune.get("runs", 0) or 0) + 1
        autotune["applied"] = int(autotune.get("applied", 0) or 0) + (1 if applied else 0)
        autotune["last_status"] = result_status
        autotune["last_reason"] = decision_reason
        autotune["last_run_at"] = now_iso
        autotune["last_scope_key"] = scope_key
        autotune["last_run_monotonic"] = float(autotune["runs"])
        autotune["history"] = history[-240:]
        if applied:
            autotune["last_apply_at"] = now_iso
            last_apply_by_scope = autotune.get("last_apply_by_scope", {})
            if not isinstance(last_apply_by_scope, dict):
                last_apply_by_scope = {}
            last_apply_by_scope[scope_key] = float(autotune["runs"])
            autotune["last_apply_by_scope"] = last_apply_by_scope
        self.connector_remediation_policy_state["autotune"] = autotune
        return {
            "status": result_status,
            "applied": bool(applied),
            "reason": decision_reason,
            "action": action_name,
            "provider": provider_name,
            "mission_mode": mission_mode_name,
            "scope_key": scope_key,
            "source": source,
            "dry_run": bool(dry_run),
            "force": bool(force),
            "decision": {
                "confidence": round(confidence, 6),
                "sample_count": int(sample_count),
                "drift_pressure": float(
                    (
                        trend_summary.get("stability", {})
                        if isinstance(trend_summary.get("stability", {}), dict)
                        else {}
                    ).get("drift_pressure", 0.0)
                    or 0.0
                ),
                "profile_change": bool(profile_change),
                "controls_change": bool(controls_change),
                "profile_from": current_profile,
                "profile_to": recommended_profile,
                "would_apply": bool(should_apply),
                "cooldown_remaining_s": 0.0,
            },
            "recommendation": recommendation,
            "trend_summary": trend_summary,
            "apply": apply_payload,
        }

    def external_connector_remediation_policy_autotune_scan(
        self,
        *,
        max_pairs: int = 8,
        mission_mode: str = "",
        source: str = "desktop-ui",
        reason: str = "scan",
        force: bool = False,
        dry_run: bool = True,
        limit: int = 320,
        recent_window: int = 16,
        baseline_window: int = 72,
        status: str = "",
    ) -> Dict[str, Any]:
        bounded_pairs = max(1, min(int(max_pairs), 60))
        mission_mode_name = str(mission_mode or "").strip().lower() or "stable"
        pairs: list[tuple[str, str]] = []
        seen: set[str] = set()
        for row in reversed(self.connector_simulation_history):
            if not isinstance(row, dict):
                continue
            action_name = str(row.get("action", "")).strip().lower()
            provider_name = str(row.get("recommended_provider", row.get("requested_provider", "auto")) or "auto").strip().lower() or "auto"
            if not action_name:
                continue
            key = f"{action_name}|{provider_name}"
            if key in seen:
                continue
            seen.add(key)
            pairs.append((action_name, provider_name))
            if len(pairs) >= bounded_pairs:
                break
        if not pairs:
            return {"status": "skip", "message": "No connector simulation pairs are available for autotune scan.", "count": 0, "items": []}
        items: list[Dict[str, Any]] = []
        applied_count = 0
        for action_name, provider_name in pairs:
            payload = self.external_connector_remediation_policy_autotune(
                action=action_name,
                provider=provider_name,
                mission_mode=mission_mode_name,
                source=source,
                reason=f"{reason}:scan",
                force=force,
                dry_run=dry_run,
                limit=limit,
                recent_window=recent_window,
                baseline_window=baseline_window,
                status=status,
            )
            items.append(payload)
            if bool(payload.get("applied", False)):
                applied_count += 1
        return {
            "status": "success",
            "mission_mode": mission_mode_name,
            "count": len(items),
            "applied_count": applied_count,
            "dry_run": bool(dry_run),
            "force": bool(force),
            "limit": int(limit),
            "recent_window": int(recent_window),
            "baseline_window": int(baseline_window),
            "status_filter": str(status or ""),
            "items": items,
        }

    def compare_external_connector_preflight_simulations(self, *, left_id: str, right_id: str) -> Dict[str, Any]:
        left_key = str(left_id or "").strip()
        right_key = str(right_id or "").strip()
        if not left_key or not right_key:
            return {"status": "error", "message": "left_id and right_id are required"}
        index: Dict[str, Dict[str, Any]] = {}
        for row in self.connector_simulation_history:
            if isinstance(row, dict):
                sim_id = str(row.get("simulation_id", "")).strip()
                if sim_id:
                    index[sim_id] = row
        left = index.get(left_key)
        right = index.get(right_key)
        if not left:
            return {"status": "error", "message": f"left simulation '{left_key}' was not found"}
        if not right:
            return {"status": "error", "message": f"right simulation '{right_key}' was not found"}
        left_score = float(left.get("recommendation_confidence", 0.0) or 0.0)
        right_score = float(right.get("recommendation_confidence", 0.0) or 0.0)
        winner = "tie"
        if abs(left_score - right_score) > 0.01:
            winner = "left" if left_score > right_score else "right"
        return {
            "status": "success",
            "left": {
                "simulation_id": str(left.get("simulation_id", "")),
                "recommended_provider": str(left.get("recommended_provider", "")),
                "recommendation_confidence": left_score,
                "ready_count": int(left.get("ready_count", 0) or 0),
                "total_runs": int(left.get("total_runs", 0) or 0),
                "advisor_candidate_count": int(left.get("advisor_candidate_count", 0) or 0),
                "execution_candidate_count": int(left.get("execution_candidate_count", 0) or 0),
                "advisor_context": dict(left.get("advisor_context", {})) if isinstance(left.get("advisor_context"), dict) else {},
            },
            "right": {
                "simulation_id": str(right.get("simulation_id", "")),
                "recommended_provider": str(right.get("recommended_provider", "")),
                "recommendation_confidence": right_score,
                "ready_count": int(right.get("ready_count", 0) or 0),
                "total_runs": int(right.get("total_runs", 0) or 0),
                "advisor_candidate_count": int(right.get("advisor_candidate_count", 0) or 0),
                "execution_candidate_count": int(right.get("execution_candidate_count", 0) or 0),
                "advisor_context": dict(right.get("advisor_context", {})) if isinstance(right.get("advisor_context"), dict) else {},
            },
            "comparison": {
                "winner": winner,
                "score_gap": round(abs(left_score - right_score), 6),
                "provider_changed": str(left.get("recommended_provider", "")) != str(right.get("recommended_provider", "")),
                "advisor_replay": bool(int(left.get("advisor_candidate_count", 0) or 0) or int(right.get("advisor_candidate_count", 0) or 0)),
                "advisor_candidate_delta": int(right.get("advisor_candidate_count", 0) or 0) - int(left.get("advisor_candidate_count", 0) or 0),
                "execution_candidate_delta": int(right.get("execution_candidate_count", 0) or 0) - int(left.get("execution_candidate_count", 0) or 0),
                "summary": "Fake comparison result.",
            },
        }

    def create_external_task(
        self,
        args: Dict[str, Any] | None = None,
        *,
        source: str = "desktop-ui",
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        payload = dict(args or {})
        title = str(payload.get("title", "")).strip()
        if not title:
            return {"status": "error", "message": "title is required"}
        self._trigger_counter += 1
        task_id = f"task-{self._trigger_counter}"
        return {
            "status": "success",
            "provider": str(payload.get("provider", "auto") or "auto"),
            "task_id": task_id,
            "title": title,
            "source": source,
            "metadata": dict(metadata or {}),
        }

    def update_external_task(
        self,
        args: Dict[str, Any] | None = None,
        *,
        source: str = "desktop-ui",
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        payload = dict(args or {})
        task_id = str(payload.get("task_id", "")).strip()
        if not task_id:
            return {"status": "error", "message": "task_id is required"}
        status_text = str(payload.get("status", "")).strip()
        notes = str(payload.get("notes", "")).strip()
        if not any((status_text, notes, str(payload.get("title", "")).strip(), str(payload.get("due", "")).strip())):
            return {
                "status": "error",
                "message": "At least one mutable field is required (title/notes/due/status).",
            }
        return {
            "status": "success",
            "provider": str(payload.get("provider", "auto") or "auto"),
            "task_id": task_id,
            "status_value": status_text,
            "source": source,
            "metadata": dict(metadata or {}),
        }

    def computer_click_target(
        self,
        args: Dict[str, Any] | None = None,
        *,
        source: str = "desktop-ui",
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        payload = dict(args or {})
        query = str(payload.get("query", "")).strip()
        if not query:
            return {"status": "error", "message": "query is required"}
        return {
            "status": "success",
            "query": query,
            "method": "accessibility",
            "x": 640,
            "y": 360,
            "source": source,
            "metadata": dict(metadata or {}),
        }

    def list_telemetry_events(self, event: str | None = None, after_id: int = 0, limit: int = 200) -> Dict[str, Any]:
        rows = list(self.telemetry_events)
        normalized_event = str(event or "").strip().lower()
        normalized_after = max(0, int(after_id))
        bounded_limit = max(1, min(int(limit), 1000))
        if normalized_event:
            rows = [item for item in rows if str(item.get("event", "")).lower() == normalized_event]
        if normalized_after > 0:
            rows = [item for item in rows if int(item.get("event_id", 0)) > normalized_after]
        latest = max((int(item.get("event_id", 0)) for item in self.telemetry_events), default=0)
        rows = rows[-bounded_limit:]
        return {"items": rows, "count": len(rows), "latest_event_id": latest}

    def list_voice_stream_events(
        self,
        *,
        after_id: int = 0,
        limit: int = 200,
        events: list[str] | None = None,
        include_action_events: bool = False,
    ) -> Dict[str, Any]:
        del include_action_events
        rows = list(self.telemetry_events)
        normalized_after = max(0, int(after_id))
        bounded_limit = max(1, min(int(limit), 1000))
        filters = [str(item or "").strip().lower() for item in (events or []) if str(item or "").strip()]
        selected: list[Dict[str, Any]] = []
        for item in rows:
            event_name = str(item.get("event", "")).strip().lower()
            if filters:
                matched = False
                for pattern in filters:
                    if pattern.endswith("*") and event_name.startswith(pattern[:-1]):
                        matched = True
                        break
                    if event_name == pattern:
                        matched = True
                        break
                if not matched:
                    continue
            elif not event_name.startswith("voice."):
                continue
            event_id = int(item.get("event_id", 0) or 0)
            if event_id <= normalized_after:
                continue
            selected.append(item)
        latest = max((int(item.get("event_id", 0)) for item in self.telemetry_events), default=0)
        selected = selected[-bounded_limit:]
        return {"status": "success", "items": selected, "count": len(selected), "latest_event_id": latest}

    def list_approvals(self, status: str | None = None, include_expired: bool = False, limit: int = 200) -> Dict[str, Any]:
        rows = list(self.approvals.values())[:limit]
        return {"items": rows, "count": len(rows)}

    def get_approval(self, approval_id: str) -> Dict[str, Any] | None:
        return self.approvals.get(approval_id)

    def approve_approval(self, approval_id: str, note: str = "") -> Dict[str, Any]:
        record = self.approvals.get(approval_id)
        if not record:
            return {"status": "error", "message": "Approval not found"}
        record["status"] = "approved"
        record["note"] = note
        return {"status": "success", "approval": record}

    def create_schedule(
        self,
        *,
        text: str,
        run_at: str,
        source: str = "desktop-schedule",
        metadata: Dict[str, Any] | None = None,
        max_attempts: int = 3,
        retry_delay_s: int = 60,
        repeat_interval_s: int = 0,
    ) -> Dict[str, Any]:
        self._schedule_counter += 1
        schedule_id = f"schedule-{self._schedule_counter}"
        now_iso = datetime.now(timezone.utc).isoformat()
        record = {
            "schedule_id": schedule_id,
            "text": text,
            "run_at": run_at,
            "next_run_at": run_at,
            "source": source,
            "metadata": metadata or {},
            "max_attempts": max_attempts,
            "retry_delay_s": retry_delay_s,
            "repeat_interval_s": max(0, int(repeat_interval_s)),
            "attempt_count": 0,
            "run_count": 0,
            "last_run_at": "",
            "last_goal_id": "",
            "last_error": "",
            "checkpoint": {},
            "created_at": now_iso,
            "updated_at": now_iso,
            "status": "pending",
        }
        self.schedules[schedule_id] = record
        return record

    def list_schedules(self, status: str | None = None, limit: int = 200) -> Dict[str, Any]:
        rows = list(self.schedules.values())
        if status:
            rows = [item for item in rows if item.get("status") == status]
        return {"items": rows[:limit], "count": min(len(rows), limit)}

    def get_schedule(self, schedule_id: str) -> Dict[str, Any] | None:
        return self.schedules.get(schedule_id)

    def cancel_schedule(self, schedule_id: str) -> Dict[str, Any]:
        record = self.schedules.get(schedule_id)
        if not record:
            return {"status": "error", "message": "Schedule not found"}
        record["status"] = "cancelled"
        return {"status": "success", "schedule": record}

    def pause_schedule(self, schedule_id: str) -> Dict[str, Any]:
        record = self.schedules.get(schedule_id)
        if not record:
            return {"status": "error", "message": "Schedule not found"}
        if record.get("status") == "cancelled":
            return {"status": "error", "message": "Cannot pause cancelled schedule"}
        record["status"] = "paused"
        return {"status": "success", "schedule": record}

    def resume_schedule(self, schedule_id: str) -> Dict[str, Any]:
        record = self.schedules.get(schedule_id)
        if not record:
            return {"status": "error", "message": "Schedule not found"}
        if record.get("status") != "paused":
            return {"status": "error", "message": "Schedule is not paused"}
        record["status"] = "pending"
        return {"status": "success", "schedule": record}

    def run_schedule_now(self, schedule_id: str) -> Dict[str, Any]:
        record = self.schedules.get(schedule_id)
        if not record:
            return {"status": "error", "message": "Schedule not found"}
        if record.get("status") == "cancelled":
            return {"status": "error", "message": "Cancelled schedules cannot run"}
        record["status"] = "pending"
        record["next_run_at"] = datetime.now(timezone.utc).isoformat()
        return {"status": "success", "schedule": record}

    def create_trigger(
        self,
        *,
        text: str,
        interval_s: int,
        start_at: str = "",
        source: str = "desktop-trigger",
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        self._trigger_counter += 1
        trigger_id = f"trigger-{self._trigger_counter}"
        now_iso = datetime.now(timezone.utc).isoformat()
        record = {
            "trigger_id": trigger_id,
            "text": text,
            "source": source,
            "metadata": metadata or {},
            "interval_s": int(interval_s),
            "next_run_at": start_at or now_iso,
            "status": "active",
            "run_count": 0,
            "last_goal_id": "",
            "last_fired_at": "",
            "last_error": "",
            "created_at": now_iso,
            "updated_at": now_iso,
        }
        self.triggers[trigger_id] = record
        return record

    def list_triggers(self, status: str | None = None, limit: int = 200) -> Dict[str, Any]:
        rows = list(self.triggers.values())
        if status:
            rows = [item for item in rows if item.get("status") == status]
        return {"items": rows[:limit], "count": min(len(rows), limit)}

    def get_trigger(self, trigger_id: str) -> Dict[str, Any] | None:
        return self.triggers.get(trigger_id)

    def pause_trigger(self, trigger_id: str) -> Dict[str, Any]:
        record = self.triggers.get(trigger_id)
        if not record:
            return {"status": "error", "message": "Trigger not found"}
        record["status"] = "paused"
        return {"status": "success", "trigger": record}

    def resume_trigger(self, trigger_id: str) -> Dict[str, Any]:
        record = self.triggers.get(trigger_id)
        if not record:
            return {"status": "error", "message": "Trigger not found"}
        record["status"] = "active"
        return {"status": "success", "trigger": record}

    def run_trigger_now(self, trigger_id: str) -> Dict[str, Any]:
        record = self.triggers.get(trigger_id)
        if not record:
            return {"status": "error", "message": "Trigger not found"}
        record["next_run_at"] = datetime.now(timezone.utc).isoformat()
        record["status"] = "active"
        return {"status": "success", "trigger": record}

    def cancel_trigger(self, trigger_id: str) -> Dict[str, Any]:
        record = self.triggers.get(trigger_id)
        if not record:
            return {"status": "error", "message": "Trigger not found"}
        record["status"] = "cancelled"
        return {"status": "success", "trigger": record}

    def list_macros(self, query: str = "", limit: int = 100) -> Dict[str, Any]:
        rows = list(self.macros.values())
        q = str(query or "").strip().lower()
        if q:
            rows = [
                item
                for item in rows
                if q in str(item.get("name", "")).lower()
                or q in str(item.get("text", "")).lower()
                or any(q in str(action).lower() for action in item.get("actions", []))
            ]
        return {"items": rows[:limit], "count": min(len(rows), limit), "query": query}

    def get_macro(self, macro_id: str) -> Dict[str, Any] | None:
        return self.macros.get(macro_id)

    def run_macro(
        self,
        macro_id: str,
        source: str = "desktop-macro",
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        record = self.macros.get(macro_id)
        if not record:
            return {"status": "error", "message": "Macro not found"}
        now_iso = datetime.now(timezone.utc).isoformat()
        record["usage_count"] = int(record.get("usage_count", 0)) + 1
        record["last_used_at"] = now_iso
        record["updated_at"] = now_iso
        metadata_out = dict(metadata) if isinstance(metadata, dict) else {}
        metadata_out["macro_id"] = macro_id
        goal_id = self.submit_goal(record.get("text", ""), source=source, metadata=metadata_out)
        return {"status": "success", "goal_id": goal_id, "macro": record}

    def submit_goal(self, text: str, source: str = "desktop-ui", metadata: Dict[str, Any] | None = None) -> str:
        self._goal_counter += 1
        goal_id = f"goal-{self._goal_counter}"
        status = "running" if "long" in text.lower() else "completed"
        now_iso = datetime.now(timezone.utc).isoformat()
        metadata_out = dict(metadata) if isinstance(metadata, dict) else {}
        mission_id = str(metadata_out.get("__jarvis_mission_id", "")).strip()
        if not mission_id:
            self._mission_counter += 1
            mission_id = f"mission-{self._mission_counter}"
        metadata_out["__jarvis_mission_id"] = mission_id
        self.goal_to_mission[goal_id] = mission_id
        mission = self.missions.get(mission_id)
        if mission is None:
            mission = {
                "mission_id": mission_id,
                "root_goal_id": goal_id,
                "latest_goal_id": goal_id,
                "text": text,
                "source": source,
                "status": "running",
                "resume_count": 0,
                "created_at": now_iso,
                "updated_at": now_iso,
                "metadata": metadata_out,
            }
            self.missions[mission_id] = mission
        else:
            mission["latest_goal_id"] = goal_id
            mission["status"] = "running"
            mission["updated_at"] = now_iso

        self.goals[goal_id] = {
            "goal_id": goal_id,
            "text": text,
            "source": source,
            "metadata": metadata_out,
            "status": status,
            "mission_id": mission_id,
            "created_at": now_iso,
            "started_at": now_iso if status in {"running", "completed"} else "",
            "completed_at": now_iso if status == "completed" else "",
            "failure_reason": "",
            "results": (
                []
                if status == "running"
                else [
                    {
                        "action": "time_now",
                        "status": "success",
                        "output": {"status": "success", "timezone": "UTC", "iso": "2026-01-01T00:00:00+00:00"},
                    }
                ]
            ),
        }
        if status == "completed":
            mission["status"] = "completed"
            mission["updated_at"] = now_iso
        return goal_id

    def list_goals(self, status: str | None = None, limit: int = 100) -> Dict[str, Any]:
        rows = list(self.goals.values())
        normalized = str(status or "").strip().lower()
        if normalized:
            rows = [item for item in rows if str(item.get("status", "")).strip().lower() == normalized]
        rows.sort(
            key=lambda item: (
                str(item.get("created_at", "")),
                str(item.get("goal_id", "")),
            ),
            reverse=True,
        )
        bounded = max(1, min(int(limit), 1000))
        sliced = rows[:bounded]
        return {"items": sliced, "count": len(sliced), "total": len(rows)}

    def get_goal(self, goal_id: str) -> Dict[str, Any] | None:
        return self.goals.get(goal_id)

    def explain_goal(self, goal_id: str, *, include_memory_hints: bool = True) -> Dict[str, Any]:
        goal = self.goals.get(goal_id)
        if not goal:
            return {"status": "error", "message": "Goal not found"}
        mission_id = str(goal.get("mission_id", "")).strip()
        mission = self.missions.get(mission_id, {}) if mission_id else {}
        results = goal.get("results", [])
        failed_action_counts: Dict[str, int] = {}
        if isinstance(results, list):
            for row in results:
                if not isinstance(row, dict):
                    continue
                status_text = str(row.get("status", "")).strip().lower()
                action = str(row.get("action", "")).strip()
                if status_text in {"failed", "blocked"} and action:
                    failed_action_counts[action] = int(failed_action_counts.get(action, 0)) + 1
        return {
            "status": "success",
            "goal_id": goal_id,
            "goal": {
                "status": str(goal.get("status", "")),
                "source": str(goal.get("source", "")),
                "text": str(goal.get("text", "")),
                "created_at": str(goal.get("created_at", "")),
                "started_at": str(goal.get("started_at", "")),
                "completed_at": str(goal.get("completed_at", "")),
                "failure_reason": str(goal.get("failure_reason", "")),
                "result_count": len(results) if isinstance(results, list) else 0,
            },
            "plan": {
                "plan_id": "plan-fake",
                "intent": "fake-intent",
                "step_count": len(results) if isinstance(results, list) else 0,
                "planner_mode": "deterministic",
                "planner_provider": "none",
                "policy_profile": "interactive",
                "recovery_profile": self.recovery_default_profile,
            },
            "results": {
                "status_counts": {"success": 1},
                "action_counts": {"time_now": 1},
                "failed_action_counts": failed_action_counts,
                "recent_failures": [],
            },
            "mission": {
                "mission_id": mission_id,
                "record": mission,
                "diagnostics": {"status": "success"},
                "resume_preview": {"status": "success"},
            },
            "rollback": {"status": "success", "items": [], "count": 0, "total": 0},
            "memory_hints": {"runtime": [], "episodic": [], "strategy": {}} if include_memory_hints else {},
            "recommendations": ["Inspect mission diagnostics before rerun."],
        }

    def autonomy_report(self, *, limit_recent_goals: int = 250) -> Dict[str, Any]:
        del limit_recent_goals
        return {
            "status": "success",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "window_recent_goals": len(self.goals),
            "goal_status_counts": {"completed": 1},
            "pressures": {
                "failure_pressure": 0.2,
                "open_breaker_pressure": 0.0,
                "approval_pressure": 0.0,
            },
            "scores": {"reliability": 83.5, "autonomy": 78.2, "tier": "medium"},
            "automation": {
                "pending_approvals": len(self.approvals),
                "pending_schedules": sum(1 for item in self.schedules.values() if item.get("status") == "pending"),
                "active_triggers": sum(1 for item in self.triggers.values() if item.get("status") == "active"),
                "running_missions": sum(1 for item in self.missions.values() if item.get("status") == "running"),
                "pending_auto_resumes": 0,
            },
            "memory": {"runtime_hint_count": 1, "episodic": {"count": 1}},
            "integrations": {"oauth_token_count": len(self.oauth_tokens), "oauth_maintenance": dict(self.oauth_maintenance)},
            "circuit_breakers": {"open_count": 0, "total_count": 1, "open_actions": []},
            "recovery": {
                "current_profile": self.recovery_default_profile,
                "recommended_profile": "balanced",
                "profiles": self.list_recovery_profiles(),
            },
            "action_hotspots": [],
            "recommendations": ["Backend autonomy is stable."],
            "last_tune": {"status": "idle", "last_run_at": "", "changed": False, "target_profile": "", "reason": ""},
        }

    def autonomy_tune(self, *, dry_run: bool = False, reason: str = "manual") -> Dict[str, Any]:
        report = self.autonomy_report(limit_recent_goals=250)
        target = str(report.get("recovery", {}).get("recommended_profile", "balanced"))
        changed = False
        message = "No recovery profile change required."
        current = self.recovery_default_profile
        if target and target != current:
            message = f"Would switch recovery profile from {current} to {target}." if dry_run else "Recovery profile updated."
            if not dry_run:
                self.recovery_default_profile = target
                changed = True
        return {
            "status": "success",
            "last_run_at": datetime.now(timezone.utc).isoformat(),
            "changed": changed,
            "target_profile": target,
            "reason": reason,
            "dry_run": dry_run,
            "message": message,
            "current_profile": current,
            "report": report,
        }

    def list_missions(self, *, status: str = "", limit: int = 100) -> Dict[str, Any]:
        rows = list(self.missions.values())
        normalized = str(status or "").strip().lower()
        if normalized:
            rows = [item for item in rows if str(item.get("status", "")).lower() == normalized]
        rows.sort(key=lambda item: str(item.get("updated_at", "")), reverse=True)
        sliced = rows[: max(1, min(int(limit), 2000))]
        return {"status": "success", "items": sliced, "count": len(sliced), "total": len(rows)}

    def get_mission(self, mission_id: str) -> Dict[str, Any] | None:
        return self.missions.get(mission_id)

    def mission_timeline(
        self,
        mission_id: str,
        *,
        limit: int = 200,
        event: str = "",
        step_id: str = "",
        status: str = "",
        descending: bool = True,
    ) -> Dict[str, Any]:
        mission = self.missions.get(mission_id)
        if mission is None:
            return {"status": "error", "message": "mission not found"}

        rows = [
            {
                "sequence": 2,
                "event": "finished",
                "step_id": "step-2",
                "action": "time_now",
                "status": "success",
                "attempt": 1,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "goal_id": str(mission.get("latest_goal_id", "")),
                "plan_id": "plan-fake",
                "args_fingerprint": "abc",
                "error": "",
                "duration_ms": 32,
                "evidence": {"step_id": "step-2"},
            },
            {
                "sequence": 1,
                "event": "started",
                "step_id": "step-1",
                "action": "time_now",
                "status": "running",
                "attempt": 1,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "goal_id": str(mission.get("latest_goal_id", "")),
                "plan_id": "plan-fake",
                "args_fingerprint": "def",
                "error": "",
                "duration_ms": 0,
                "evidence": {"step_id": "step-1"},
            },
        ]
        if event:
            rows = [item for item in rows if str(item.get("event", "")).lower() == str(event).lower()]
        if step_id:
            rows = [item for item in rows if str(item.get("step_id", "")) == str(step_id)]
        if status:
            rows = [item for item in rows if str(item.get("status", "")).lower() == str(status).lower()]
        rows.sort(key=lambda item: int(item.get("sequence", 0)), reverse=bool(descending))
        bounded = max(1, min(int(limit), 5000))
        items = rows[:bounded]
        return {
            "status": "success",
            "mission_id": mission_id,
            "items": items,
            "count": len(items),
            "total": len(rows),
            "active_step_id": "step-1",
            "active_goal_id": str(mission.get("latest_goal_id", "")),
            "active_plan_id": "plan-fake",
            "checkpoint_sequence": 2,
        }

    def mission_resume_preview(self, mission_id: str) -> Dict[str, Any]:
        mission = self.missions.get(mission_id)
        if mission is None:
            return {"status": "error", "message": "mission not found"}
        return {
            "status": "success",
            "mission": mission,
            "resume_plan": {
                "plan_id": "plan-fake-resume",
                "goal_id": str(mission.get("latest_goal_id", "")),
                "intent": "resume",
                "steps": [
                    {
                        "step_id": "step-2",
                        "action": "time_now",
                        "args": {"timezone": "UTC"},
                        "depends_on": [],
                        "verify": {"expect_status": "success"},
                    }
                ],
                "context": {"resume_mode": True, "resume_cursor_step_id": "step-2"},
            },
            "completed_step_ids": ["step-1"],
            "remaining_steps": 1,
            "resume_cursor": {
                "step_id": "step-2",
                "index": 1,
                "status": "running",
                "checkpoint_sequence": 2,
            },
            "step_status": {"step-1": "success", "step-2": "running"},
        }

    def mission_diagnostics(self, mission_id: str, *, hotspot_limit: int = 8) -> Dict[str, Any]:
        mission = self.missions.get(mission_id)
        if mission is None:
            return {"status": "error", "message": "mission not found"}
        bounded = max(1, min(int(hotspot_limit), 50))
        return {
            "status": "success",
            "mission_id": mission_id,
            "mission_status": str(mission.get("status", "running")),
            "plan": {
                "plan_id": "plan-fake",
                "step_count": 2,
                "dependency_edges": 1,
                "unresolved_dependency_edges": 1,
                "missing_dependency_edges": 0,
            },
            "execution": {
                "checkpoint_count": 2,
                "last_checkpoint_at": datetime.now(timezone.utc).isoformat(),
                "active_step_id": "step-2",
                "last_error": "",
            },
            "step_counts": {
                "pending": 0,
                "running": 1,
                "success": 1,
                "failed": 0,
                "blocked": 0,
                "skipped": 0,
            },
            "dependency_issues": [
                {
                    "step_id": "step-2",
                    "status": "running",
                    "depends_on": ["step-1"],
                    "missing_dependencies": [],
                    "unresolved_dependencies": [],
                }
            ][:bounded],
            "hotspots": {
                "retry": [{"step_id": "step-2", "attempts": 2, "status": "running", "action": "time_now"}][:bounded],
                "slow": [{"step_id": "step-2", "samples": 1, "avg_duration_ms": 1800, "max_duration_ms": 1800}][:bounded],
                "failures": [],
            },
            "resume": {"ready": True, "remaining_steps": 1, "error": ""},
            "risk": {
                "score": 0.48,
                "level": "medium",
                "reasons": ["1 step(s) have unresolved dependencies."],
            },
            "recommendations": ["Tune retries/timeouts for hotspot steps and add stronger precondition checks."],
        }

    def queue_diagnostics(
        self,
        *,
        limit: int = 200,
        include_terminal: bool = False,
        status: str = "",
        source: str = "",
        mission_id: str = "",
    ) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 5000))
        normalized_status = str(status or "").strip().lower()
        normalized_source = str(source or "").strip().lower()
        normalized_mission = str(mission_id or "").strip()

        terminal_statuses = {"completed", "failed", "blocked", "cancelled"}
        queued_rows: list[Dict[str, Any]] = []
        terminal_rows: list[Dict[str, Any]] = []
        for goal in self.goals.values():
            goal_status = str(goal.get("status", "")).strip().lower()
            goal_source = str(goal.get("source", "")).strip()
            goal_source_lower = goal_source.lower()
            goal_mission_id = str(goal.get("mission_id", "")).strip() or str(
                self.goal_to_mission.get(str(goal.get("goal_id", "")), "")
            ).strip()
            if normalized_status and goal_status != normalized_status:
                continue
            if normalized_source and goal_source_lower != normalized_source:
                continue
            if normalized_mission and goal_mission_id != normalized_mission:
                continue
            metadata = goal.get("metadata") if isinstance(goal.get("metadata"), dict) else {}
            queue_priority = int(metadata.get("queue_priority", 0) or 0)
            row = {
                "goal_id": str(goal.get("goal_id", "")),
                "mission_id": goal_mission_id,
                "status": goal_status,
                "source": goal_source,
                "text": str(goal.get("text", "")),
                "queue_index": -1,
                "created_at": str(goal.get("created_at", "")),
                "queue_enqueued_at": str(metadata.get("queue_enqueued_at", "") or ""),
                "started_at": str(goal.get("started_at", "")),
                "completed_at": str(goal.get("completed_at", "")),
                "waited_s": 0.0,
                "base_priority": queue_priority,
                "effective_priority": queue_priority,
                "starvation_windows": 0,
                "queue_priority": queue_priority,
                "queue_priority_reason": str(metadata.get("queue_priority_reason", "") or ""),
                "queue_promoted_at": str(metadata.get("queue_promoted_at", "") or ""),
                "queue_promoted_reason": str(metadata.get("queue_promoted_reason", "") or ""),
                "queue_deadline_enforced": False,
                "queue_deadline_at": "",
                "queue_deadline_remaining_s": None,
                "queue_deadline_reason": "",
                "failure_reason": str(goal.get("failure_reason", "")),
            }
            if goal_status in terminal_statuses:
                terminal_rows.append(row)
            else:
                queued_rows.append(row)

        queued_rows.sort(
            key=lambda row: (
                int(row.get("effective_priority", 0)),
                str(row.get("created_at", "")),
                str(row.get("goal_id", "")),
            )
        )
        for index, row in enumerate(queued_rows):
            row["queue_index"] = index
        rows = list(queued_rows)
        if include_terminal:
            terminal_rows.sort(key=lambda row: (str(row.get("completed_at", "")), str(row.get("goal_id", ""))), reverse=True)
            rows.extend(terminal_rows)
        selected = rows[:bounded]

        status_counts: Dict[str, int] = {}
        source_counts: Dict[str, int] = {}
        for row in selected:
            status_key = str(row.get("status", "unknown")).strip().lower() or "unknown"
            source_key = str(row.get("source", "unknown")).strip().lower() or "unknown"
            status_counts[status_key] = int(status_counts.get(status_key, 0)) + 1
            source_counts[source_key] = int(source_counts.get(source_key, 0)) + 1

        return {
            "status": "success",
            "items": selected,
            "count": len(selected),
            "total": len(rows),
            "queue_length": len(queued_rows),
            "orphaned_pending_count": 0,
            "filters": {
                "status": normalized_status,
                "source": normalized_source,
                "mission_id": normalized_mission,
                "include_terminal": bool(include_terminal),
            },
            "summary": {
                "status_counts": status_counts,
                "source_counts": source_counts,
            },
            "policy": {
                "priority_enabled": True,
                "starvation_window_s": 45.0,
                "default_source_priority": 0,
                "queue_deadline_enforced": False,
                "default_max_queue_wait_s": 0.0,
            },
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    def prioritize_goal(
        self,
        goal_id: str,
        *,
        priority: int = -3,
        reason: str = "manual",
    ) -> Dict[str, Any]:
        clean_goal_id = str(goal_id or "").strip()
        if not clean_goal_id:
            return {"status": "error", "message": "goal id is required"}
        goal = self.goals.get(clean_goal_id)
        if goal is None:
            return {"status": "error", "message": "Goal not found", "goal_id": clean_goal_id}
        goal_status = str(goal.get("status", "")).strip().lower()
        if goal_status in {"completed", "failed", "blocked", "cancelled"}:
            return {
                "status": "error",
                "message": f"Goal cannot be prioritized from status '{goal_status}'.",
                "goal_id": clean_goal_id,
                "goal_status": goal_status,
            }
        metadata = goal.get("metadata") if isinstance(goal.get("metadata"), dict) else {}
        bounded_priority = max(-20, min(int(priority), 20))
        metadata["queue_priority"] = bounded_priority
        metadata["queue_priority_reason"] = str(reason or "manual").strip() or "manual"
        metadata["queue_promoted_at"] = datetime.now(timezone.utc).isoformat()
        metadata["queue_promoted_reason"] = "manual"
        goal["metadata"] = metadata
        mission_id = str(goal.get("mission_id", "")).strip()
        return {
            "status": "success",
            "goal_id": clean_goal_id,
            "mission_id": mission_id,
            "priority": bounded_priority,
            "reason": str(reason or "manual").strip() or "manual",
            "goal_status": goal_status,
        }

    def prioritize_mission(
        self,
        mission_id: str,
        *,
        priority: int = -4,
        reason: str = "manual",
        demote_others: bool = False,
    ) -> Dict[str, Any]:
        clean_mission_id = str(mission_id or "").strip()
        if not clean_mission_id:
            return {"status": "error", "message": "mission id is required"}
        mission = self.missions.get(clean_mission_id)
        if not mission:
            return {"status": "error", "message": "Mission not found", "mission_id": clean_mission_id}
        bounded_priority = max(-20, min(int(priority), 20))
        clean_reason = str(reason or "manual").strip() or "manual"
        rows = [
            goal
            for goal in self.goals.values()
            if str(goal.get("mission_id", "")).strip() == clean_mission_id
            and str(goal.get("status", "")).strip().lower() not in {"completed", "failed", "blocked", "cancelled"}
        ]
        if not rows:
            return {
                "status": "error",
                "message": "Mission has no queued goals to prioritize.",
                "mission_id": clean_mission_id,
            }

        promoted_goal_ids: list[str] = []
        for row in rows:
            goal_id = str(row.get("goal_id", "")).strip()
            if not goal_id:
                continue
            promoted = self.prioritize_goal(
                goal_id,
                priority=bounded_priority,
                reason=f"{clean_reason}:mission_priority",
            )
            if promoted.get("status") == "success":
                promoted_goal_ids.append(goal_id)

        demoted_goal_ids: list[str] = []
        if bool(demote_others):
            demote_priority = max(-20, min(bounded_priority + 3, 20))
            for row in self.goals.values():
                goal_id = str(row.get("goal_id", "")).strip()
                row_mission_id = str(row.get("mission_id", "")).strip()
                row_status = str(row.get("status", "")).strip().lower()
                if not goal_id or row_mission_id == clean_mission_id:
                    continue
                if row_status in {"completed", "failed", "blocked", "cancelled"}:
                    continue
                metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
                metadata["queue_priority"] = demote_priority
                metadata["queue_priority_reason"] = f"{clean_reason}:demote_for_mission:{clean_mission_id}"
                metadata["queue_promoted_at"] = datetime.now(timezone.utc).isoformat()
                metadata["queue_promoted_reason"] = "mission_demote"
                row["metadata"] = metadata
                demoted_goal_ids.append(goal_id)

        promoted_goal_ids = list(dict.fromkeys(promoted_goal_ids))
        demoted_goal_ids = list(dict.fromkeys(demoted_goal_ids))
        return {
            "status": "success",
            "mission_id": clean_mission_id,
            "priority": bounded_priority,
            "reason": clean_reason,
            "demote_others": bool(demote_others),
            "promoted_goal_ids": promoted_goal_ids,
            "promoted_count": len(promoted_goal_ids),
            "demoted_goal_ids": demoted_goal_ids,
            "demoted_count": len(demoted_goal_ids),
            "mission_status": str(mission.get("status", "")).strip().lower(),
        }

    def resume_mission(
        self,
        mission_id: str,
        *,
        source: str = "desktop-mission",
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        mission = self.missions.get(mission_id)
        if not mission:
            return {"status": "error", "message": "mission not found"}
        merged_metadata = dict(metadata) if isinstance(metadata, dict) else {}
        merged_metadata["__jarvis_mission_id"] = mission_id
        goal_id = self.submit_goal(str(mission.get("text", "")), source=source, metadata=merged_metadata)
        mission["latest_goal_id"] = goal_id
        mission["resume_count"] = int(mission.get("resume_count", 0)) + 1
        mission["status"] = "running"
        mission["updated_at"] = datetime.now(timezone.utc).isoformat()
        return {"status": "success", "mission_id": mission_id, "goal_id": goal_id, "remaining_steps": 1}

    def cancel_mission(self, mission_id: str, *, reason: str = "Cancelled by user request.") -> Dict[str, Any]:
        mission = self.missions.get(mission_id)
        if mission is None:
            return {"status": "error", "message": "mission not found"}
        status = str(mission.get("status", "running")).strip().lower()
        if status in {"completed", "cancelled"}:
            return {"status": "error", "message": f"Mission is already {status}.", "mission_id": mission_id}

        goal_id = str(mission.get("active_goal_id", "")).strip() or str(mission.get("latest_goal_id", "")).strip()
        mission["status"] = "cancelled"
        mission["last_error"] = reason
        mission["updated_at"] = datetime.now(timezone.utc).isoformat()
        if goal_id and goal_id in self.goals:
            self.goals[goal_id]["status"] = "cancelled"
            self.goals[goal_id]["failure_reason"] = reason
            self.goals[goal_id]["completed_at"] = datetime.now(timezone.utc).isoformat()
        return {
            "status": "success",
            "message": "Mission cancellation requested.",
            "mission_id": mission_id,
            "goal_id": goal_id,
            "mission": mission,
        }

    def list_rollbacks(self, *, status: str = "", goal_id: str = "", limit: int = 200) -> Dict[str, Any]:
        rows = list(self.rollbacks.values())
        normalized = str(status or "").strip().lower()
        if normalized:
            rows = [item for item in rows if str(item.get("status", "")).lower() == normalized]
        if goal_id:
            rows = [item for item in rows if str(item.get("goal_id", "")) == goal_id]
        rows.sort(key=lambda item: str(item.get("created_at", "")), reverse=True)
        sliced = rows[: max(1, min(int(limit), 2000))]
        return {"status": "success", "items": sliced, "count": len(sliced), "total": len(rows)}

    def get_rollback(self, rollback_id: str) -> Dict[str, Any] | None:
        return self.rollbacks.get(rollback_id)

    def run_rollback(self, rollback_id: str, *, dry_run: bool = False) -> Dict[str, Any]:
        row = self.rollbacks.get(rollback_id)
        if not row:
            return {"status": "error", "message": "rollback entry not found"}
        if not dry_run:
            row["status"] = "rolled_back"
            row["updated_at"] = datetime.now(timezone.utc).isoformat()
        return {"status": "success", "rollback": row, "dry_run": dry_run}

    def run_goal_rollback(self, goal_id: str, *, dry_run: bool = False) -> Dict[str, Any]:
        rows = [item for item in self.rollbacks.values() if str(item.get("goal_id", "")) == str(goal_id)]
        if not rows:
            return {"status": "error", "message": "no rollback entries for this goal"}
        if not dry_run:
            now_iso = datetime.now(timezone.utc).isoformat()
            for row in rows:
                row["status"] = "rolled_back"
                row["updated_at"] = now_iso
        return {"status": "success", "goal_id": goal_id, "rolled_back": len(rows), "failed": 0, "dry_run": dry_run}

    def wait_for_goal(self, goal_id: str, timeout_s: float = 10.0, poll_s: float = 0.2) -> Dict[str, Any] | None:
        goal = self.goals.get(goal_id)
        if not goal:
            return None
        if goal.get("status") == "running":
            now_iso = datetime.now(timezone.utc).isoformat()
            goal["status"] = "completed"
            goal["completed_at"] = now_iso
            goal["results"] = [
                {
                    "action": "time_now",
                    "status": "success",
                    "output": {"status": "success", "timezone": "UTC", "iso": "2026-01-01T00:00:00+00:00"},
                }
            ]
            mission_id = str(goal.get("mission_id", ""))
            if mission_id and mission_id in self.missions:
                self.missions[mission_id]["status"] = "completed"
                self.missions[mission_id]["updated_at"] = now_iso
        return goal

    def cancel_goal(self, goal_id: str, reason: str = "Cancelled by user request.") -> Dict[str, Any]:
        record = self.goals.get(goal_id)
        if not record:
            return {"status": "error", "message": "Goal not found"}
        status = str(record.get("status", ""))
        if status in {"completed", "failed", "blocked", "cancelled"}:
            return {"status": "error", "message": f"Goal already {status}", "goal": record}
        record["status"] = "cancelled"
        record["failure_reason"] = reason
        mission_id = str(record.get("mission_id", ""))
        if mission_id and mission_id in self.missions:
            self.missions[mission_id]["status"] = "cancelled"
            self.missions[mission_id]["updated_at"] = datetime.now(timezone.utc).isoformat()
        return {"status": "success", "message": "Cancellation requested", "goal": record}

    def preview_plan(self, text: str, source: str = "desktop-ui", metadata: Dict[str, Any] | None = None) -> Dict[str, Any]:
        return {
            "status": "success",
            "plan": {
                "plan_id": "preview-plan-1",
                "goal_id": "preview-goal-1",
                "intent": "time_query",
                "created_at": "2026-02-23T00:00:00+00:00",
                "planner_mode": "deterministic",
                "planner_provider": "",
                "planner_model": "",
                "step_count": 1,
                "risk": "low",
                "steps": [
                    {
                        "index": 1,
                        "step_id": "step-1",
                        "action": "time_now",
                        "args": {"timezone": "UTC"},
                        "risk": "low",
                        "requires_confirmation": False,
                        "description": "Get current time in a specified timezone.",
                        "verify": {"expect_status": "success"},
                    }
                ],
            },
        }

    def execute_action(
        self,
        action: str,
        args: Dict[str, Any] | None = None,
        source: str = "desktop-ui",
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        metadata = metadata or {}
        args = args or {}
        if action == "copy_file" and not metadata.get("approval_id"):
            self._approval_counter += 1
            approval_id = f"approval-{self._approval_counter}"
            record = {
                "approval_id": approval_id,
                "status": "pending",
                "action": "copy_file",
                "args_preview": args,
            }
            self.approvals[approval_id] = record
            return {
                "action": action,
                "status": "blocked",
                "error": "approval required",
                "output": {"status": "error", "approval_required": True, "approval": record},
            }
        return {
            "action": action,
            "status": "success",
            "output": {"status": "success", "echo_args": args, "approval_id": metadata.get("approval_id")},
        }

    def chat(
        self,
        message: str,
        history: list | None = None,
        photo_data_uri: str | None = None,
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        return {"reply": f"echo:{message}", "history_used": len(history or [])}

    def speak(
        self,
        text: str,
        *,
        provider: str = "",
        voice: str = "",
        rate: int | None = None,
        volume: float | None = None,
        allow_text_fallback: bool | None = None,
    ) -> Dict[str, Any]:
        return {
            "status": "success",
            "text": text,
            "mode": "local-pyttsx3",
            "requested_provider": provider or "auto",
            "provider": provider or "auto",
            "voice": voice,
            "rate": rate,
            "volume": volume,
            "allow_text_fallback": allow_text_fallback,
            "attempts": [{"provider": "local", "status": "success", "latency_s": 0.02}],
        }

    def stop_speaking(
        self,
        *,
        source: str = "desktop-ui",
        metadata: Dict[str, Any] | None = None,
        session_id: str = "",
        provider: str = "",
    ) -> Dict[str, Any]:
        del source, metadata
        return {
            "status": "success",
            "stopped": True,
            "session_id": str(session_id),
            "provider": str(provider),
            "results": [{"status": "success", "stopped": True}],
        }

    def transcribe(
        self,
        *,
        duration_s: float = 4.0,
        submit_goal: bool = False,
        speak_reply: bool = False,
        source: str = "desktop-stt",
        metadata: Dict[str, Any] | None = None,
        stt_mode: str = "auto",
        vad_frame_s: float = 0.2,
        vad_energy_threshold: float = 0.015,
        vad_silence_s: float = 0.9,
        vad_min_speech_s: float = 0.35,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "status": "success",
            "duration_s": float(duration_s),
            "stt_mode": str(stt_mode),
            "text": "what time is it in UTC",
            "transcription": {"status": "success", "text": "what time is it in UTC", "source": "fake-stt", "model": "fake-whisper"},
            "capture": {
                "vad_frame_s": float(vad_frame_s),
                "vad_energy_threshold": float(vad_energy_threshold),
                "vad_silence_s": float(vad_silence_s),
                "vad_min_speech_s": float(vad_min_speech_s),
            },
        }
        if submit_goal:
            goal_id = self.submit_goal("what time is it in UTC", source=source, metadata=metadata)
            payload["goal_id"] = goal_id
            payload["goal"] = self.wait_for_goal(goal_id)
            payload["reply"] = "Completed: what time is it in UTC"
        if speak_reply:
            payload["speak"] = {"status": "success", "text": payload.get("reply", "Completed")}
        return payload

    def get_voice_session_status(self) -> Dict[str, Any]:
        payload = dict(self.voice_state)
        payload["route_policy"] = dict(self.voice_route_policy_state.get("stt", {}))
        payload["wakeword_route_policy"] = dict(self.voice_route_policy_state.get("wakeword", {}))
        payload["tts_route_policy"] = dict(self.voice_route_policy_state.get("tts", {}))
        payload["route_policy_summary"] = dict(self.voice_route_policy_state.get("summary", {}))
        payload["route_policy_timeline"] = list(self.voice_route_policy_timeline_state.get("items", []))
        payload["wakeword_supervision"] = dict(self.wakeword_supervision_history_state.get("current", {}))
        payload["wakeword_supervision_timeline"] = list(self.wakeword_supervision_history_state.get("items", []))
        return payload

    def voice_route_policy_timeline(self, *, limit: int = 80, force_refresh: bool = False) -> Dict[str, Any]:
        del force_refresh
        payload = dict(self.voice_route_policy_timeline_state)
        payload["limit"] = max(1, min(int(limit), 180))
        payload["history_limit"] = payload["limit"]
        payload["count"] = min(len(payload.get("items", [])), payload["limit"])
        payload["items"] = list(payload.get("items", []))[: payload["limit"]]
        payload["current"] = dict(self.voice_route_policy_state)
        return payload

    def voice_route_policy_history(
        self,
        *,
        limit: int = 160,
        task: str = "",
        status: str = "",
        refresh: bool = False,
    ) -> Dict[str, Any]:
        del refresh
        payload = dict(self.voice_route_policy_history_state)
        payload["limit"] = max(1, min(int(limit), 2000))
        clean_task = str(task or "").strip().lower()
        clean_status = str(status or "").strip().lower()
        rows = list(payload.get("items", []))
        if clean_task:
            rows = [row for row in rows if str(row.get("task", "")).strip().lower() == clean_task]
        if clean_status:
            rows = [row for row in rows if str(row.get("status", "")).strip().lower() == clean_status]
        payload["total"] = len(rows)
        payload["count"] = min(len(rows), payload["limit"])
        payload["items"] = rows[: payload["limit"]]
        payload["task"] = clean_task
        payload["status_filter"] = clean_status
        payload["current"] = dict(self.voice_route_policy_state)
        return payload

    def voice_mission_reliability_status(self, *, mission_id: str = "", limit: int = 24) -> Dict[str, Any]:
        payload = dict(self.voice_mission_reliability_state)
        clean_mission = str(mission_id or payload.get("mission_id", "")).strip()
        current = dict(payload.get("current", {})) if isinstance(payload.get("current", {}), dict) else {}
        if clean_mission:
            current["mission_id"] = clean_mission
            payload["mission_id"] = clean_mission
        payload["current"] = current
        payload["items"] = [current] if current else []
        payload["count"] = min(len(payload["items"]), max(1, int(limit)))
        payload["total"] = len(payload["items"])
        return payload

    def wakeword_supervision_history(
        self,
        *,
        limit: int = 160,
        status: str = "",
        refresh: bool = False,
    ) -> Dict[str, Any]:
        del refresh
        payload = dict(self.wakeword_supervision_history_state)
        payload["limit"] = max(1, min(int(limit), 2000))
        clean_status = str(status or "").strip().lower()
        rows = list(payload.get("items", []))
        if clean_status:
            rows = [row for row in rows if str(row.get("status", "")).strip().lower() == clean_status]
        payload["total"] = len(rows)
        payload["count"] = min(len(rows), payload["limit"])
        payload["items"] = rows[: payload["limit"]]
        payload["status_filter"] = clean_status
        payload["current"] = dict(self.wakeword_supervision_history_state.get("current", {}))
        return payload

    def wakeword_restart_history(
        self,
        *,
        limit: int = 160,
        event_type: str = "",
        refresh: bool = False,
    ) -> Dict[str, Any]:
        del refresh
        payload = dict(self.wakeword_restart_history_state)
        payload["limit"] = max(1, min(int(limit), 2000))
        clean_event_type = str(event_type or "").strip().lower()
        rows = list(payload.get("items", []))
        if clean_event_type:
            rows = [row for row in rows if str(row.get("event_type", "")).strip().lower() == clean_event_type]
        payload["total"] = len(rows)
        payload["count"] = min(len(rows), payload["limit"])
        payload["items"] = rows[: payload["limit"]]
        payload["event_type_filter"] = clean_event_type
        payload["current"] = dict(self.wakeword_restart_history_state.get("current", {}))
        return payload

    def wakeword_restart_policy_history(
        self,
        *,
        limit: int = 160,
        refresh: bool = False,
    ) -> Dict[str, Any]:
        del refresh
        payload = dict(self.wakeword_restart_policy_history_state)
        payload["limit"] = max(1, min(int(limit), 2000))
        rows = list(payload.get("items", []))
        payload["total"] = len(rows)
        payload["count"] = min(len(rows), payload["limit"])
        payload["items"] = rows[: payload["limit"]]
        payload["current"] = dict(self.wakeword_restart_policy_history_state.get("current", {}))
        return payload

    def get_tts_diagnostics(
        self,
        *,
        history_limit: int = 24,
        source: str = "desktop-ui",
        mission_id: str = "",
        risk_level: str = "",
        policy_profile: str = "",
        requires_offline: bool = False,
        privacy_mode: bool = False,
    ) -> Dict[str, Any]:
        bounded = max(1, min(int(history_limit), 200))
        route_bundle = self.model_route_bundle(
            stack_name="voice",
            tasks=["tts"],
            requires_offline=requires_offline,
            privacy_mode=privacy_mode,
            mission_profile=policy_profile or "balanced",
        )
        route_item = {}
        for row in route_bundle.get("items", []):
            if isinstance(row, dict) and str(row.get("task", "")).strip().lower() == "tts":
                route_item = dict(row)
                break
        route_policy = route_item.get("route_policy", {}) if isinstance(route_item.get("route_policy", {}), dict) else {}
        route_policy_summary = route_bundle.get("launch_policy_summary", {}) if isinstance(route_bundle.get("launch_policy_summary", {}), dict) else {}
        policy = self.get_tts_policy(
            limit=bounded,
            source=source,
            mission_id=mission_id,
            risk_level=risk_level,
            policy_profile=policy_profile,
            requires_offline=requires_offline,
            privacy_mode=privacy_mode,
        )
        recommended = str(policy.get("recommended_provider", self.tts_state.get("recommended_provider", "local"))).strip().lower() or "local"
        route_recommended = str(route_policy.get("recommended_provider", "")).strip().lower()
        if route_recommended in {"local", "elevenlabs"}:
            recommended = route_recommended
        if (requires_offline or privacy_mode) and str(route_item.get("provider", "")).strip().lower() in {"local", "elevenlabs"}:
            recommended = str(route_item.get("provider", "")).strip().lower()
        remediation_hints: list[Dict[str, Any]] = []
        if bool(route_item.get("route_adjusted", False)):
            remediation_hints.append(
                {
                    "code": "tts_route_policy_rerouted",
                    "severity": "low",
                    "message": str(route_policy.get("reason", "") or "TTS route rerouted by local launcher policy.").strip(),
                    "recommended_provider": route_recommended or recommended,
                }
            )
        if bool(route_item.get("route_blocked", False)):
            remediation_hints.append(
                {
                    "code": "tts_route_policy_blocked",
                    "severity": "medium",
                    "message": str(route_policy.get("reason", "") or "Local TTS route blocked by launcher policy.").strip(),
                    "recommended_provider": route_recommended or recommended,
                }
            )
        return {
            "status": "success",
            "history_limit": bounded,
            "providers": {
                "local": {
                    **dict(self.tts_state.get("local", {})),
                    "selected_model_path": str(route_item.get("selected_path", "")).strip(),
                    "selected_model": str(route_item.get("model", "")).strip(),
                    "neural_runtime": {
                        **dict(self.tts_state.get("local", {}).get("neural_runtime", {})),
                        "bridge": dict(self.tts_bridge_state),
                    },
                },
                "elevenlabs": dict(self.tts_state.get("elevenlabs", {})),
            },
            "recommended_provider": recommended,
            "remediation_hints": remediation_hints,
            "policy": policy,
            "model_route": {
                "status": "success",
                "selected_provider": str(route_item.get("provider", "")).strip().lower(),
                "selected_model": str(route_item.get("model", "")).strip(),
                "selected_local_path": str(route_item.get("selected_path", "")).strip(),
                "route_item": route_item,
                "route_policy": dict(route_policy),
                "route_adjusted": bool(route_item.get("route_adjusted", False)),
                "route_blocked": bool(route_item.get("route_blocked", False)),
                "route_warning": str(route_item.get("route_warning", "")),
            },
            "route_bundle": route_bundle,
            "route_policy_summary": route_policy_summary,
            "provider_credentials": {
                "status": "success",
                "providers": {
                    "groq": {"ready": True, "present": True},
                    "nvidia": {"ready": True, "present": True},
                    "elevenlabs": {"ready": True, "present": True},
                },
            },
        }

    def get_tts_policy(
        self,
        *,
        limit: int = 120,
        source: str = "desktop-ui",
        mission_id: str = "",
        risk_level: str = "",
        policy_profile: str = "",
        requires_offline: bool = False,
        privacy_mode: bool = False,
    ) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 2000))
        recommended = str(self.tts_policy_state.get("recommended_provider", "elevenlabs")).strip().lower() or "local"
        route_bias = self.tts_policy_state.get("route_bias", {})
        if requires_offline or privacy_mode or risk_level.strip().lower() == "high" or policy_profile.strip().lower() == "privacy":
            recommended = "local"
        return {
            "status": "success",
            "enabled": bool(self.tts_policy_state.get("enabled", True)),
            "learning_enabled": bool(self.tts_policy_state.get("learning_enabled", True)),
            "alpha": float(self.tts_policy_state.get("alpha", 0.24)),
            "failure_weight": float(self.tts_policy_state.get("failure_weight", 2.4)),
            "latency_weight": float(self.tts_policy_state.get("latency_weight", 0.6)),
            "route_bias": dict(route_bias if isinstance(route_bias, dict) else {}),
            "providers": dict(self.tts_policy_state.get("providers", {})),
            "recommended_provider": recommended,
            "recommended_chain": [recommended, "local" if recommended == "elevenlabs" else "elevenlabs"],
            "decision_history": list(self.tts_policy_state.get("decision_history", []))[-bounded:],
            "history_tail": list(self.tts_policy_state.get("history_tail", []))[-bounded:],
            "decision_count": len(self.tts_policy_state.get("decision_history", [])),
            "attempt_count": len(self.tts_policy_state.get("history_tail", [])),
            "state_path": "data/tts_policy_state.json",
            "last_loaded_at": "",
            "last_saved_at": "",
            "last_save_error": "",
            "dirty_updates": 0,
            "source": source,
            "mission_id": mission_id,
        }

    def update_tts_policy(self, payload: Dict[str, Any], *, source: str = "desktop-ui") -> Dict[str, Any]:
        data = payload if isinstance(payload, dict) else {}
        changed: Dict[str, Any] = {}
        for key in ("enabled", "learning_enabled"):
            if key in data:
                self.tts_policy_state[key] = bool(data.get(key))
                changed[key] = self.tts_policy_state[key]
        for key in ("alpha", "failure_weight", "latency_weight"):
            if key in data:
                self.tts_policy_state[key] = float(data.get(key))
                changed[key] = self.tts_policy_state[key]
        if isinstance(data.get("route_bias"), dict):
            route_bias = self.tts_policy_state.get("route_bias")
            if not isinstance(route_bias, dict):
                route_bias = {}
            route_bias.update(data.get("route_bias"))
            self.tts_policy_state["route_bias"] = route_bias
            changed["route_bias"] = dict(route_bias)
        if "recommended_provider" in data:
            self.tts_policy_state["recommended_provider"] = str(data.get("recommended_provider", "")).strip().lower() or "local"
            changed["recommended_provider"] = self.tts_policy_state["recommended_provider"]
        return {
            "status": "success",
            "updated": bool(changed),
            "changed": changed,
            "policy": self.get_tts_policy(limit=120, source=source),
        }

    def get_stt_policy(self, *, history_limit: int = 80, source: str = "desktop-ui") -> Dict[str, Any]:
        bounded = max(1, min(int(history_limit), 2000))
        route_bundle = self.model_route_bundle(
            stack_name="voice",
            tasks=["stt"],
            requires_offline=False,
            privacy_mode=True,
            mission_profile="balanced",
        )
        route_item = {}
        for row in route_bundle.get("items", []):
            if isinstance(row, dict) and str(row.get("task", "")).strip().lower() == "stt":
                route_item = dict(row)
                break
        route_policy = route_item.get("route_policy", {}) if isinstance(route_item.get("route_policy", {}), dict) else {}
        payload = dict(self.stt_policy_state)
        payload["status"] = "success"
        payload["available"] = True
        payload["source"] = source
        payload["history_limit"] = bounded
        payload["runtime_profile"] = dict(self.stt_runtime_profile_state)
        payload["model_route"] = {
            "status": "success",
            "selected_provider": str(route_item.get("provider", "")).strip().lower(),
            "selected_model": str(route_item.get("model", "")).strip(),
            "selected_local_path": str(route_item.get("selected_path", "")).strip(),
            "route_item": route_item,
            "route_policy": dict(route_policy),
            "route_adjusted": bool(route_item.get("route_adjusted", False)),
            "route_blocked": bool(route_item.get("route_blocked", False)),
            "route_warning": str(route_item.get("route_warning", "")),
        }
        payload["route_bundle"] = route_bundle
        payload["route_policy_summary"] = (
            dict(route_bundle.get("launch_policy_summary", {}))
            if isinstance(route_bundle.get("launch_policy_summary", {}), dict)
            else {}
        )
        payload["recommended_provider"] = str(route_policy.get("recommended_provider", "local")).strip().lower() or "local"
        payload["remediation_hints"] = [
            {
                "code": "stt_route_policy_blocked",
                "severity": "medium",
                "message": str(route_policy.get("reason", "") or "Local STT route blocked by launcher policy.").strip(),
                "recommended_provider": payload["recommended_provider"],
            }
            for _ in [1]
            if bool(route_item.get("route_blocked", False))
        ]
        return payload

    def update_stt_policy(self, payload: Dict[str, Any], *, source: str = "desktop-ui") -> Dict[str, Any]:
        data = payload if isinstance(payload, dict) else {}
        changed: Dict[str, Any] = {}
        float_keys = {
            "provider_cooldown_s",
            "provider_max_cooldown_s",
            "policy_base_cooldown_s",
            "policy_max_cooldown_s",
            "provider_state_persist_interval_s",
        }
        int_keys = {"provider_failure_streak_threshold", "policy_failure_streak_threshold"}
        bool_keys = {"provider_state_enabled"}
        for key in int_keys:
            if key in data:
                self.stt_policy_state[key] = int(data.get(key))
                changed[key] = self.stt_policy_state[key]
        for key in float_keys:
            if key in data:
                self.stt_policy_state[key] = float(data.get(key))
                changed[key] = self.stt_policy_state[key]
        for key in bool_keys:
            if key in data:
                self.stt_policy_state[key] = bool(data.get(key))
                changed[key] = self.stt_policy_state[key]
        if isinstance(data.get("providers"), dict):
            providers = self.stt_policy_state.get("providers")
            if not isinstance(providers, dict):
                providers = {}
            for provider_key, provider_value in data.get("providers", {}).items():
                provider_name = str(provider_key or "").strip().lower()
                if provider_name == "cloud":
                    provider_name = "groq"
                if provider_name not in {"local", "groq"}:
                    continue
                row = providers.get(provider_name)
                if not isinstance(row, dict):
                    row = {}
                if isinstance(provider_value, dict):
                    if "enabled" in provider_value:
                        row["enabled"] = bool(provider_value.get("enabled"))
                        changed[f"providers.{provider_name}.enabled"] = bool(row["enabled"])
                else:
                    row["enabled"] = bool(provider_value)
                    changed[f"providers.{provider_name}.enabled"] = bool(row["enabled"])
                providers[provider_name] = row
            self.stt_policy_state["providers"] = providers
        if bool(data.get("reset_runtime_history", False)):
            self.stt_policy_state["attempt_chain_history"] = []
            changed["reset_runtime_history"] = True
        if bool(data.get("reset_provider_policies", False)):
            self.stt_policy_state["provider_policies"] = {}
            changed["reset_provider_policies"] = True
        autotune = self.stt_policy_state.get("autotune")
        if not isinstance(autotune, dict):
            autotune = {}
        if "autotune_enabled" in data:
            autotune["enabled"] = bool(data.get("autotune_enabled"))
            changed["autotune_enabled"] = bool(autotune["enabled"])
        if "autotune_alpha" in data:
            autotune["alpha"] = float(data.get("autotune_alpha"))
            changed["autotune_alpha"] = autotune["alpha"]
        if "autotune_min_samples" in data:
            autotune["min_samples"] = int(data.get("autotune_min_samples"))
            changed["autotune_min_samples"] = autotune["min_samples"]
        if "autotune_bad_threshold" in data:
            autotune["bad_threshold"] = float(data.get("autotune_bad_threshold"))
            changed["autotune_bad_threshold"] = autotune["bad_threshold"]
        if "autotune_good_threshold" in data:
            autotune["good_threshold"] = float(data.get("autotune_good_threshold"))
            changed["autotune_good_threshold"] = autotune["good_threshold"]
        if "autotune_apply_cooldown_s" in data:
            autotune["apply_cooldown_s"] = float(data.get("autotune_apply_cooldown_s"))
            changed["autotune_apply_cooldown_s"] = autotune["apply_cooldown_s"]
        if "autotune_persist_every" in data:
            autotune["persist_every"] = int(data.get("autotune_persist_every"))
            changed["autotune_persist_every"] = autotune["persist_every"]
        if bool(data.get("reset_autotune_state", False)):
            autotune["scope_count"] = 0
            autotune["recent_count"] = 0
            changed["reset_autotune_state"] = True
        self.stt_policy_state["autotune"] = autotune
        return {
            "status": "success",
            "updated": bool(changed),
            "changed": changed,
            "policy": self.get_stt_policy(history_limit=120, source=source),
        }

    def get_voice_diagnostics(self, *, history_limit: int = 24) -> Dict[str, Any]:
        bounded = max(1, min(int(history_limit), 200))
        route_bundle = self.model_route_bundle(
            stack_name="voice",
            requires_offline=False,
            privacy_mode=True,
            mission_profile="balanced",
        )
        stt_policy = self.get_stt_policy(history_limit=bounded, source="voice-diagnostics")
        return {
            "status": "success",
            "history_limit": bounded,
            "voice": dict(self.voice_state),
            "stt": {
                "status": "success",
                "available": True,
                "provider": "fake-stt",
                "runtime_profile": dict(self.stt_runtime_profile_state),
                "last_model": self.stt_runtime_profile_state.get("model", ""),
                "provider_health": self.stt_policy_state.get("provider_health", "healthy"),
                "model_route": dict(stt_policy.get("model_route", {})) if isinstance(stt_policy.get("model_route", {}), dict) else {},
                "route_policy_summary": dict(stt_policy.get("route_policy_summary", {}))
                if isinstance(stt_policy.get("route_policy_summary", {}), dict)
                else {},
            },
            "stt_policy": stt_policy,
            "tts": self.get_tts_diagnostics(history_limit=bounded),
            "tts_bridge": dict(self.tts_bridge_state),
            "adaptive_learning": {
                "status": "success",
                "enabled": True,
                "history_limit": 50,
                "dynamic_profile_by_risk": {"low": "power", "medium": "balanced", "high": "strict"},
            },
            "route_bundle": route_bundle,
            "route_policy_summary": (
                dict(route_bundle.get("launch_policy_summary", {}))
                if isinstance(route_bundle.get("launch_policy_summary", {}), dict)
                else {}
            ),
            "route_policy_timeline": self.voice_route_policy_timeline(limit=max(8, min(80, bounded * 2))),
            "route_policy_history": self.voice_route_policy_history(limit=max(24, min(240, bounded * 5))),
            "wakeword_supervision_history": self.wakeword_supervision_history(limit=max(24, min(240, bounded * 5))),
            "wakeword_restart_history": self.wakeword_restart_history(limit=max(24, min(240, bounded * 5))),
            "wakeword_restart_policy_history": self.wakeword_restart_policy_history(
                limit=max(24, min(240, bounded * 5))
            ),
            "mission_reliability": self.voice_mission_reliability_status(mission_id="mission-voice-1", limit=12),
            "route_recovery_recommendation": {
                "mission_id": "mission-voice-1",
                "recovery_profile": "hybrid_polling",
                "wakeword_strategy": "hybrid_polling",
                "confidence": 0.74,
                "reasons": ["wakeword route is unstable, preferring faster fallback polling"],
                "session_overrides": {
                    "fallback_interval_s": 2.4,
                    "route_policy_resume_stability_s": 1.0,
                },
            },
            "planner_voice_route_policy": {
                "mission_id": "mission-voice-1",
                "policy_profile": "balanced",
                "risk_level": "medium",
                "ban_local_reasoning": True,
                "preferred_reasoning_provider": "groq",
                "local_voice_pressure_score": 0.63,
                "reason_code": "voice_route_policy_pressure",
            },
        }

    def get_local_neural_tts_bridge_status(self, *, probe: bool = False) -> Dict[str, Any]:
        payload = dict(self.tts_bridge_state)
        if probe:
            payload["probe_attempts"] = int(payload.get("probe_attempts", 0) or 0) + 1
        return payload

    def start_local_neural_tts_bridge(
        self,
        *,
        wait_ready: bool = True,
        timeout_s: float = 18.0,
        reason: str = "desktop-ui",
        force: bool = False,
    ) -> Dict[str, Any]:
        self.tts_bridge_state["running"] = True
        self.tts_bridge_state["ready"] = True
        self.tts_bridge_state["message"] = "bridge started"
        self.tts_bridge_state["wait_ready"] = bool(wait_ready)
        self.tts_bridge_state["timeout_s"] = float(timeout_s)
        self.tts_bridge_state["reason"] = str(reason)
        self.tts_bridge_state["force"] = bool(force)
        self.tts_bridge_state["start_attempts"] = int(self.tts_bridge_state.get("start_attempts", 0) or 0) + 1
        return dict(self.tts_bridge_state)

    def stop_local_neural_tts_bridge(self, *, reason: str = "desktop-ui") -> Dict[str, Any]:
        self.tts_bridge_state["running"] = False
        self.tts_bridge_state["ready"] = False
        self.tts_bridge_state["message"] = "bridge stopped"
        self.tts_bridge_state["reason"] = str(reason)
        return dict(self.tts_bridge_state)

    def probe_local_neural_tts_bridge(self, *, force: bool = True) -> Dict[str, Any]:
        self.tts_bridge_state["probe_attempts"] = int(self.tts_bridge_state.get("probe_attempts", 0) or 0) + 1
        self.tts_bridge_state["message"] = "bridge healthy" if self.tts_bridge_state.get("ready", False) else "bridge probe failed"
        self.tts_bridge_state["force"] = bool(force)
        return dict(self.tts_bridge_state)

    def apply_local_neural_tts_profile(
        self,
        *,
        profile_id: str,
        replace: bool = True,
        restart: bool = True,
        wait_ready: bool = True,
        timeout_s: float | None = None,
        force: bool = False,
        override_updates: Dict[str, Any] | None = None,
        template_id: str = "",
    ) -> Dict[str, Any]:
        clean_profile_id = str(profile_id or "").strip().lower()
        clean_template_id = str(template_id or "").strip().lower()
        selected = next(
            (
                row
                for row in self.model_bridge_profiles(task="tts", limit=64).get("profiles", [])
                if str((row or {}).get("profile_id", "")).strip().lower() == clean_profile_id
            ),
            None,
        )
        if not isinstance(selected, dict):
            return {"status": "error", "message": f"tts bridge profile not found: {clean_profile_id}", "ready": False}
        override_patch = dict(override_updates) if isinstance(override_updates, dict) else (
            dict(selected.get("override_patch", {})) if isinstance(selected.get("override_patch"), dict) else {}
        )
        if replace:
            self.tts_bridge_state["runtime_overrides"] = {}
        self.tts_bridge_state["runtime_overrides"] = dict(override_patch)
        self.tts_bridge_state["active_profile_id"] = clean_profile_id
        self.tts_bridge_state["active_template_id"] = clean_template_id
        self.tts_bridge_state["endpoint"] = str(override_patch.get("endpoint", self.tts_bridge_state.get("endpoint", "")) or "")
        self.tts_bridge_state["healthcheck_url"] = str(
            override_patch.get("healthcheck_url", self.tts_bridge_state.get("healthcheck_url", ""))
            or ""
        )
        self.tts_bridge_state["message"] = (
            f"Applied local neural TTS template '{clean_template_id}' for profile '{clean_profile_id}'."
            if clean_template_id
            else f"Applied local neural TTS profile '{clean_profile_id}'."
        )
        if restart:
            self.tts_bridge_state["running"] = True
            self.tts_bridge_state["ready"] = True
            self.tts_bridge_state["wait_ready"] = bool(wait_ready)
            self.tts_bridge_state["timeout_s"] = float(timeout_s or 18.0)
            self.tts_bridge_state["force"] = bool(force)
            self.tts_bridge_state["restart_count"] = int(self.tts_bridge_state.get("restart_count", 0) or 0) + 1
        local_tts = self.tts_state.get("local", {})
        if isinstance(local_tts, dict):
            neural_runtime = dict(local_tts.get("neural_runtime", {})) if isinstance(local_tts.get("neural_runtime"), dict) else {}
            neural_runtime.update(
                {
                    "configured": True,
                    "enabled": True,
                    "ready": True,
                    "backend": str(override_patch.get("backend", neural_runtime.get("backend", "llama_cpp")) or "llama_cpp"),
                    "execution_backend": str(
                        override_patch.get("execution_backend", neural_runtime.get("execution_backend", "openai_http"))
                        or "openai_http"
                    ),
                    "model_path": str(override_patch.get("model_path", neural_runtime.get("model_path", "")) or ""),
                    "model_label": str(override_patch.get("model_label", neural_runtime.get("model_label", "")) or ""),
                    "http_endpoint": str(
                        override_patch.get(
                            "http_endpoint",
                            override_patch.get("endpoint", neural_runtime.get("http_endpoint", "")),
                        )
                        or ""
                    ),
                    "http_model": str(override_patch.get("http_model", neural_runtime.get("http_model", "tts-1")) or "tts-1"),
                    "voice": str(override_patch.get("voice", neural_runtime.get("voice", "jarvis")) or "jarvis"),
                    "output_format": str(override_patch.get("output_format", neural_runtime.get("output_format", "wav")) or "wav"),
                    "active_profile_id": clean_profile_id,
                    "active_template_id": clean_template_id,
                    "runtime_overrides": dict(override_patch),
                    "bridge_ready": bool(self.tts_bridge_state.get("ready", False)),
                    "bridge": dict(self.tts_bridge_state),
                }
            )
            local_tts["neural_runtime"] = neural_runtime
            providers = local_tts.get("providers")
            if isinstance(providers, dict):
                provider_row = providers.get("neural_runtime")
                if not isinstance(provider_row, dict):
                    provider_row = {}
                provider_row.update(
                    {
                        "ready": True,
                        "configured": True,
                        "enabled": True,
                        "backend": neural_runtime.get("backend"),
                        "execution_backend": neural_runtime.get("execution_backend"),
                        "bridge_ready": bool(self.tts_bridge_state.get("ready", False)),
                    }
                )
                providers["neural_runtime"] = provider_row
                local_tts["providers"] = providers
            self.tts_state["local"] = local_tts
        return {
            "status": "success",
            "profile_id": clean_profile_id,
            "template_id": clean_template_id,
            "profile": dict(selected),
            "replace": bool(replace),
            "override_patch": dict(override_patch),
            "ready": True,
            "bridge": dict(self.tts_bridge_state),
            "restart": dict(self.tts_bridge_state) if restart else None,
            "neural_runtime": dict(self.tts_state.get("local", {}).get("neural_runtime", {})),
            "tts_diagnostics": self.get_tts_diagnostics(history_limit=24),
        }

    def apply_local_stt_profile(
        self,
        *,
        profile_id: str,
        replace: bool = True,
        restart_voice_if_running: bool = True,
        override_updates: Dict[str, Any] | None = None,
        template_id: str = "",
    ) -> Dict[str, Any]:
        clean_profile_id = str(profile_id or "").strip().lower()
        clean_template_id = str(template_id or "").strip().lower()
        selected = next(
            (
                row
                for row in self.model_bridge_profiles(task="stt", limit=64).get("profiles", [])
                if str((row or {}).get("profile_id", "")).strip().lower() == clean_profile_id
            ),
            None,
        )
        if not isinstance(selected, dict):
            return {"status": "error", "message": f"stt runtime profile not found: {clean_profile_id}", "ready": False}
        override_patch = dict(override_updates) if isinstance(override_updates, dict) else (
            dict(selected.get("override_patch", {})) if isinstance(selected.get("override_patch"), dict) else {}
        )
        self.stt_runtime_profile_state.update(
            {
                "status": "success",
                "profile_id": clean_profile_id,
                "template_id": clean_template_id,
                "model": str(override_patch.get("model", self.stt_runtime_profile_state.get("model", "whisper-large-v3"))),
                "local_model_path": str(
                    override_patch.get("local_model_path", self.stt_runtime_profile_state.get("local_model_path", ""))
                ),
                "voice_running": bool(restart_voice_if_running and self.voice_state.get("running", False)),
                "available": True,
                "updated_at": 1741400000.0,
            }
        )
        return {
            "status": "success",
            "profile_id": clean_profile_id,
            "template_id": clean_template_id,
            "profile": dict(selected),
            "replace": bool(replace),
            "override_patch": dict(override_patch),
            "ready": True,
            "stt_diagnostics": {
                "status": "success",
                "available": True,
                "provider_health": self.stt_policy_state.get("provider_health", "healthy"),
                "last_model": self.stt_runtime_profile_state.get("model", ""),
                "runtime_profile": dict(self.stt_runtime_profile_state),
            },
            "stt_policy": self.get_stt_policy(history_limit=120, source="stt-profile-apply"),
            "voice": dict(self.voice_state),
            "restart": dict(self.voice_state) if restart_voice_if_running else None,
            "runtime_profile": dict(self.stt_runtime_profile_state),
        }

    def apply_local_vision_profile(
        self,
        *,
        profile_id: str,
        override_updates: Dict[str, Any] | None = None,
        template_id: str = "",
    ) -> Dict[str, Any]:
        clean_profile_id = str(profile_id or "").strip().lower()
        clean_template_id = str(template_id or "").strip().lower()
        selected = next(
            (
                row
                for row in self.model_bridge_profiles(task="vision", limit=64).get("profiles", [])
                if str((row or {}).get("profile_id", "")).strip().lower() == clean_profile_id
            ),
            None,
        )
        if not isinstance(selected, dict):
            return {"status": "error", "message": f"vision runtime profile not found: {clean_profile_id}", "ready": False}
        override_patch = dict(override_updates) if isinstance(override_updates, dict) else (
            dict(selected.get("override_patch", {})) if isinstance(selected.get("override_patch"), dict) else {}
        )
        selected_models = [
            str(item or "").strip().lower()
            for item in (override_patch.get("models", []) if isinstance(override_patch.get("models"), list) else [])
            if str(item or "").strip()
        ]
        if not selected_models:
            return {"status": "error", "message": "vision runtime profile has no selected models", "ready": False}
        if bool(override_patch.get("clear_cache", False)):
            self.reset_vision_runtime(models=selected_models, clear_cache=True)
        warm_payload = self.warm_vision_runtime(
            models=selected_models,
            force_reload=bool(override_patch.get("force_reload", False)) and not bool(override_patch.get("clear_cache", False)),
        )
        self.vision_runtime_profile_state.update(
            {
                "status": "success",
                "profile_id": clean_profile_id,
                "template_id": clean_template_id,
                "models": list(selected_models),
                "loaded_count": int(self.vision_runtime_state.get("loaded_count", 0) or 0),
                "available": True,
                "updated_at": 1741400000.0,
            }
        )
        return {
            "status": "success",
            "profile_id": clean_profile_id,
            "template_id": clean_template_id,
            "profile": dict(selected),
            "override_patch": dict(override_patch),
            "ready": True,
            "vision_runtime": dict(self.vision_runtime_state),
            "warm": dict(warm_payload),
            "runtime_profile": dict(self.vision_runtime_profile_state),
        }

    def execute_model_launch_template(
        self,
        *,
        profile_id: str,
        template_id: str,
        replace: bool = True,
        wait_ready: bool = True,
        timeout_s: float | None = None,
        force: bool = False,
        allow_unready: bool = False,
        probe: bool = True,
        auto_fallback: bool = True,
        retry_on_failure: bool = True,
        max_attempts: int = 2,
        retry_profile: str = "",
        retry_base_delay_ms: int | None = None,
        retry_max_delay_ms: int | None = None,
        retry_jitter_ms: int | None = None,
        retry_prefer_recommended: bool | None = None,
        _attempt_chain: list[Dict[str, Any]] | None = None,
        _attempted_template_ids: list[str] | None = None,
        _attempt_index: int = 1,
        _attempt_chain_id: str = "",
        _original_requested_template_id: str = "",
        _original_requested_template: Dict[str, Any] | None = None,
        _retry_trigger: str = "",
    ) -> Dict[str, Any]:
        clean_profile_id = str(profile_id or "").strip().lower()
        clean_template_id = str(template_id or "").strip().lower()
        selected = next(
            (
                row
                for row in self.model_bridge_profiles(limit=96).get("profiles", [])
                if str((row or {}).get("profile_id", "")).strip().lower() == clean_profile_id
            ),
            None,
        )
        if not isinstance(selected, dict):
            return {"status": "error", "message": f"bridge profile not found: {clean_profile_id}", "ready": False}
        selected_template = next(
            (
                row
                for row in selected.get("launch_templates", [])
                if isinstance(row, dict) and str((row or {}).get("template_id", "")).strip().lower() == clean_template_id
            ),
            None,
        )
        if not isinstance(selected_template, dict):
            return {"status": "error", "message": f"launch template not found: {clean_template_id}", "ready": False}
        requested_template_id = clean_template_id
        requested_template = dict(selected_template)
        requested_health = (
            dict(requested_template.get("health", {}))
            if isinstance(requested_template.get("health"), dict)
            else {}
        )
        bounded_max_attempts = max(1, min(int(max_attempts or 1), 4))
        attempt_index = max(1, int(_attempt_index or 1))
        attempt_chain_id = str(_attempt_chain_id or f"{clean_profile_id}:{time.time_ns()}").strip()
        attempted_template_ids = [
            str(item or "").strip().lower()
            for item in (_attempted_template_ids or [])
            if str(item or "").strip()
        ]
        original_requested_template_id = (
            str(_original_requested_template_id or requested_template_id).strip().lower()
            or requested_template_id
        )
        original_requested_template = (
            dict(_original_requested_template)
            if isinstance(_original_requested_template, dict)
            else dict(requested_template)
        )
        if (
            str(original_requested_template.get("template_id", "") or "").strip().lower()
            != original_requested_template_id
        ):
            original_requested_template = next(
                (
                    dict(row)
                    for row in selected.get("launch_templates", [])
                    if isinstance(row, dict)
                    and str(row.get("template_id", "") or "").strip().lower() == original_requested_template_id
                ),
                dict(original_requested_template),
            )
        fallback_applied = False
        fallback_reason = ""
        fallback_source = ""
        recommended_template_id = str(selected.get("recommended_launch_template_id", "") or "").strip().lower()
        if auto_fallback and recommended_template_id and recommended_template_id != clean_template_id:
            fallback_candidate = next(
                (
                    row
                    for row in selected.get("launch_templates", [])
                    if isinstance(row, dict) and str((row or {}).get("template_id", "")).strip().lower() == recommended_template_id
                ),
                None,
            )
            fallback_reasons: list[str] = []
            if bool(requested_template.get("blacklisted", False)) or bool(requested_health.get("blacklisted", False)):
                fallback_reasons.append("policy_blacklisted")
            if bool(requested_template.get("suppressed", False)) or bool(requested_health.get("suppressed", False)):
                fallback_reasons.append("strategy_suppressed")
            if bool(requested_health.get("demoted", False)):
                fallback_reasons.append("demoted")
            elif bool(requested_health.get("unstable", False)) and (
                int(requested_health.get("failure_streak", 0) or 0) >= 2
                or float(requested_health.get("recent_failure_rate", 0.0) or 0.0) >= 0.67
            ):
                fallback_reasons.append("unstable")
            if not bool(requested_template.get("ready", False)) and not allow_unready:
                fallback_reasons.append("not_ready")
            if fallback_reasons and isinstance(fallback_candidate, dict):
                selected_template = dict(fallback_candidate)
                clean_template_id = recommended_template_id
                fallback_applied = True
                fallback_reason = "+".join(dict.fromkeys(fallback_reasons))
                fallback_source = "recommended_launch_template"
        if not bool(selected_template.get("ready", False)) and not allow_unready:
            return {
                "status": "error",
                "message": f"launch template '{clean_template_id}' is not ready",
                "profile_id": clean_profile_id,
                "template_id": clean_template_id,
                "profile": dict(selected),
                "template": dict(selected_template),
                "missing_requirements": list(selected_template.get("missing_requirements", [])),
                "remediation_hints": list(selected_template.get("remediation_hints", [])),
                "ready": False,
            }
        template_patch = dict(selected_template.get("profile_patch", {})) if isinstance(selected_template.get("profile_patch"), dict) else {}
        bridge_kind = str(selected.get("bridge_kind", "") or "").strip().lower()
        launcher = str(selected_template.get("launcher", "") or "").strip().lower()
        manual_only = bool(selected_template.get("manual_only", False))
        autostart_capable = bool(selected_template.get("autostart_capable", False))
        forced_status = str(self.launch_template_forced_status_by_id.get(clean_template_id, "") or "").strip().lower()

        def _finish(payload: Dict[str, Any]) -> Dict[str, Any]:
            response = dict(payload)
            response.setdefault("bridge_kind", bridge_kind)
            response.setdefault("profile_id", clean_profile_id)
            response.setdefault("template_id", clean_template_id)
            response.setdefault("launcher", launcher)
            response.setdefault("manual_only", manual_only)
            response.setdefault("autostart_capable", autostart_capable)
            response.setdefault("profile", dict(selected))
            response.setdefault("template", dict(selected_template))
            response.setdefault("requested_template_id", requested_template_id)
            response.setdefault("requested_template", dict(requested_template))
            response.setdefault("executed_template_id", clean_template_id)
            response.setdefault("fallback_applied", fallback_applied)
            response.setdefault("fallback_reason", fallback_reason)
            response.setdefault("fallback_source", fallback_source)
            response.setdefault("attempt_chain_id", attempt_chain_id)
            response.setdefault("attempt_index", attempt_index)
            response.setdefault("retry_trigger", str(_retry_trigger or "").strip().lower())
            response.setdefault("missing_requirements", list(selected_template.get("missing_requirements", [])))
            response.setdefault("remediation_hints", list(selected_template.get("remediation_hints", [])))
            response["duration_s"] = 0.18
            if forced_status in {"error", "degraded"}:
                response["status"] = forced_status
                response["ready"] = False
                if forced_status == "error":
                    response["message"] = f"forced failure for {clean_template_id}"
            initial_failure_like = str(response.get("status", "unknown")).strip().lower() in {"error", "blocked", "degraded"} or not bool(response.get("ready", False))
            initial_retry_policy = self._model_launch_retry_policy(
                bridge_kind=bridge_kind,
                profile_id=clean_profile_id,
                requested_template=requested_template,
                current_template=selected_template,
                recommended_template_id=str(selected.get("recommended_launch_template_id", "") or ""),
                attempt_index=attempt_index,
                max_attempts=bounded_max_attempts,
                failure_like=initial_failure_like,
                retry_profile=retry_profile,
                retry_base_delay_ms=retry_base_delay_ms,
                retry_max_delay_ms=retry_max_delay_ms,
                retry_jitter_ms=retry_jitter_ms,
                retry_prefer_recommended=retry_prefer_recommended,
            )
            attempt_execution_diff = self._model_launch_execution_diff(
                requested_template_id=requested_template_id,
                executed_template_id=clean_template_id,
                requested_template=requested_template,
                executed_template=selected_template,
            )
            root_execution_diff = self._model_launch_execution_diff(
                requested_template_id=original_requested_template_id,
                executed_template_id=clean_template_id,
                requested_template=original_requested_template,
                executed_template=selected_template,
            )
            requested_template_launcher = str(requested_template.get("launcher", "") or launcher).strip().lower()
            requested_template_transport = str(requested_template.get("transport", "") or "").strip().lower()
            requested_template_manual_only = bool(requested_template.get("manual_only", manual_only))
            requested_template_autostart_capable = bool(
                requested_template.get("autostart_capable", autostart_capable)
            )
            history_event = self._record_launch_template_event(
                profile_id=clean_profile_id,
                template_id=clean_template_id,
                requested_template_id=requested_template_id,
                bridge_kind=bridge_kind,
                launcher=launcher,
                requested_launcher=requested_template_launcher,
                requested_transport=requested_template_transport,
                requested_manual_only=requested_template_manual_only,
                requested_autostart_capable=requested_template_autostart_capable,
                ready=bool(response.get("ready", False)),
                status=str(response.get("status", "unknown") or "unknown"),
                fallback_applied=fallback_applied,
                fallback_reason=fallback_reason,
                fallback_source=fallback_source,
                attempt_chain_id=attempt_chain_id,
                attempt_index=attempt_index,
                retry_trigger=str(response.get("retry_trigger", "") or ""),
                retry_requested_profile=str(initial_retry_policy.get("requested_profile", "") or ""),
                retry_profile=str(initial_retry_policy.get("profile", "") or ""),
                retry_profile_adjusted=bool(initial_retry_policy.get("profile_adjusted", False)),
                retry_profile_adjustment_reason=str(initial_retry_policy.get("profile_adjustment_reason", "") or ""),
                retry_strategy=str(initial_retry_policy.get("strategy", "") or ""),
                retry_strategy_score=round(
                    float(
                        (
                            initial_retry_policy.get("strategy_health", {})
                            if isinstance(initial_retry_policy.get("strategy_health"), dict)
                            else {}
                        ).get("score", 0.0)
                        or 0.0
                    ),
                    4,
                ),
                retry_escalation_mode=str(initial_retry_policy.get("escalation_mode", "") or ""),
                retry_prefer_recommended=bool(initial_retry_policy.get("prefer_recommended", False)),
                retry_delay_ms=int(initial_retry_policy.get("delay_ms", 0) or 0),
                retry_jitter_ms=int(initial_retry_policy.get("jitter_ms", 0) or 0),
                retry_recommended_template_id=str(initial_retry_policy.get("recommended_template_id", "") or ""),
                retry_preview_schedule_ms=list(initial_retry_policy.get("preview_schedule_ms", [])),
                execution_diff=attempt_execution_diff,
            )
            refreshed_catalog = self.model_bridge_profiles(task=bridge_kind, limit=96)
            refreshed_profile = next(
                (
                    row
                    for row in refreshed_catalog.get("profiles", [])
                    if isinstance(row, dict) and str(row.get("profile_id", "")).strip().lower() == clean_profile_id
                ),
                dict(selected),
            )
            refreshed_template = next(
                (
                    row
                    for row in refreshed_profile.get("launch_templates", [])
                    if isinstance(row, dict) and str(row.get("template_id", "")).strip().lower() == clean_template_id
                ),
                dict(selected_template),
            )
            response["profile"] = dict(refreshed_profile) if isinstance(refreshed_profile, dict) else dict(selected)
            response["template"] = dict(refreshed_template) if isinstance(refreshed_template, dict) else dict(selected_template)
            response["history_record"] = dict(history_event)
            response["template_health"] = (
                dict(refreshed_template.get("health", {}))
                if isinstance(refreshed_template, dict) and isinstance(refreshed_template.get("health"), dict)
                else {}
            )
            response["profile_launch_health"] = (
                dict(refreshed_profile.get("launch_health", {}))
                if isinstance(refreshed_profile, dict) and isinstance(refreshed_profile.get("launch_health"), dict)
                else {}
            )
            response["recommended_template_id"] = (
                str(refreshed_profile.get("recommended_launch_template_id", "")).strip()
                if isinstance(refreshed_profile, dict)
                else ""
            )
            response["execution_diff"] = root_execution_diff
            response["attempt_execution_diff"] = attempt_execution_diff
            response["history_record"] = dict(history_event)
            current_attempt = {
                "attempt_index": attempt_index,
                "attempt_chain_id": attempt_chain_id,
                "retry_trigger": str(_retry_trigger or "").strip().lower(),
                "status": str(response.get("status", "unknown") or "unknown").strip().lower() or "unknown",
                "ready": bool(response.get("ready", False)),
                "requested_template_id": requested_template_id,
                "executed_template_id": clean_template_id,
                "fallback_applied": fallback_applied,
                "fallback_reason": fallback_reason,
                "history_record": dict(history_event),
                "execution_diff": dict(attempt_execution_diff),
            }
            combined_attempt_chain = list(_attempt_chain or [])
            combined_attempt_chain.append(current_attempt)
            response["attempt_chain"] = combined_attempt_chain
            response["attempt_count"] = len(combined_attempt_chain)
            response["attempt_chain_id"] = attempt_chain_id
            response["requested_template_id"] = original_requested_template_id
            response["root_requested_template_id"] = original_requested_template_id
            response["attempt_requested_template_id"] = requested_template_id
            response["retry_applied"] = len(combined_attempt_chain) > 1
            if response.get("fallback_applied"):
                response.setdefault(
                    "message",
                    f"Auto-fallback switched launch execution from {requested_template_id} to {clean_template_id}.",
                )
            failure_like = str(response.get("status", "unknown")).strip().lower() in {"error", "blocked", "degraded"} or not bool(response.get("ready", False))
            response_retry_policy = self._model_launch_retry_policy(
                bridge_kind=bridge_kind,
                profile_id=clean_profile_id,
                requested_template=requested_template,
                current_template=refreshed_template if isinstance(refreshed_template, dict) else selected_template,
                recommended_template_id=(
                    str(refreshed_profile.get("recommended_launch_template_id", "")).strip()
                    if isinstance(refreshed_profile, dict)
                    else ""
                ),
                attempt_index=attempt_index,
                max_attempts=bounded_max_attempts,
                failure_like=failure_like,
                retry_profile=retry_profile,
                retry_base_delay_ms=retry_base_delay_ms,
                retry_max_delay_ms=retry_max_delay_ms,
                retry_jitter_ms=retry_jitter_ms,
                retry_prefer_recommended=retry_prefer_recommended,
            )
            response["retry_policy"] = dict(response_retry_policy)
            current_attempt["retry_policy"] = dict(response_retry_policy)
            current_attempt["retry_requested_profile"] = str(response_retry_policy.get("requested_profile", "") or "").strip().lower()
            current_attempt["retry_profile"] = str(response_retry_policy.get("profile", "") or "").strip().lower()
            current_attempt["retry_profile_adjusted"] = bool(response_retry_policy.get("profile_adjusted", False))
            current_attempt["retry_profile_adjustment_reason"] = str(
                response_retry_policy.get("profile_adjustment_reason", "") or ""
            ).strip().lower()
            current_attempt["retry_strategy"] = str(response_retry_policy.get("strategy", "") or "").strip().lower()
            current_attempt["retry_strategy_score"] = round(
                float(
                    (
                        response_retry_policy.get("strategy_health", {})
                        if isinstance(response_retry_policy.get("strategy_health"), dict)
                        else {}
                    ).get("score", 0.0)
                    or 0.0
                ),
                4,
            )
            current_attempt["retry_escalation_mode"] = str(
                response_retry_policy.get("escalation_mode", "") or ""
            ).strip().lower()
            current_attempt["retry_delay_ms"] = int(response_retry_policy.get("delay_ms", 0) or 0)
            current_attempt["retry_prefer_recommended"] = bool(response_retry_policy.get("prefer_recommended", False))
            exhausted_attempts = list(dict.fromkeys(attempted_template_ids + [clean_template_id]))
            if retry_on_failure and bounded_max_attempts > attempt_index and failure_like:
                retry_candidate = None
                preferred_retry_template_id = (
                    str(response_retry_policy.get("recommended_template_id", "") or "").strip().lower()
                    if bool(response_retry_policy.get("prefer_recommended", False))
                    else ""
                )
                if isinstance(refreshed_profile, dict):
                    launch_templates = refreshed_profile.get("launch_templates", [])
                    if isinstance(launch_templates, list) and preferred_retry_template_id:
                        retry_candidate = next(
                            (
                                row
                                for row in launch_templates
                                if isinstance(row, dict)
                                and str(row.get("template_id", "")).strip().lower() == preferred_retry_template_id
                                and str(row.get("template_id", "")).strip().lower() not in exhausted_attempts
                                and (allow_unready or bool(row.get("ready", False)))
                                and not bool(row.get("blacklisted", False))
                                and not bool(row.get("suppressed", False))
                            ),
                            None,
                        )
                    if not isinstance(retry_candidate, dict):
                        retry_candidate = next(
                            (
                                row
                                for row in launch_templates
                                if isinstance(row, dict)
                                and str(row.get("template_id", "")).strip().lower() not in exhausted_attempts
                                and (allow_unready or bool(row.get("ready", False)))
                                and not bool(row.get("blacklisted", False))
                                and not bool(row.get("suppressed", False))
                            ),
                            None,
                        )
                    if not isinstance(retry_candidate, dict):
                        retry_candidate = next(
                            (
                                row
                                for row in launch_templates
                                if isinstance(row, dict)
                                and str(row.get("template_id", "")).strip().lower() not in exhausted_attempts
                                and (allow_unready or bool(row.get("ready", False)))
                            ),
                            None,
                        )
                if isinstance(retry_candidate, dict):
                    retry_template_id = str(retry_candidate.get("template_id", "")).strip().lower()
                    retry_delay_ms = int(response_retry_policy.get("delay_ms", 0) or 0)
                    current_attempt["retry_target_template_id"] = retry_template_id
                    current_attempt["retry_delay_ms"] = retry_delay_ms
                    current_attempt["retry_backoff_applied"] = retry_delay_ms > 0
                    response["retry_reason"] = "prior_attempt_failure"
                    response["retry_target_template_id"] = retry_template_id
                    response["retry_delay_ms"] = retry_delay_ms
                    response["retry_backoff_applied"] = retry_delay_ms > 0
                    retry_response = self.execute_model_launch_template(
                        profile_id=clean_profile_id,
                        template_id=retry_template_id,
                        replace=replace,
                        wait_ready=wait_ready,
                        timeout_s=timeout_s,
                        force=force,
                        allow_unready=allow_unready,
                        probe=probe,
                        auto_fallback=False,
                        retry_on_failure=retry_on_failure,
                        max_attempts=bounded_max_attempts,
                        retry_profile=retry_profile,
                        retry_base_delay_ms=retry_base_delay_ms,
                        retry_max_delay_ms=retry_max_delay_ms,
                        retry_jitter_ms=retry_jitter_ms,
                        retry_prefer_recommended=retry_prefer_recommended,
                        _attempt_chain=combined_attempt_chain,
                        _attempted_template_ids=exhausted_attempts,
                        _attempt_index=attempt_index + 1,
                        _attempt_chain_id=attempt_chain_id,
                        _original_requested_template_id=original_requested_template_id,
                        _original_requested_template=original_requested_template,
                        _retry_trigger="prior_attempt_failure",
                    )
                    if isinstance(retry_response, dict):
                        retry_response["retry_applied"] = True
                        retry_response.setdefault("retry_reason", "prior_attempt_failure")
                        retry_response.setdefault("retry_target_template_id", retry_template_id)
                        retry_response.setdefault("retry_delay_ms", retry_delay_ms)
                        retry_response.setdefault("retry_backoff_applied", retry_delay_ms > 0)
                        return retry_response
                response["retry_candidates_exhausted"] = True
            return response

        if bridge_kind == "reasoning":
            should_restart = not manual_only and (autostart_capable or launcher in {"llama-server", "reasoning_bridge"})
            apply_payload = self.apply_local_reasoning_bridge_profile(
                profile_id=clean_profile_id,
                replace=replace,
                restart=should_restart,
                wait_ready=wait_ready,
                timeout_s=timeout_s,
                force=force,
                override_updates=template_patch,
                template_id=clean_template_id,
            )
            probe_payload = self.probe_local_reasoning_bridge(force=True) if probe and not should_restart else None
            bridge_payload = dict(probe_payload) if isinstance(probe_payload, dict) else dict(apply_payload.get("bridge", {}))
            return _finish({
                "status": "success" if bool(bridge_payload.get("ready", False)) or not probe else "degraded",
                "bridge_kind": bridge_kind,
                "profile_id": clean_profile_id,
                "template_id": clean_template_id,
                "launcher": launcher,
                "manual_only": manual_only,
                "autostart_capable": autostart_capable,
                "ready": bool(bridge_payload.get("ready", False)),
                "profile": dict(selected),
                "template": dict(selected_template),
                "bridge": bridge_payload,
                "apply": dict(apply_payload),
                "probe_result": dict(probe_payload) if isinstance(probe_payload, dict) else None,
                "reasoning_runtime": self.model_runtime_supervisors(preferred_model_name=str(selected.get("name", "")), limit=8),
                "missing_requirements": list(selected_template.get("missing_requirements", [])),
                "remediation_hints": list(selected_template.get("remediation_hints", [])),
                "recommendations": list(selected_template.get("remediation_hints", []))
                + (
                    [
                        f"Auto-fallback switched from {requested_template_id} to {clean_template_id} because the requested launcher was {fallback_reason.replace('+', ', ')}."
                    ]
                    if fallback_applied
                    else []
                ),
            })
        if bridge_kind == "stt":
            apply_payload = self.apply_local_stt_profile(
                profile_id=clean_profile_id,
                replace=replace,
                restart_voice_if_running=True,
                override_updates=template_patch,
                template_id=clean_template_id,
            )
            return _finish({
                "status": "success" if bool(apply_payload.get("ready", False)) else "degraded",
                "bridge_kind": bridge_kind,
                "profile_id": clean_profile_id,
                "template_id": clean_template_id,
                "launcher": launcher,
                "manual_only": manual_only,
                "autostart_capable": autostart_capable,
                "ready": bool(apply_payload.get("ready", False)),
                "profile": dict(selected),
                "template": dict(selected_template),
                "apply": dict(apply_payload),
                "stt_diagnostics": dict(apply_payload.get("stt_diagnostics", {})),
                "stt_policy": dict(apply_payload.get("stt_policy", {})),
                "runtime_profile": dict(apply_payload.get("runtime_profile", {})),
                "missing_requirements": list(selected_template.get("missing_requirements", [])),
                "remediation_hints": list(selected_template.get("remediation_hints", [])),
                "recommendations": list(selected_template.get("remediation_hints", []))
                + (
                    [
                        f"Auto-fallback switched from {requested_template_id} to {clean_template_id} because the requested launcher was {fallback_reason.replace('+', ', ')}."
                    ]
                    if fallback_applied
                    else []
                ),
            })
        execution_backend = str(template_patch.get("execution_backend", selected.get("execution_backend", "")) or "").strip().lower()
        if bridge_kind == "tts":
            should_restart = not manual_only
            apply_payload = self.apply_local_neural_tts_profile(
                profile_id=clean_profile_id,
                replace=replace,
                restart=should_restart,
                wait_ready=wait_ready,
                timeout_s=timeout_s,
                force=force,
                override_updates=template_patch,
                template_id=clean_template_id,
            )
            probe_payload = self.probe_local_neural_tts_bridge(force=True) if probe and manual_only and execution_backend == "openai_http" else None
            bridge_payload = dict(probe_payload) if isinstance(probe_payload, dict) else dict(apply_payload.get("bridge", {}))
            return _finish({
                "status": "success" if bool(apply_payload.get("ready", False)) else "degraded",
                "bridge_kind": bridge_kind,
                "profile_id": clean_profile_id,
                "template_id": clean_template_id,
                "launcher": launcher,
                "manual_only": manual_only,
                "autostart_capable": autostart_capable,
                "execution_backend": execution_backend,
                "ready": bool(apply_payload.get("ready", False)),
                "profile": dict(selected),
                "template": dict(selected_template),
                "bridge": bridge_payload,
                "apply": dict(apply_payload),
                "probe_result": dict(probe_payload) if isinstance(probe_payload, dict) else None,
                "neural_runtime": dict(apply_payload.get("neural_runtime", {})),
                "tts_diagnostics": self.get_tts_diagnostics(history_limit=24),
                "missing_requirements": list(selected_template.get("missing_requirements", [])),
                "remediation_hints": list(selected_template.get("remediation_hints", [])),
                "recommendations": list(selected_template.get("remediation_hints", []))
                + (
                    [
                        f"Auto-fallback switched from {requested_template_id} to {clean_template_id} because the requested launcher was {fallback_reason.replace('+', ', ')}."
                    ]
                    if fallback_applied
                    else []
                ),
            })
        if bridge_kind == "vision":
            apply_payload = self.apply_local_vision_profile(
                profile_id=clean_profile_id,
                override_updates=template_patch,
                template_id=clean_template_id,
            )
            return _finish({
                "status": "success" if bool(apply_payload.get("ready", False)) else "degraded",
                "bridge_kind": bridge_kind,
                "profile_id": clean_profile_id,
                "template_id": clean_template_id,
                "launcher": launcher,
                "manual_only": manual_only,
                "autostart_capable": autostart_capable,
                "ready": bool(apply_payload.get("ready", False)),
                "profile": dict(selected),
                "template": dict(selected_template),
                "apply": dict(apply_payload),
                "vision_runtime": dict(apply_payload.get("vision_runtime", {})),
                "runtime_profile": dict(apply_payload.get("runtime_profile", {})),
                "missing_requirements": list(selected_template.get("missing_requirements", [])),
                "remediation_hints": list(selected_template.get("remediation_hints", [])),
            "recommendations": list(selected_template.get("remediation_hints", []))
            + (
                [
                    f"Auto-fallback switched from {requested_template_id} to {clean_template_id} because the requested launcher was {fallback_reason.replace('+', ', ')}."
                ]
                if fallback_applied
                else []
            ),
        })
        return _finish({
            "status": "error",
            "message": f"unsupported bridge kind: {bridge_kind}",
            "profile_id": clean_profile_id,
            "template_id": clean_template_id,
            "profile": dict(selected),
            "template": dict(selected_template),
            "ready": False,
        })

    def start_voice_session(self, config: Dict[str, Any] | None = None) -> Dict[str, Any]:
        self.voice_state["running"] = True
        self.voice_state["session_started_at"] = datetime.now(timezone.utc).isoformat()
        self.voice_state["config"] = config or {}
        return {"status": "success", "voice": dict(self.voice_state)}

    def stop_voice_session(self) -> Dict[str, Any]:
        self.voice_state["running"] = False
        return {"status": "success", "voice": dict(self.voice_state)}

    def trigger_voice_session(self, trigger_type: str = "manual") -> Dict[str, Any]:
        self.voice_state["last_trigger_type"] = trigger_type
        return {"status": "success", "voice": dict(self.voice_state), "trigger": {"status": "success", "queued": True}}

    def run_voice_session_continuous(
        self,
        *,
        duration_s: float = 45.0,
        max_turns: int = 3,
        stop_on_idle_s: float = 10.0,
        stop_after: bool = True,
        config: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        del config
        self.voice_state["running"] = not bool(stop_after)
        self.voice_state["transcription_count"] = int(self.voice_state.get("transcription_count", 0) or 0) + 2
        self.voice_state["last_transcript"] = "what time is it in UTC"
        self.voice_state["last_reply"] = "Current time (UTC): 2026-01-01T00:00:00+00:00."
        turns = [
            {
                "turn_index": 1,
                "trigger_type": "manual",
                "triggered_at": datetime.now(timezone.utc).isoformat(),
                "transcript": "what time is it in UTC",
                "reply": "Current time (UTC): 2026-01-01T00:00:00+00:00.",
            }
        ]
        return {
            "status": "success",
            "duration_s": float(duration_s),
            "max_turns": int(max_turns),
            "stop_on_idle_s": float(stop_on_idle_s),
            "stop_after": bool(stop_after),
            "baseline_transcriptions": 0,
            "captured_turns": len(turns),
            "turns": turns,
            "checkpoints": [
                {
                    "captured_at": datetime.now(timezone.utc).isoformat(),
                    "transcription_count": 1,
                    "last_transcript": "what time is it in UTC",
                    "last_reply": "Current time (UTC): 2026-01-01T00:00:00+00:00.",
                }
            ],
            "barge_in_enabled": True,
            "route_policy_end_reason": "",
            "route_policy_recovery_wait_s": 90.0,
            "route_policy_resume_stability_s": 0.75,
            "route_policy_pause_count": 1,
            "route_policy_resume_count": 1,
            "route_policy_pause_total_s": 3.0,
            "last_route_policy_pause_at": "2026-03-08T09:59:10+00:00",
            "last_route_policy_resume_at": "2026-03-08T09:59:13+00:00",
            "route_policy_pause_events": [
                {
                    "event_id": "voice-pause-1",
                    "paused_at": "2026-03-08T09:59:10+00:00",
                    "task": "wakeword",
                    "reason_code": "local_launch_template_blacklisted",
                    "reason": "Wakeword route in recovery.",
                    "cooldown_hint_s": 3.0,
                    "next_retry_at": "2026-03-08T09:59:13+00:00",
                    "resumed_at": "2026-03-08T09:59:13+00:00",
                    "pause_duration_s": 3.0,
                    "wakeword_supervision_status": "hybrid_polling",
                    "wakeword_supervision_reason": "mission_reliability_hybrid_polling",
                    "wakeword_supervision_strategy": "hybrid_polling",
                }
            ],
            "wakeword_supervision_snapshot": {
                "status": "hybrid_polling",
                "strategy": "hybrid_polling",
                "allow_wakeword": False,
                "next_retry_at": "2026-03-08T09:59:13+00:00",
                "reason_code": "mission_reliability_hybrid_polling",
                "reason": "Mission recovery history prefers hybrid polling.",
            },
            "end_reason": "max_turns",
            "session_id": "",
            "voice": dict(self.voice_state),
            "stop": {"status": "success", "voice": dict(self.voice_state)} if stop_after else None,
        }

    def start_voice_continuous_session(
        self,
        *,
        duration_s: float = 45.0,
        max_turns: int = 3,
        stop_on_idle_s: float = 10.0,
        stop_after: bool = True,
        config: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        session_id = f"voice-cont-{len(self.voice_continuous_runs) + 1}"
        result = self.run_voice_session_continuous(
            duration_s=duration_s,
            max_turns=max_turns,
            stop_on_idle_s=stop_on_idle_s,
            stop_after=stop_after,
            config=config,
        )
        result["session_id"] = session_id
        row = {
            "session_id": session_id,
            "status": "completed",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "result": result,
            "captured_turns": int(result.get("captured_turns", 0) or 0),
            "wakeword_supervision_snapshot": dict(result.get("wakeword_supervision_snapshot", {})),
        }
        self.voice_continuous_runs[session_id] = row
        return {"status": "success", "session_id": session_id, "session": row}

    def list_voice_continuous_runs(self, *, limit: int = 100) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 1000))
        rows = list(self.voice_continuous_runs.values())[::-1][:bounded]
        return {"status": "success", "items": rows, "count": len(rows), "total": len(self.voice_continuous_runs)}

    def get_voice_continuous_run(self, session_id: str) -> Dict[str, Any]:
        row = self.voice_continuous_runs.get(session_id)
        if not row:
            return {"status": "error", "message": "session not found", "session_id": session_id}
        return {"status": "success", "session": row}

    def cancel_voice_continuous_session(self, session_id: str, *, reason: str = "cancelled") -> Dict[str, Any]:
        row = self.voice_continuous_runs.get(session_id)
        if not row:
            return {"status": "error", "message": "session not found", "session_id": session_id}
        row["status"] = "cancelled"
        row["cancel_reason"] = reason
        row["cancel_requested_at"] = datetime.now(timezone.utc).isoformat()
        self.voice_continuous_runs[session_id] = row
        return {"status": "success", "session": row}

    def context_status(self) -> Dict[str, Any]:
        payload = dict(self.context_state)
        payload["latest_snapshot"] = {
            "active_window_title": "Fake Window",
            "active_application": "notepad",
        }
        return payload

    def start_context_monitoring(self, config: Dict[str, Any] | None = None) -> Dict[str, Any]:
        payload = config if isinstance(config, dict) else {}
        if "monitoring_interval_s" in payload:
            try:
                self.context_state["monitoring_interval_s"] = float(payload.get("monitoring_interval_s"))
            except Exception:
                pass
        if "pattern_detection_enabled" in payload:
            self.context_state["pattern_detection_enabled"] = bool(payload.get("pattern_detection_enabled"))
        if "proactive_suggestions_enabled" in payload:
            self.context_state["proactive_suggestions_enabled"] = bool(payload.get("proactive_suggestions_enabled"))
        self.context_state["running"] = True
        response = self.context_status()
        response["message"] = "Context monitoring started."
        return response

    def stop_context_monitoring(self) -> Dict[str, Any]:
        self.context_state["running"] = False
        response = self.context_status()
        response["message"] = "Context monitoring stopped."
        return response

    def context_snapshot(self) -> Dict[str, Any]:
        return {
            "status": "success",
            "has_snapshot": True,
            "snapshot": {
                "active_window_title": "Fake Window",
                "active_application": "notepad",
                "focus_mode": "normal",
            },
        }

    def context_activity_summary(self, *, duration_minutes: int = 60) -> Dict[str, Any]:
        bounded = max(1, min(int(duration_minutes), 24 * 60))
        return {
            "status": "success",
            "duration_minutes": bounded,
            "snapshot_count": 12,
            "primary_activity": "coding",
            "activity_distribution": {"coding": 8, "browsing": 4},
        }

    def context_patterns(self, *, limit: int = 20) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 200))
        items = [
            {
                "pattern_id": "pattern-1",
                "name": "morning-code-review",
                "confidence": 0.82,
                "frequency": 4,
            }
        ]
        return {
            "status": "success",
            "items": items[:bounded],
            "count": min(len(items), bounded),
            "total": len(items),
        }

    def context_opportunities(self, *, limit: int = 20) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 200))
        items = [
            {
                "opportunity_id": "opp-1",
                "opportunity_type": "automation",
                "description": "Repeatable workflow detected",
                "priority": 7,
            }
        ]
        return {
            "status": "success",
            "items": items[:bounded],
            "count": min(len(items), bounded),
            "total": len(items),
        }

    def list_context_opportunity_records(self, *, limit: int = 100) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 2000))
        rows = list(self.context_opportunity_records[-bounded:])
        return {"status": "success", "items": rows[::-1], "count": len(rows), "total": len(self.context_opportunity_records)}

    def list_context_opportunity_runs(self, *, limit: int = 100) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 2000))
        rows = list(self.context_opportunity_runs[-bounded:])
        return {"status": "success", "items": rows[::-1], "count": len(rows), "total": len(self.context_opportunity_runs)}

    def context_opportunity_policy(self) -> Dict[str, Any]:
        return {
            "status": "success",
            "autorun": bool(self.context_policy.get("autorun", False)),
            "min_priority": int(self.context_policy.get("min_priority", 7)),
            "min_confidence": float(self.context_policy.get("min_confidence", 0.75)),
            "cooldown_s": float(self.context_policy.get("cooldown_s", 60.0)),
            "max_workers": int(self.context_policy.get("max_workers", 2)),
            "priority_weight": float(self.context_policy.get("priority_weight", 1.0)),
            "confidence_weight": float(self.context_policy.get("confidence_weight", 2.0)),
            "retry_penalty": float(self.context_policy.get("retry_penalty", 0.75)),
            "aging_window_s": float(self.context_policy.get("aging_window_s", 30.0)),
            "fairness_window_s": float(self.context_policy.get("fairness_window_s", 60.0)),
            "per_type_max_in_window": int(self.context_policy.get("per_type_max_in_window", 4)),
            "per_type_max_concurrency": int(self.context_policy.get("per_type_max_concurrency", 1)),
            "class_weights": self.context_policy.get("class_weights", {}),
            "class_limits_in_window": self.context_policy.get("class_limits_in_window", {}),
            "class_max_concurrency": self.context_policy.get("class_max_concurrency", {}),
            "starvation_override_s": float(self.context_policy.get("starvation_override_s", 45.0)),
            "preflight_enabled": True,
            "preflight_max_steps": 8,
            "preflight_external_contract_enabled": bool(
                self.context_policy.get("preflight_external_contract_enabled", True)
            ),
            "preflight_external_max_checks": int(self.context_policy.get("preflight_external_max_checks", 3)),
            "verify_enabled": True,
            "verify_timeout_s": 45.0,
            "verify_max_retries": 1,
            "multiobjective_enabled": bool(self.context_policy.get("multiobjective_enabled", True)),
            "deadline_weight": float(self.context_policy.get("deadline_weight", 2.6)),
            "utility_weight": float(self.context_policy.get("utility_weight", 1.8)),
            "risk_weight": float(self.context_policy.get("risk_weight", 1.4)),
            "duration_weight": float(self.context_policy.get("duration_weight", 0.65)),
            "self_tune_enabled": bool(self.context_policy.get("self_tune_enabled", True)),
            "self_tune_alpha": float(self.context_policy.get("self_tune_alpha", 0.28)),
            "self_tune_min_samples": int(self.context_policy.get("self_tune_min_samples", 5)),
            "self_tune_bad_threshold": float(self.context_policy.get("self_tune_bad_threshold", 0.48)),
            "self_tune_good_threshold": float(self.context_policy.get("self_tune_good_threshold", 0.8)),
            "dynamic_priority_offsets_by_type": {},
            "dynamic_confidence_offsets_by_type": {},
            "dynamic_retry_penalty_by_type": {},
            "dynamic_class_weight_offsets": {},
            "dynamic_class_limit_scale_by_class": {},
            "dynamic_class_concurrency_scale_by_class": {},
            "effective_class_limits_in_window": self.context_policy.get("class_limits_in_window", {}),
            "effective_class_max_concurrency": self.context_policy.get("class_max_concurrency", {}),
            "external_pressure_enabled": bool(self.context_policy.get("external_pressure_enabled", True)),
            "external_refresh_s": float(self.context_policy.get("external_refresh_s", 15.0)),
            "external_penalty_weight": float(self.context_policy.get("external_penalty_weight", 2.8)),
            "external_penalty_weight_offset": 0.0,
            "external_recovery_boost": float(self.context_policy.get("external_recovery_boost", 1.2)),
            "external_recovery_boost_offset": 0.0,
            "external_limit_floor_scale": float(self.context_policy.get("external_limit_floor_scale", 0.35)),
            "external_concurrency_floor_scale": float(self.context_policy.get("external_concurrency_floor_scale", 0.4)),
            "external_autotune_enabled": bool(self.context_policy.get("external_autotune_enabled", True)),
            "external_autotune_alpha": float(self.context_policy.get("external_autotune_alpha", 0.24)),
            "external_autotune_bad_threshold": float(self.context_policy.get("external_autotune_bad_threshold", 0.58)),
            "external_autotune_good_threshold": float(self.context_policy.get("external_autotune_good_threshold", 0.36)),
            "external_policy_learning_state": {},
            "external_pressure_snapshot": {
                "status": "success",
                "global_pressure": 0.0,
                "mission_mode": "stable",
                "provider_count": 0,
                "class_pressure": {"external": 0.0, "recovery": 0.0, "automation": 0.0, "insight": 0.0, "other": 0.0},
                "provider_pressure_by_name": {},
            },
            "external_last_error": "",
            "learning_items": [],
            "learning_count": 0,
            "dispatch_window_counts": {},
            "dispatch_window_counts_by_class": {},
            "next_ready_in_s": 0.0,
            "tuning_state_path": "data/context_opportunity_tuning_state.json",
            "tuning_last_loaded_at": "",
            "tuning_last_saved_at": "",
            "tuning_last_save_error": "",
            "queue_depth": 0,
            "active_runs": [],
            "active_run_count": 0,
            "worker_running": True,
            "worker_count": int(self.context_policy.get("max_workers", 2)),
        }

    def update_context_opportunity_policy(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        config = payload if isinstance(payload, dict) else {}
        if "autorun" in config:
            self.context_policy["autorun"] = bool(config.get("autorun"))
        if "min_priority" in config:
            self.context_policy["min_priority"] = int(config.get("min_priority", self.context_policy["min_priority"]))
        if "min_confidence" in config:
            self.context_policy["min_confidence"] = float(config.get("min_confidence", self.context_policy["min_confidence"]))
        if "cooldown_s" in config:
            self.context_policy["cooldown_s"] = float(config.get("cooldown_s", self.context_policy["cooldown_s"]))
        for key in (
            "max_workers",
            "priority_weight",
            "confidence_weight",
            "retry_penalty",
            "aging_window_s",
            "fairness_window_s",
            "per_type_max_in_window",
            "per_type_max_concurrency",
            "class_weights",
            "class_limits_in_window",
            "class_max_concurrency",
            "starvation_override_s",
            "multiobjective_enabled",
            "deadline_weight",
            "utility_weight",
            "risk_weight",
            "duration_weight",
            "self_tune_enabled",
            "self_tune_alpha",
            "self_tune_min_samples",
            "self_tune_bad_threshold",
            "self_tune_good_threshold",
            "preflight_external_contract_enabled",
            "preflight_external_max_checks",
            "external_pressure_enabled",
            "external_refresh_s",
            "external_penalty_weight",
            "external_recovery_boost",
            "external_limit_floor_scale",
            "external_concurrency_floor_scale",
            "external_autotune_enabled",
            "external_autotune_alpha",
            "external_autotune_bad_threshold",
            "external_autotune_good_threshold",
        ):
            if key in config:
                self.context_policy[key] = config.get(key)
        payload_out = self.context_opportunity_policy()
        payload_out["message"] = "Context opportunity policy updated."
        if "preflight_enabled" in config:
            payload_out["preflight_enabled"] = bool(config.get("preflight_enabled"))
        if "preflight_max_steps" in config:
            payload_out["preflight_max_steps"] = int(config.get("preflight_max_steps"))
        if "verify_enabled" in config:
            payload_out["verify_enabled"] = bool(config.get("verify_enabled"))
        if "verify_timeout_s" in config:
            payload_out["verify_timeout_s"] = float(config.get("verify_timeout_s"))
        if "verify_max_retries" in config:
            payload_out["verify_max_retries"] = int(config.get("verify_max_retries"))
        return payload_out

    def execute_context_opportunity(
        self,
        *,
        opportunity_id: str,
        reason: str = "manual",
        force: bool = False,
        wait: bool = False,
        timeout_s: float = 20.0,
        metadata_overrides: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        del wait, timeout_s, metadata_overrides
        record = next((row for row in self.context_opportunity_records if row.get("opportunity_id") == opportunity_id), None)
        if not record:
            return {"status": "error", "message": "opportunity not found", "opportunity_id": opportunity_id}
        now_iso = datetime.now(timezone.utc).isoformat()
        run = {
            "run_id": f"context-run-{len(self.context_opportunity_runs) + 1}",
            "opportunity_id": opportunity_id,
            "opportunity_type": record.get("opportunity_type", ""),
            "status": "submitted",
            "reason": reason,
            "force": bool(force),
            "goal_id": "goal-context-1",
            "submitted_at": now_iso,
        }
        self.context_opportunity_runs.append(run)
        return {"status": "success", "run": run}

    def preview_context_opportunity_contract(
        self,
        *,
        opportunity_id: str,
        metadata_overrides: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        del metadata_overrides
        record = next((row for row in self.context_opportunity_records if row.get("opportunity_id") == opportunity_id), None)
        if not record:
            return {"status": "error", "message": "opportunity not found", "opportunity_id": opportunity_id}
        return {
            "status": "success",
            "opportunity": record,
            "goal_text": "Analyze the repeating workflow in notepad and produce a robust automation plan with verification checkpoints and rollback steps.",
            "contract": {
                "status": "success",
                "ready": True,
                "checks": {
                    "enabled": True,
                    "risk": "medium",
                    "step_count": 3,
                    "confirmation_steps": 0,
                    "high_risk_steps": 0,
                    "too_complex": False,
                    "needs_human_gate": False,
                    "max_steps": 8,
                },
                "rollback_contract": {"required": False, "verification_mode": "standard", "recovery_profile": "balanced"},
                "remediation_hints": ["No blocking preflight issues detected."],
            },
        }

    def rbac_status(self) -> Dict[str, Any]:
        return {
            "status": "success",
            "default_role": "developer",
            "source_role_defaults": {"desktop-ui": "developer", "voice-loop": "developer"},
            "mission_rbac_enabled": True,
            "mission_role_by_policy": {"automation_power": "admin"},
            "mission_role_by_risk": {"high": "developer", "medium": "developer", "low": "developer"},
            "mission_role_cache_ttl_s": 30.0,
            "mission_role_cache_count": 0,
            "mission_autonomy_adapt_enabled": True,
            "mission_autonomy_refresh_s": 12.0,
            "mission_autonomy_override_explicit": False,
            "mission_autonomy_profiles": {"high": "automation_safe", "medium": "interactive", "low": "automation_power"},
            "mission_autonomy_roles": {"high": "developer", "medium": "developer", "low": "admin"},
            "mission_autonomy_cache_count": 0,
        }

    def mission_autonomy_learning_status(self, *, limit: int = 200) -> Dict[str, Any]:
        del limit
        return {
            "status": "success",
            "enabled": True,
            "alpha": 0.28,
            "min_samples": 6,
            "bad_threshold": 0.46,
            "good_threshold": 0.82,
            "dynamic_profile_by_risk": {"low": "automation_power"},
            "dynamic_role_by_risk": {"low": "admin"},
            "items": [],
            "count": 0,
            "total": 0,
        }

    def reset_mission_autonomy_learning(self) -> Dict[str, Any]:
        return {"status": "success", "message": "Mission autonomy learning state reset."}

    def get_metrics(self) -> Dict[str, Any]:
        return {"cpuUsage": 10.0, "ramUsage": 22.0, "diskUsage": 40.0}

    def analyze_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        return {"hasCriticalIssues": False, "overallSummary": "ok", "issues": []}

    def query_memory(
        self,
        query: str = "",
        limit: int = 20,
        *,
        mode: str = "hybrid",
        status: str = "",
        source: str = "",
        min_score: float = 0.12,
        must_tags: list[str] | None = None,
        prefer_tags: list[str] | None = None,
        exclude_goal_ids: list[str] | None = None,
        diversify_by_goal: bool = True,
    ) -> Dict[str, Any]:
        if mode not in {"hybrid", "lexical", "semantic"}:
            return {"status": "error", "message": "mode must be one of: hybrid, lexical, semantic"}
        rows = [
            {
                "memory_id": "m1",
                "text": "what time is it in UTC",
                "status": "completed",
                "actions": ["time_now"],
                "source": "desktop-ui",
                "tags": ["source:desktop-ui", "status:completed", "action:time_now"],
                "memory_source": "runtime_lexical",
            }
        ]
        if query:
            rows = [item for item in rows if query.lower() in item["text"].lower()]
        if status:
            rows = [item for item in rows if str(item.get("status", "")).lower() == status.lower()]
        if source:
            rows = [item for item in rows if str(item.get("source", "")).lower() == source.lower()]
        return {
            "status": "success",
            "items": rows[:limit],
            "count": len(rows[:limit]),
            "query": query,
            "mode": mode,
            "stats": {
                "filters": {
                    "min_score": float(min_score),
                    "must_tags": must_tags or [],
                    "prefer_tags": prefer_tags or [],
                    "exclude_goal_ids": exclude_goal_ids or [],
                    "diversify_by_goal": bool(diversify_by_goal),
                }
            },
        }

    def memory_strategy(self, query: str = "", limit: int = 12, *, min_score: float = 0.08) -> Dict[str, Any]:
        return {
            "status": "success",
            "query": query,
            "sample_count": 2,
            "recommended_actions": [
                {
                    "action": "external_email_send",
                    "support": 0.71,
                    "support_count": 2,
                    "success_rate": 0.81,
                    "failure_rate": 0.12,
                }
            ],
            "avoid_actions": [
                {
                    "action": "browser_read_dom",
                    "support": 0.22,
                    "support_count": 1,
                    "success_rate": 0.21,
                    "failure_rate": 0.69,
                }
            ],
            "top_sequences": [{"sequence": ["external_connector_status", "external_email_send"], "support_count": 2}],
            "failure_patterns": [{"pattern": "timeout", "count": 1}],
            "strategy_hint": "Prefer external_email_send after connector status checks.",
            "limit": int(limit),
            "min_score": float(min_score),
        }

    def get_desktop_state_latest(self) -> Dict[str, Any]:
        if not self.state_history:
            return {"status": "empty", "count": 0}
        latest = dict(self.state_history[-1])
        latest["status"] = "success"
        latest["count"] = len(self.state_history)
        return latest

    def list_desktop_state(self, *, limit: int = 20, include_normalized: bool = False) -> Dict[str, Any]:
        rows = [dict(item) for item in self.state_history[-max(1, int(limit)) :]]
        if not include_normalized:
            for row in rows:
                row.pop("normalized", None)
        return {"status": "success", "items": rows, "count": len(rows)}

    def diff_desktop_state(self, *, from_hash: str = "", to_hash: str = "") -> Dict[str, Any]:
        return {
            "status": "success",
            "from_hash": from_hash or "hash_1",
            "to_hash": to_hash or "hash_1",
            "changed_paths": ["window.title"],
            "change_count": 1,
        }

    def desktop_anchor_memory_status(self, *, action: str = "", query: str = "", limit: int = 200) -> Dict[str, Any]:
        rows = [dict(item) for item in self.desktop_anchor_items]
        if action:
            rows = [row for row in rows if str(row.get("action", "")) == action]
        if query:
            rows = [row for row in rows if query.lower() in str(row.get("query", "")).lower()]
        quarantine_rows = [dict(item) for item in self.desktop_anchor_quarantine]
        if action:
            quarantine_rows = [row for row in quarantine_rows if str(row.get("action", "")) == action]
        if query:
            quarantine_rows = [row for row in quarantine_rows if query.lower() in str(row.get("query", "")).lower()]
        selected = rows[: max(1, int(limit))]
        return {
            "status": "success",
            "count": len(selected),
            "total": len(rows),
            "quarantine_count": len(quarantine_rows),
            "quarantine_items": quarantine_rows[:20],
            "items": selected,
        }

    def reset_desktop_anchor_memory(self, *, action: str = "", query: str = "") -> Dict[str, Any]:
        removed = 0
        if action or query:
            keep: list[Dict[str, Any]] = []
            for row in self.desktop_anchor_items:
                action_match = bool(action) and str(row.get("action", "")) == action
                query_match = bool(query) and query.lower() in str(row.get("query", "")).lower()
                if action_match or query_match:
                    removed += 1
                    continue
                keep.append(row)
            self.desktop_anchor_items = keep
        else:
            removed = len(self.desktop_anchor_items)
            self.desktop_anchor_items = []
        return {"status": "success", "removed": removed, "action": action, "query": query}

    def desktop_anchor_quarantine_status(self, *, action: str = "", query: str = "", limit: int = 200) -> Dict[str, Any]:
        rows = [dict(item) for item in self.desktop_anchor_quarantine]
        if action:
            rows = [row for row in rows if str(row.get("action", "")) == action]
        if query:
            rows = [row for row in rows if query.lower() in str(row.get("query", "")).lower()]
        selected = rows[: max(1, int(limit))]
        return {"status": "success", "count": len(selected), "total": len(rows), "items": selected}

    def reset_desktop_anchor_quarantine(self, *, key: str = "", action: str = "", query: str = "") -> Dict[str, Any]:
        removed = 0
        if key:
            keep = []
            for row in self.desktop_anchor_quarantine:
                if str(row.get("key", "")) == key:
                    removed += 1
                    continue
                keep.append(row)
            self.desktop_anchor_quarantine = keep
            return {"status": "success", "removed": removed, "key": key, "action": action, "query": query}
        if action or query:
            keep = []
            for row in self.desktop_anchor_quarantine:
                action_match = bool(action) and str(row.get("action", "")) == action
                query_match = bool(query) and query.lower() in str(row.get("query", "")).lower()
                if action_match or query_match:
                    removed += 1
                    continue
                keep.append(row)
            self.desktop_anchor_quarantine = keep
        else:
            removed = len(self.desktop_anchor_quarantine)
            self.desktop_anchor_quarantine = []
        return {"status": "success", "removed": removed, "key": key, "action": action, "query": query}

    def desktop_workflow_memory_status(
        self,
        *,
        limit: int = 200,
        action: str = "",
        app_name: str = "",
        profile_id: str = "",
        intent: str = "",
    ) -> Dict[str, Any]:
        rows = [dict(item) for item in self.desktop_workflow_items]
        if action:
            rows = [row for row in rows if str(row.get("action", "")) == action]
        if app_name:
            rows = [
                row
                for row in rows
                if app_name.lower() in str(row.get("app_name", "")).lower()
                or app_name.lower() in str(row.get("window_title", "")).lower()
            ]
        if profile_id:
            rows = [row for row in rows if str(row.get("profile_id", "")) == profile_id]
        if intent:
            rows = [row for row in rows if intent.lower() in str(row.get("intent", "")).lower()]
        selected = rows[: max(1, int(limit))]
        return {
            "status": "success",
            "count": len(selected),
            "total": len(rows),
            "items": selected,
            "summary": {"entry_count": len(rows)},
            "filters": {
                "action": action,
                "app_name": app_name,
                "profile_id": profile_id,
                "intent": intent,
            },
        }

    def reset_desktop_workflow_memory(
        self,
        *,
        action: str = "",
        app_name: str = "",
        profile_id: str = "",
        intent: str = "",
    ) -> Dict[str, Any]:
        removed = 0
        if action or app_name or profile_id or intent:
            keep: list[Dict[str, Any]] = []
            for row in self.desktop_workflow_items:
                action_match = bool(action) and str(row.get("action", "")) == action
                app_match = bool(app_name) and (
                    app_name.lower() in str(row.get("app_name", "")).lower()
                    or app_name.lower() in str(row.get("window_title", "")).lower()
                )
                profile_match = bool(profile_id) and str(row.get("profile_id", "")) == profile_id
                intent_match = bool(intent) and intent.lower() in str(row.get("intent", "")).lower()
                if action_match or app_match or profile_match or intent_match:
                    removed += 1
                    continue
                keep.append(row)
            self.desktop_workflow_items = keep
        else:
            removed = len(self.desktop_workflow_items)
            self.desktop_workflow_items = []
        return {
            "status": "success",
            "removed": removed,
            "filters": {
                "action": action,
                "app_name": app_name,
                "profile_id": profile_id,
                "intent": intent,
            },
        }

    def desktop_evaluation_catalog(
        self,
        *,
        scenario_name: str = "",
        pack: str = "",
        category: str = "",
        capability: str = "",
        risk_level: str = "",
        autonomy_tier: str = "",
        mission_family: str = "",
        app_name: str = "",
        limit: int = 200,
    ) -> Dict[str, Any]:
        rows = self._filter_desktop_evaluation_rows(
            scenario_name=scenario_name,
            pack=pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app_name=app_name,
        )
        selected = rows[: max(1, int(limit))]
        return {
            "status": "success",
            "count": len(selected),
            "items": [dict(item) for item in selected],
            "filters": {
                "scenario_name": scenario_name,
                "pack": pack,
                "category": category,
                "capability": capability,
                "risk_level": risk_level,
                "autonomy_tier": autonomy_tier,
                "mission_family": mission_family,
                "app": app_name,
                "limit": max(1, int(limit)),
            },
            "summary": self._desktop_evaluation_catalog_summary(rows),
            "latest_run": dict(self.desktop_evaluation_last_run),
        }

    def desktop_evaluation_run(
        self,
        *,
        scenario_name: str = "",
        pack: str = "",
        category: str = "",
        capability: str = "",
        risk_level: str = "",
        autonomy_tier: str = "",
        mission_family: str = "",
        app_name: str = "",
        limit: int = 200,
    ) -> Dict[str, Any]:
        rows = self._filter_desktop_evaluation_rows(
            scenario_name=scenario_name,
            pack=pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app_name=app_name,
        )[: max(1, int(limit))]
        items: list[Dict[str, Any]] = []
        total_weight = 0.0
        total_pass_weight = 0.0
        total_score_weight = 0.0
        recovery_weight = 0.0
        recovery_pass_weight = 0.0
        recovery_score_weight = 0.0
        hybrid_weight = 0.0
        hybrid_pass_weight = 0.0
        hybrid_score_weight = 0.0
        pack_scores: Dict[str, Dict[str, float]] = {}
        category_scores: Dict[str, Dict[str, float]] = {}
        capability_scores: Dict[str, Dict[str, float]] = {}
        risk_scores: Dict[str, Dict[str, float]] = {}
        autonomy_scores: Dict[str, Dict[str, float]] = {}
        mission_scores: Dict[str, Dict[str, float]] = {}
        for row in rows:
            weight = float(row.get("weight", 1.0) or 1.0)
            score = 0.91 if row.get("risk_level") != "high" else 0.82
            passed = score >= 0.8
            total_weight += weight
            total_score_weight += score * weight
            if passed:
                total_pass_weight += weight
            self._accumulate_eval_bucket(pack_scores, str(row.get("pack", "")), weight, score, passed)
            self._accumulate_eval_bucket(category_scores, str(row.get("category", "")), weight, score, passed)
            self._accumulate_eval_bucket(risk_scores, str(row.get("risk_level", "")), weight, score, passed)
            self._accumulate_eval_bucket(autonomy_scores, str(row.get("autonomy_tier", "")), weight, score, passed)
            self._accumulate_eval_bucket(mission_scores, str(row.get("mission_family", "")), weight, score, passed)
            for capability_name in row.get("capabilities", []):
                self._accumulate_eval_bucket(capability_scores, str(capability_name), weight, score, passed)
            if bool(row.get("recovery_expected", False)):
                recovery_weight += weight
                recovery_score_weight += score * weight
                if passed:
                    recovery_pass_weight += weight
            if bool(row.get("native_hybrid_focus", False)):
                hybrid_weight += weight
                hybrid_score_weight += score * weight
                if passed:
                    hybrid_pass_weight += weight
            items.append(
                {
                    "scenario": row.get("name"),
                    "category": row.get("category"),
                    "pack": row.get("pack"),
                    "platform": row.get("platform"),
                    "mission_family": row.get("mission_family"),
                    "autonomy_tier": row.get("autonomy_tier"),
                    "capabilities": list(row.get("capabilities", [])),
                    "risk_level": row.get("risk_level"),
                    "apps": list(row.get("apps", [])),
                    "recovery_expected": bool(row.get("recovery_expected", False)),
                    "native_hybrid_focus": bool(row.get("native_hybrid_focus", False)),
                    "replayable": bool(row.get("replayable", False)),
                    "horizon_steps": int(row.get("horizon_steps", 1) or 1),
                    "tags": list(row.get("tags", [])),
                    "passed": passed,
                    "expected": list(row.get("expected_actions", [])),
                    "actual": list(row.get("expected_actions", [])),
                    "score": round(score, 6),
                    "precision": round(score, 6),
                    "recall": 1.0,
                    "order_score": 0.95,
                    "required_coverage": 1.0,
                    "missing_required": [],
                    "missing_expected": [],
                    "unexpected_actions": [],
                    "weight": weight,
                    "notes": row.get("notes", ""),
                }
            )
        weighted_pass_rate = round(total_pass_weight / total_weight, 6) if total_weight else 0.0
        weighted_score = round(total_score_weight / total_weight, 6) if total_weight else 0.0
        summary = {
            "count": len(items),
            "weighted_pass_rate": weighted_pass_rate,
            "weighted_score": weighted_score,
            "top_unexpected_actions": [],
            "pack_breakdown": self._desktop_evaluation_bucket_view(pack_scores),
            "category_breakdown": self._desktop_evaluation_bucket_view(category_scores),
            "capability_coverage": self._desktop_evaluation_bucket_view(capability_scores),
            "risk_breakdown": self._desktop_evaluation_bucket_view(risk_scores),
            "autonomy_tier_breakdown": self._desktop_evaluation_bucket_view(autonomy_scores),
            "mission_family_breakdown": self._desktop_evaluation_bucket_view(mission_scores),
            "recovery_readiness": {
                "weighted_pass_rate": round(recovery_pass_weight / recovery_weight, 6) if recovery_weight else 0.0,
                "weighted_score": round(recovery_score_weight / recovery_weight, 6) if recovery_weight else 0.0,
                "weight": round(recovery_weight, 6),
            },
            "native_hybrid_coverage": {
                "weighted_pass_rate": round(hybrid_pass_weight / hybrid_weight, 6) if hybrid_weight else 0.0,
                "weighted_score": round(hybrid_score_weight / hybrid_weight, 6) if hybrid_weight else 0.0,
                "weight": round(hybrid_weight, 6),
            },
        }
        regression = {
            "status": "stable",
            "weighted_pass_rate_delta": round(weighted_pass_rate - 0.92, 6),
            "weighted_score_delta": round(weighted_score - 0.9, 6),
            "scenario_regressions": [],
            "pack_regressions": [],
            "category_regressions": [],
            "capability_regressions": [],
        }
        self.desktop_evaluation_last_run = {
            "status": "success",
            "executed_at": datetime.now(timezone.utc).isoformat(),
            "scenario_count": len(items),
            "summary": dict(summary),
            "regression": dict(regression),
        }
        self.desktop_evaluation_history_items.insert(0, dict(self.desktop_evaluation_last_run))
        self.desktop_evaluation_history_items = self.desktop_evaluation_history_items[:12]
        return {
            "status": "success",
            "items": items,
            "summary": summary,
            "regression": regression,
            "filters": {
                "scenario_name": scenario_name,
                "pack": pack,
                "category": category,
                "capability": capability,
                "risk_level": risk_level,
                "autonomy_tier": autonomy_tier,
                "mission_family": mission_family,
                "app": app_name,
                "limit": max(1, int(limit)),
            },
            "history_size": len(self.desktop_evaluation_history_items),
            "latest_run": dict(self.desktop_evaluation_last_run),
        }

    def desktop_evaluation_history(self, *, limit: int = 12) -> Dict[str, Any]:
        selected = [dict(item) for item in self.desktop_evaluation_history_items[: max(1, int(limit))]]
        return {
            "status": "success",
            "count": len(selected),
            "limit": max(1, int(limit)),
            "items": selected,
            "latest_run": dict(self.desktop_evaluation_last_run),
        }

    def desktop_evaluation_lab(
        self,
        *,
        scenario_name: str = "",
        pack: str = "",
        category: str = "",
        capability: str = "",
        risk_level: str = "",
        autonomy_tier: str = "",
        mission_family: str = "",
        app_name: str = "",
        limit: int = 200,
        history_limit: int = 8,
    ) -> Dict[str, Any]:
        rows = self._filter_desktop_evaluation_rows(
            scenario_name=scenario_name,
            pack=pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app_name=app_name,
        )[: max(1, int(limit))]
        long_horizon = [row for row in rows if int(row.get("horizon_steps", 1) or 1) >= 4]
        replay_candidates = [
            {
                "scenario": str(row.get("name", "") or ""),
                "pack": str(row.get("pack", "") or ""),
                "score": 0.82 if str(row.get("risk_level", "") or "").lower() == "high" else 0.91,
                "horizon_steps": int(row.get("horizon_steps", 1) or 1),
                "reasons": ["long_horizon"] if int(row.get("horizon_steps", 1) or 1) >= 4 else ["standard"],
                "replay_query": {"scenario_name": str(row.get("name", "") or ""), "limit": 1},
            }
            for row in long_horizon[:4]
        ]
        return {
            "status": "success",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "filters": {
                "scenario_name": scenario_name,
                "pack": pack,
                "category": category,
                "capability": capability,
                "risk_level": risk_level,
                "autonomy_tier": autonomy_tier,
                "mission_family": mission_family,
                "app": app_name,
                "limit": max(1, int(limit)),
            },
            "catalog_summary": self._desktop_evaluation_catalog_summary(rows),
            "coverage": {
                "scenario_count": len(rows),
                "replayable": {"count": sum(1 for row in rows if bool(row.get("replayable", False)))},
                "long_horizon": {
                    "count": len(long_horizon),
                    "max_horizon_steps": max((int(row.get("horizon_steps", 1) or 1) for row in rows), default=0),
                },
            },
            "history_trend": {
                "run_count": min(len(self.desktop_evaluation_history_items), max(1, int(history_limit))),
                "direction": "improving",
                "weighted_score_delta": 0.07,
                "weighted_pass_rate_delta": 0.05,
                "regression_run_count": 1,
            },
            "latest_run": dict(self.desktop_evaluation_last_run),
            "latest_summary": dict(self.desktop_evaluation_last_run.get("summary", {})),
            "latest_regression": dict(self.desktop_evaluation_last_run.get("regression", {})),
            "replay_candidates": replay_candidates,
            "installed_app_coverage": {
                "status": "success",
                "installed_profile_count": 4,
                "benchmarked_installed_app_count": 3,
                "benchmarked_ratio": 0.75,
                "covered_apps": ["Settings", "Installer", "Visual Studio Code"],
                "missing_apps": ["Outlook"],
                "missing_category_counts": {"communication": 1},
            },
            "history_size": len(self.desktop_evaluation_history_items),
        }

    def desktop_evaluation_lab_sessions(
        self,
        *,
        limit: int = 12,
        session_id: str = "",
        status: str = "",
    ) -> Dict[str, Any]:
        selected = [dict(item) for item in self.desktop_evaluation_lab_sessions_items]
        if str(session_id or "").strip():
            selected = [item for item in selected if str(item.get("session_id", "") or "").strip() == str(session_id or "").strip()]
        if str(status or "").strip():
            selected = [
                item
                for item in selected
                if str(item.get("status", "") or "").strip().lower() == str(status or "").strip().lower()
            ]
        selected = selected[: max(1, int(limit))]
        pending_replays = 0
        failed_replays = 0
        completed_replays = 0
        for item in self.desktop_evaluation_lab_sessions_items:
            pending_replays += int(item.get("pending_replay_count", 0) or 0)
            failed_replays += int(item.get("failed_replay_count", 0) or 0)
            completed_replays += int(item.get("completed_replay_count", 0) or 0)
        return {
            "status": "success",
            "count": len(selected),
            "total": len(self.desktop_evaluation_lab_sessions_items),
            "limit": max(1, int(limit)),
            "items": selected,
            "latest_session": dict(selected[0]) if selected else {},
            "summary": {
                "pending_replays": pending_replays,
                "failed_replays": failed_replays,
                "completed_replays": completed_replays,
            },
        }

    def desktop_evaluation_create_lab_session(
        self,
        *,
        scenario_name: str = "",
        pack: str = "",
        category: str = "",
        capability: str = "",
        risk_level: str = "",
        autonomy_tier: str = "",
        mission_family: str = "",
        app_name: str = "",
        limit: int = 200,
        history_limit: int = 8,
        source: str = "",
        label: str = "",
    ) -> Dict[str, Any]:
        lab = self.desktop_evaluation_lab(
            scenario_name=scenario_name,
            pack=pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app_name=app_name,
            limit=limit,
            history_limit=history_limit,
        )
        native_targets = self.desktop_evaluation_native_targets(
            scenario_name=scenario_name,
            pack=pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app_name=app_name,
            limit=limit,
            history_limit=history_limit,
        )
        guidance = self.desktop_evaluation_guidance()
        session_id = f"benchlab-test-{len(self.desktop_evaluation_lab_sessions_items) + 1}"
        replay_candidates = [
            {
                **dict(item),
                "replay_status": "pending",
                "replay_count": 0,
            }
            for item in lab.get("replay_candidates", [])[:6]
            if isinstance(item, dict)
        ]
        session = {
            "session_id": session_id,
            "status": "ready",
            "label": str(label or pack or category or app_name or "benchmark lab session"),
            "source": str(source or "action_control_panel"),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "focus_summary": list(native_targets.get("focus_summary", [])),
            "replay_candidates": replay_candidates,
            "replay_candidate_count": len(replay_candidates),
            "pending_replay_count": len(replay_candidates),
            "failed_replay_count": 0,
            "completed_replay_count": 0,
            "target_app_count": len(native_targets.get("target_apps", [])),
            "target_apps": [str(item.get("app_name", "") or "") for item in native_targets.get("target_apps", []) if isinstance(item, dict)],
            "strongest_tactics": dict(native_targets.get("strongest_tactics", {})),
            "coverage_gap_apps": list(native_targets.get("coverage_gap_apps", [])),
            "history_direction": str(dict(lab.get("history_trend", {})).get("direction", "") or ""),
            "latest_weighted_score": float(dict(lab.get("latest_summary", {})).get("weighted_score", 0.0) or 0.0),
            "latest_weighted_pass_rate": float(dict(lab.get("latest_summary", {})).get("weighted_pass_rate", 0.0) or 0.0),
            "filters": dict(lab.get("filters", {})),
            "catalog_summary": dict(lab.get("catalog_summary", {})),
            "coverage": dict(lab.get("coverage", {})),
            "history_trend": dict(lab.get("history_trend", {})),
            "lab_snapshot": dict(lab),
            "native_targets_snapshot": dict(native_targets),
            "guidance_snapshot": dict(guidance),
        }
        self.desktop_evaluation_lab_sessions_items.insert(0, session)
        self.desktop_evaluation_lab_sessions_items = self.desktop_evaluation_lab_sessions_items[:12]
        return {
            "status": "success",
            "session": dict(session),
            "lab": dict(lab),
            "native_targets": dict(native_targets),
            "guidance": dict(guidance),
        }

    def desktop_evaluation_replay_lab_session(
        self,
        *,
        session_id: str = "",
        scenario_name: str = "",
    ) -> Dict[str, Any]:
        selected = next(
            (
                item
                for item in self.desktop_evaluation_lab_sessions_items
                if str(item.get("session_id", "") or "").strip() == str(session_id or "").strip()
            ),
            None,
        )
        if not isinstance(selected, dict):
            return {"status": "error", "message": "benchmark lab session not found"}
        candidates = [
            dict(item)
            for item in selected.get("replay_candidates", [])
            if isinstance(item, dict)
        ]
        chosen = next(
            (
                item
                for item in candidates
                if str(item.get("scenario", "") or "").strip() == str(scenario_name or "").strip()
            ),
            candidates[0] if candidates else None,
        )
        if not isinstance(chosen, dict):
            return {"status": "error", "message": "benchmark lab session has no replay candidates"}
        replay_query = dict(chosen.get("replay_query", {})) if isinstance(chosen.get("replay_query", {}), dict) else {}
        replay_result = self.desktop_evaluation_run(
            scenario_name=str(replay_query.get("scenario_name", chosen.get("scenario", "")) or ""),
            pack=str(replay_query.get("pack", "") or ""),
            category=str(replay_query.get("category", "") or ""),
            capability=str(replay_query.get("capability", "") or ""),
            risk_level=str(replay_query.get("risk_level", "") or ""),
            autonomy_tier=str(replay_query.get("autonomy_tier", "") or ""),
            mission_family=str(replay_query.get("mission_family", "") or ""),
            app_name=str(replay_query.get("app", replay_query.get("app_name", "")) or ""),
            limit=int(replay_query.get("limit", 1) or 1),
        )
        updated_candidates: list[Dict[str, Any]] = []
        for item in candidates:
            updated = dict(item)
            if str(updated.get("scenario", "") or "").strip() == str(chosen.get("scenario", "") or "").strip():
                updated["replay_status"] = "completed"
                updated["replay_count"] = int(updated.get("replay_count", 0) or 0) + 1
                updated["last_replayed_at"] = datetime.now(timezone.utc).isoformat()
            updated_candidates.append(updated)
        selected["replay_candidates"] = updated_candidates
        selected["pending_replay_count"] = sum(
            1
            for item in updated_candidates
            if str(item.get("replay_status", "pending") or "pending").strip().lower() == "pending"
        )
        selected["completed_replay_count"] = sum(
            1
            for item in updated_candidates
            if str(item.get("replay_status", "") or "").strip().lower() == "completed"
        )
        selected["failed_replay_count"] = 0
        selected["updated_at"] = datetime.now(timezone.utc).isoformat()
        selected["latest_weighted_score"] = float(dict(replay_result.get("summary", {})).get("weighted_score", 0.0) or 0.0)
        selected["latest_weighted_pass_rate"] = float(dict(replay_result.get("summary", {})).get("weighted_pass_rate", 0.0) or 0.0)
        selected["status"] = "complete" if int(selected.get("pending_replay_count", 0) or 0) == 0 else "ready"
        refreshed_lab = self.desktop_evaluation_lab(
            pack=str(dict(selected.get("filters", {})).get("pack", "") or ""),
            category=str(dict(selected.get("filters", {})).get("category", "") or ""),
            capability=str(dict(selected.get("filters", {})).get("capability", "") or ""),
            risk_level=str(dict(selected.get("filters", {})).get("risk_level", "") or ""),
            autonomy_tier=str(dict(selected.get("filters", {})).get("autonomy_tier", "") or ""),
            mission_family=str(dict(selected.get("filters", {})).get("mission_family", "") or ""),
            app_name=str(dict(selected.get("filters", {})).get("app", "") or ""),
            limit=int(dict(selected.get("filters", {})).get("limit", 200) or 200),
            history_limit=8,
        )
        native_targets = self.desktop_evaluation_native_targets()
        guidance = self.desktop_evaluation_guidance()
        selected["lab_snapshot"] = dict(refreshed_lab)
        selected["native_targets_snapshot"] = dict(native_targets)
        selected["guidance_snapshot"] = dict(guidance)
        return {
            "status": "success",
            "session": dict(selected),
            "replay_candidate": dict(chosen),
            "updated_candidate": next(
                (
                    dict(item)
                    for item in updated_candidates
                    if str(item.get("scenario", "") or "").strip() == str(chosen.get("scenario", "") or "").strip()
                ),
                {},
            ),
            "replay_result": dict(replay_result),
            "lab": dict(refreshed_lab),
            "native_targets": dict(native_targets),
            "guidance": dict(guidance),
        }

    def desktop_evaluation_guidance(self) -> Dict[str, Any]:
        return {
            "status": "success",
            "benchmark_ready": True,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "latest_run_executed_at": str(self.desktop_evaluation_last_run.get("executed_at", "") or ""),
            "latest_weighted_pass_rate": 0.78,
            "latest_weighted_score": 0.76,
            "weakest_pack": "unsupported_and_recovery",
            "weakest_category": "unsupported_app",
            "weakest_capability": "surface_exploration",
            "weakest_mission_family": "exploration",
            "focus_summary": ["unsupported_and_recovery", "surface_exploration", "exploration"],
            "control_biases": {
                "dialog_resolution": 0.82,
                "descendant_focus": 0.74,
                "navigation_branch": 0.41,
                "recovery_reacquire": 0.79,
                "loop_guard": 0.52,
                "native_focus": 0.77,
            },
            "recovery_focus": {"target": "recovery_readiness", "weighted_score": 0.74},
            "native_hybrid_focus": {"target": "native_hybrid_coverage", "weighted_score": 0.71},
            "improvement_candidates": {
                "packs": [{"name": "unsupported_and_recovery", "weighted_score": 0.74}],
            },
            "history_size": len(self.desktop_evaluation_history_items),
        }

    def desktop_evaluation_native_targets(
        self,
        *,
        scenario_name: str = "",
        pack: str = "",
        category: str = "",
        capability: str = "",
        risk_level: str = "",
        autonomy_tier: str = "",
        mission_family: str = "",
        app_name: str = "",
        limit: int = 200,
        history_limit: int = 8,
    ) -> Dict[str, Any]:
        del scenario_name, category, capability, risk_level, autonomy_tier, mission_family, history_limit
        return {
            "status": "success",
            "benchmark_ready": True,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "filters": {
                "pack": pack,
                "app": app_name,
                "limit": max(1, int(limit)),
            },
            "focus_summary": ["unsupported_and_recovery", "surface_exploration"],
            "target_apps": [
                {
                    "app_name": "settings",
                    "priority": 2.4,
                    "scenario_names": ["unsupported_child_dialog_chain"],
                    "query_hints": ["pair device", "confirm pairing"],
                    "mission_families": ["exploration"],
                    "max_horizon_steps": 5,
                    "control_biases": {
                        "dialog_resolution": 0.82,
                        "descendant_focus": 0.91,
                        "navigation_branch": 0.35,
                        "recovery_reacquire": 0.78,
                        "loop_guard": 0.42,
                        "native_focus": 0.88,
                    },
                }
            ],
            "target_app_biases": {
                "settings": {
                    "dialog_resolution": 0.82,
                    "descendant_focus": 0.91,
                    "navigation_branch": 0.35,
                    "recovery_reacquire": 0.78,
                    "loop_guard": 0.42,
                    "native_focus": 0.88,
                }
            },
            "replay_candidates": [
                {
                    "scenario": "unsupported_child_dialog_chain",
                    "score": 0.74,
                    "replay_query": {"scenario_name": "unsupported_child_dialog_chain", "limit": 1},
                }
            ],
            "strongest_tactics": {
                "dialog_resolution": 0.82,
                "descendant_focus": 0.91,
                "navigation_branch": 0.35,
                "recovery_reacquire": 0.78,
                "loop_guard": 0.42,
                "native_focus": 0.88,
            },
            "coverage_gap_apps": ["Outlook"],
            "installed_app_coverage": {
                "status": "success",
                "installed_profile_count": 4,
                "benchmarked_installed_app_count": 3,
                "benchmarked_ratio": 0.75,
            },
            "history_trend": {
                "run_count": 2,
                "direction": "improving",
                "weighted_score_delta": 0.06,
            },
        }

    def _filter_desktop_evaluation_rows(
        self,
        *,
        scenario_name: str = "",
        pack: str = "",
        category: str = "",
        capability: str = "",
        risk_level: str = "",
        autonomy_tier: str = "",
        mission_family: str = "",
        app_name: str = "",
    ) -> list[Dict[str, Any]]:
        rows = [dict(item) for item in self.desktop_evaluation_items]
        if scenario_name:
            rows = [row for row in rows if str(row.get("name", "")).strip().lower() == scenario_name.lower()]
        if pack:
            rows = [row for row in rows if str(row.get("pack", "")).strip().lower() == pack.lower()]
        if category:
            rows = [row for row in rows if str(row.get("category", "")).strip().lower() == category.lower()]
        if capability:
            rows = [
                row
                for row in rows
                if capability.lower() in [str(value).strip().lower() for value in row.get("capabilities", [])]
            ]
        if risk_level:
            rows = [row for row in rows if str(row.get("risk_level", "")).strip().lower() == risk_level.lower()]
        if autonomy_tier:
            rows = [
                row for row in rows if str(row.get("autonomy_tier", "")).strip().lower() == autonomy_tier.lower()
            ]
        if mission_family:
            rows = [
                row for row in rows if str(row.get("mission_family", "")).strip().lower() == mission_family.lower()
            ]
        if app_name:
            rows = [
                row
                for row in rows
                if app_name.lower() in [str(value).strip().lower() for value in row.get("apps", [])]
            ]
        return rows

    def _desktop_evaluation_catalog_summary(self, rows: list[Dict[str, Any]]) -> Dict[str, Any]:
        pack_counts: Dict[str, int] = {}
        category_counts: Dict[str, int] = {}
        capability_counts: Dict[str, int] = {}
        risk_counts: Dict[str, int] = {}
        autonomy_counts: Dict[str, int] = {}
        mission_counts: Dict[str, int] = {}
        app_counts: Dict[str, int] = {}
        recovery_expected_count = 0
        native_hybrid_focus_count = 0
        replayable_count = 0
        long_horizon_count = 0
        max_horizon_steps = 0
        for row in rows:
            self._increment_eval_count(pack_counts, str(row.get("pack", "")))
            self._increment_eval_count(category_counts, str(row.get("category", "")))
            self._increment_eval_count(risk_counts, str(row.get("risk_level", "")))
            self._increment_eval_count(autonomy_counts, str(row.get("autonomy_tier", "")))
            self._increment_eval_count(mission_counts, str(row.get("mission_family", "")))
            if bool(row.get("recovery_expected", False)):
                recovery_expected_count += 1
            if bool(row.get("native_hybrid_focus", False)):
                native_hybrid_focus_count += 1
            if bool(row.get("replayable", False)):
                replayable_count += 1
            horizon_steps = int(row.get("horizon_steps", 1) or 1)
            max_horizon_steps = max(max_horizon_steps, horizon_steps)
            if horizon_steps >= 4:
                long_horizon_count += 1
            for capability_name in row.get("capabilities", []):
                self._increment_eval_count(capability_counts, str(capability_name))
            for app in row.get("apps", []):
                self._increment_eval_count(app_counts, str(app))
        return {
            "scenario_count": len(rows),
            "pack_counts": dict(sorted(pack_counts.items())),
            "category_counts": dict(sorted(category_counts.items())),
            "capability_counts": dict(sorted(capability_counts.items())),
            "risk_counts": dict(sorted(risk_counts.items())),
            "autonomy_tier_counts": dict(sorted(autonomy_counts.items())),
            "mission_family_counts": dict(sorted(mission_counts.items())),
            "app_counts": dict(sorted(app_counts.items())),
            "recovery_expected_count": recovery_expected_count,
            "native_hybrid_focus_count": native_hybrid_focus_count,
            "replayable_count": replayable_count,
            "long_horizon_count": long_horizon_count,
            "max_horizon_steps": max_horizon_steps,
        }

    @staticmethod
    def _increment_eval_count(target: Dict[str, int], key: str) -> None:
        clean = " ".join(str(key or "").strip().lower().split())
        if not clean:
            return
        target[clean] = int(target.get(clean, 0)) + 1

    @staticmethod
    def _accumulate_eval_bucket(
        target: Dict[str, Dict[str, float]],
        key: str,
        weight: float,
        score: float,
        passed: bool,
    ) -> None:
        clean = " ".join(str(key or "").strip().split())
        if not clean:
            return
        bucket = target.setdefault(clean, {"weight": 0.0, "score_weight": 0.0, "pass_weight": 0.0})
        bucket["weight"] += weight
        bucket["score_weight"] += weight * score
        if passed:
            bucket["pass_weight"] += weight

    @staticmethod
    def _desktop_evaluation_bucket_view(source: Dict[str, Dict[str, float]]) -> list[Dict[str, Any]]:
        ordered = sorted(source.items(), key=lambda item: (-float(item[1]["weight"]), item[0]))
        rows: list[Dict[str, Any]] = []
        for name, bucket in ordered:
            weight = float(bucket.get("weight", 0.0) or 0.0)
            if weight <= 0.0:
                continue
            rows.append(
                {
                    "name": name,
                    "weighted_pass_rate": round(float(bucket.get("pass_weight", 0.0) or 0.0) / weight, 6),
                    "weighted_score": round(float(bucket.get("score_weight", 0.0) or 0.0) / weight, 6),
                    "weight": round(weight, 6),
                }
            )
        return rows

    def desktop_mission_status(
        self,
        *,
        limit: int = 200,
        mission_id: str = "",
        status: str = "",
        mission_kind: str = "",
        app_name: str = "",
        stop_reason_code: str = "",
    ) -> Dict[str, Any]:
        rows = [dict(item) for item in self.desktop_mission_items]
        if mission_id:
            rows = [row for row in rows if str(row.get("mission_id", "")).strip() == mission_id]
        if status:
            rows = [row for row in rows if str(row.get("status", "")).strip().lower() == status.lower()]
        if mission_kind:
            rows = [row for row in rows if str(row.get("mission_kind", "")).strip().lower() == mission_kind.lower()]
        if app_name:
            rows = [
                row
                for row in rows
                if app_name.lower() in str(row.get("app_name", "")).lower()
                or app_name.lower() in str(row.get("anchor_window_title", "")).lower()
                or app_name.lower() in str(row.get("blocking_window_title", "")).lower()
            ]
        if stop_reason_code:
            rows = [
                row
                for row in rows
                if str(row.get("stop_reason_code", "")).strip().lower() == stop_reason_code.lower()
            ]

        def recovery_profile_for(row: Dict[str, Any]) -> Dict[str, Any]:
            status_value = str(row.get("status", "")).strip().lower()
            approval_kind = str(row.get("approval_kind", "")).strip().lower()
            dialog_kind = str(row.get("dialog_kind", "")).strip().lower()
            stop_reason_value = str(row.get("stop_reason_code", "")).strip().lower()
            secure_desktop_likely = bool(
                isinstance(row.get("blocking_surface", {}), dict)
                and row["blocking_surface"].get("secure_desktop_likely", False)
            )
            if status_value == "completed":
                return {
                    "recovery_profile": "completed",
                    "recovery_hint": "This desktop mission is already complete.",
                    "recovery_priority": 5,
                    "resume_ready": False,
                    "manual_attention_required": False,
                    "approval_blocked": False,
                }
            if status_value == "error":
                return {
                    "recovery_profile": "failed_retry",
                    "recovery_hint": "Inspect the last failure and validate the target surface before retrying.",
                    "recovery_priority": 35,
                    "resume_ready": False,
                    "manual_attention_required": True,
                    "approval_blocked": False,
                }
            if approval_kind in {"elevation_consent", "elevation_credentials"} or secure_desktop_likely:
                return {
                    "recovery_profile": "admin_review",
                    "recovery_hint": "Administrator approval is still likely required before JARVIS can continue.",
                    "recovery_priority": 70,
                    "resume_ready": False,
                    "manual_attention_required": True,
                    "approval_blocked": True,
                }
            if approval_kind == "credential_input":
                return {
                    "recovery_profile": "credential_review",
                    "recovery_hint": "Credentials are likely required before this mission can resume.",
                    "recovery_priority": 74,
                    "resume_ready": False,
                    "manual_attention_required": True,
                    "approval_blocked": True,
                }
            if approval_kind == "permission_review":
                return {
                    "recovery_profile": "permission_review",
                    "recovery_hint": "Review the permission surface, then let JARVIS resume the mission.",
                    "recovery_priority": 78,
                    "resume_ready": False,
                    "manual_attention_required": True,
                    "approval_blocked": True,
                }
            if any(marker in dialog_kind or marker in stop_reason_value for marker in ("review", "warning", "destructive", "confirm")):
                return {
                    "recovery_profile": "operator_review",
                    "recovery_hint": "An operator review surface is likely still in the way of autonomous progress.",
                    "recovery_priority": 72,
                    "resume_ready": False,
                    "manual_attention_required": True,
                    "approval_blocked": True,
                }
            if status_value in {"paused", "resuming"}:
                return {
                    "recovery_profile": "resume_ready",
                    "recovery_hint": "This mission looks ready for a resume attempt.",
                    "recovery_priority": 90 if status_value == "paused" else 82,
                    "resume_ready": True,
                    "manual_attention_required": False,
                    "approval_blocked": False,
                }
            return {
                "recovery_profile": status_value or "unknown",
                "recovery_hint": "Inspect the stored desktop mission before resuming it.",
                "recovery_priority": 15,
                "resume_ready": False,
                "manual_attention_required": False,
                "approval_blocked": False,
            }

        def app_bucket_for(row: Dict[str, Any]) -> str:
            for candidate in (
                row.get("app_name", ""),
                row.get("anchor_window_title", ""),
                row.get("blocking_window_title", ""),
            ):
                normalized = " ".join(str(candidate or "").strip().lower().split())
                if normalized:
                    return normalized
            return ""

        enriched_rows: list[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            enriched_rows.append({**row, **recovery_profile_for(row)})

        selected = enriched_rows[: max(1, int(limit))]
        status_counts: Dict[str, int] = {}
        mission_kind_counts: Dict[str, int] = {}
        approval_kind_counts: Dict[str, int] = {}
        recovery_profile_counts: Dict[str, int] = {}
        app_counts: Dict[str, int] = {}
        stop_reason_counts: Dict[str, int] = {}
        resume_ready_count = 0
        manual_attention_count = 0
        latest_paused: Dict[str, Any] | None = None
        for row in enriched_rows:
            if not isinstance(row, dict):
                continue
            current_status = str(row.get("status", "")).strip().lower()
            if current_status:
                status_counts[current_status] = int(status_counts.get(current_status, 0)) + 1
            current_kind = str(row.get("mission_kind", "")).strip().lower()
            if current_kind:
                mission_kind_counts[current_kind] = int(mission_kind_counts.get(current_kind, 0)) + 1
            approval_kind = str(row.get("approval_kind", "")).strip().lower()
            if approval_kind:
                approval_kind_counts[approval_kind] = int(approval_kind_counts.get(approval_kind, 0)) + 1
            recovery_profile = str(row.get("recovery_profile", "")).strip().lower()
            if recovery_profile:
                recovery_profile_counts[recovery_profile] = int(recovery_profile_counts.get(recovery_profile, 0)) + 1
            if bool(row.get("resume_ready", False)):
                resume_ready_count += 1
            if bool(row.get("manual_attention_required", False)):
                manual_attention_count += 1
            app_key = app_bucket_for(row)
            if app_key:
                app_counts[app_key] = int(app_counts.get(app_key, 0)) + 1
            reason_key = str(row.get("stop_reason_code", "")).strip().lower()
            if reason_key:
                stop_reason_counts[reason_key] = int(stop_reason_counts.get(reason_key, 0)) + 1
            if latest_paused is None and current_status in {"paused", "resuming"}:
                latest_paused = dict(row)
        return {
            "status": "success",
            "count": len(selected),
            "total": len(enriched_rows),
            "items": selected,
            "status_counts": status_counts,
            "mission_kind_counts": mission_kind_counts,
            "approval_kind_counts": approval_kind_counts,
            "recovery_profile_counts": recovery_profile_counts,
            "app_counts": app_counts,
            "stop_reason_counts": stop_reason_counts,
            "resume_ready_count": resume_ready_count,
            "manual_attention_count": manual_attention_count,
            "latest_paused": latest_paused,
            "filters": {
                "mission_id": mission_id,
                "status": status,
                "mission_kind": mission_kind,
                "app_name": app_name,
                "stop_reason_code": stop_reason_code,
            },
        }

    def reset_desktop_missions(
        self,
        *,
        mission_id: str = "",
        status: str = "",
        mission_kind: str = "",
        app_name: str = "",
    ) -> Dict[str, Any]:
        removed = 0
        if mission_id:
            keep: list[Dict[str, Any]] = []
            for row in self.desktop_mission_items:
                if str(row.get("mission_id", "")).strip() == mission_id:
                    removed += 1
                    continue
                keep.append(row)
            self.desktop_mission_items = keep
        elif status or mission_kind or app_name:
            keep = []
            for row in self.desktop_mission_items:
                status_match = bool(status) and str(row.get("status", "")).strip().lower() == status.lower()
                kind_match = bool(mission_kind) and str(row.get("mission_kind", "")).strip().lower() == mission_kind.lower()
                app_match = bool(app_name) and app_name.lower() in str(row.get("app_name", "")).lower()
                if status_match or kind_match or app_match:
                    removed += 1
                    continue
                keep.append(row)
            self.desktop_mission_items = keep
        else:
            removed = len(self.desktop_mission_items)
            self.desktop_mission_items = []
        return {
            "status": "success",
            "removed": removed,
            "filters": {
                "mission_id": mission_id,
                "status": status,
                "mission_kind": mission_kind,
                "app_name": app_name,
            },
        }

    def desktop_recovery_watchdog_history(
        self,
        *,
        limit: int = 20,
        status: str = "",
        source: str = "",
        app_name: str = "",
        mission_kind: str = "",
    ) -> Dict[str, Any]:
        rows = [dict(item) for item in self.desktop_recovery_watchdog_runs]
        clean_status = str(status or "").strip().lower()
        clean_source = str(source or "").strip().lower()
        clean_app = str(app_name or "").strip().lower()
        clean_kind = str(mission_kind or "").strip().lower()
        if clean_status:
            rows = [row for row in rows if str(row.get("status", "") or "").strip().lower() == clean_status]
        if clean_source:
            rows = [row for row in rows if str(row.get("source", "") or "").strip().lower() == clean_source]
        if clean_app:
            rows = [row for row in rows if str(row.get("app_name", "") or "").strip().lower() == clean_app]
        if clean_kind:
            rows = [row for row in rows if str(row.get("mission_kind", "") or "").strip().lower() == clean_kind]
        rows.sort(key=lambda row: str(row.get("updated_at", "") or ""), reverse=True)
        items = rows[: max(1, int(limit or 20))]
        return {
            "status": "success",
            "count": len(items),
            "total": len(rows),
            "items": items,
            "status_counts": {
                key: len([row for row in rows if str(row.get("status", "") or "").strip().lower() == key])
                for key in {str(row.get("status", "") or "").strip().lower() for row in rows if str(row.get("status", "") or "").strip()}
            },
            "source_counts": {
                key: len([row for row in rows if str(row.get("source", "") or "").strip().lower() == key])
                for key in {str(row.get("source", "") or "").strip().lower() for row in rows if str(row.get("source", "") or "").strip()}
            },
            "app_counts": {
                key: len([row for row in rows if str(row.get("app_name", "") or "").strip() == key])
                for key in {str(row.get("app_name", "") or "").strip() for row in rows if str(row.get("app_name", "") or "").strip()}
            },
            "mission_kind_counts": {
                key: len([row for row in rows if str(row.get("mission_kind", "") or "").strip().lower() == key])
                for key in {str(row.get("mission_kind", "") or "").strip().lower() for row in rows if str(row.get("mission_kind", "") or "").strip()}
            },
            "triggered_run_count": len([row for row in rows if int(row.get("auto_resume_triggered_count", 0) or 0) > 0]),
            "blocked_run_count": len([row for row in rows if int(row.get("blocked_count", 0) or 0) > 0]),
            "error_run_count": len([row for row in rows if int(row.get("error_count", 0) or 0) > 0]),
            "latest_run": items[0] if items else None,
            "latest_triggered_run": next((row for row in items if int(row.get("auto_resume_triggered_count", 0) or 0) > 0), None),
            "latest_blocked_run": next((row for row in items if int(row.get("blocked_count", 0) or 0) > 0), None),
            "latest_error_run": next((row for row in items if int(row.get("error_count", 0) or 0) > 0), None),
            "filters": {
                "status": clean_status,
                "source": clean_source,
                "app_name": clean_app,
                "mission_kind": clean_kind,
            },
        }

    def reset_desktop_recovery_watchdog_history(
        self,
        *,
        run_id: str = "",
        status: str = "",
        source: str = "",
        app_name: str = "",
        mission_kind: str = "",
    ) -> Dict[str, Any]:
        clean_id = str(run_id or "").strip()
        clean_status = str(status or "").strip().lower()
        clean_source = str(source or "").strip().lower()
        clean_app = str(app_name or "").strip().lower()
        clean_kind = str(mission_kind or "").strip().lower()
        removed = 0
        if clean_id:
            before = len(self.desktop_recovery_watchdog_runs)
            self.desktop_recovery_watchdog_runs = [
                row for row in self.desktop_recovery_watchdog_runs if str(row.get("run_id", "") or "").strip() != clean_id
            ]
            removed = before - len(self.desktop_recovery_watchdog_runs)
        else:
            keep: list[Dict[str, Any]] = []
            for row in self.desktop_recovery_watchdog_runs:
                should_remove = True
                if clean_status:
                    should_remove = should_remove and str(row.get("status", "") or "").strip().lower() == clean_status
                if clean_source:
                    should_remove = should_remove and str(row.get("source", "") or "").strip().lower() == clean_source
                if clean_app:
                    should_remove = should_remove and str(row.get("app_name", "") or "").strip().lower() == clean_app
                if clean_kind:
                    should_remove = should_remove and str(row.get("mission_kind", "") or "").strip().lower() == clean_kind
                if not any([clean_status, clean_source, clean_app, clean_kind]):
                    should_remove = True
                if should_remove:
                    removed += 1
                    continue
                keep.append(row)
            self.desktop_recovery_watchdog_runs = keep
        return {
            "status": "success",
            "removed": removed,
            "filters": {
                "run_id": clean_id,
                "status": clean_status,
                "source": clean_source,
                "app_name": clean_app,
                "mission_kind": clean_kind,
            },
        }

    def desktop_recovery_supervisor_status(self, *, history_limit: int = 6) -> Dict[str, Any]:
        payload = dict(self.desktop_recovery_supervisor_state)
        payload["status"] = "success"
        payload["snapshot"] = self.desktop_mission_status(
            limit=int(payload.get("limit", 12) or 12),
            status=str(payload.get("mission_status", "") or "").strip(),
            mission_kind=str(payload.get("mission_kind", "") or "").strip(),
            app_name=str(payload.get("app_name", "") or "").strip(),
            stop_reason_code=str(payload.get("stop_reason_code", "") or "").strip(),
        )
        payload["watchdog_history"] = self.desktop_recovery_watchdog_history(
            limit=max(1, int(history_limit or 6)),
            app_name=str(payload.get("app_name", "") or "").strip(),
            mission_kind=str(payload.get("mission_kind", "") or "").strip(),
        )
        return payload

    @staticmethod
    def _desktop_recovery_policy_defaults(profile: str) -> Dict[str, bool]:
        clean = str(profile or "").strip().lower()
        if clean == "conservative":
            return {
                "allow_high_risk": False,
                "allow_critical_risk": False,
                "allow_admin_clearance": False,
                "allow_destructive": False,
            }
        if clean == "power":
            return {
                "allow_high_risk": True,
                "allow_critical_risk": True,
                "allow_admin_clearance": False,
                "allow_destructive": False,
            }
        return {
            "allow_high_risk": True,
            "allow_critical_risk": False,
            "allow_admin_clearance": False,
            "allow_destructive": False,
        }

    def configure_desktop_recovery_supervisor(
        self,
        *,
        enabled: bool | None = None,
        interval_s: float | None = None,
        limit: int | None = None,
        max_auto_resumes: int | None = None,
        policy_profile: str | None = None,
        allow_high_risk: bool | None = None,
        allow_critical_risk: bool | None = None,
        allow_admin_clearance: bool | None = None,
        allow_destructive: bool | None = None,
        mission_status: str | None = None,
        mission_kind: str | None = None,
        app_name: str | None = None,
        stop_reason_code: str | None = None,
        resume_force: bool | None = None,
        history_limit: int = 6,
    ) -> Dict[str, Any]:
        state = self.desktop_recovery_supervisor_state
        if enabled is not None:
            state["enabled"] = bool(enabled)
        if interval_s is not None:
            state["interval_s"] = float(interval_s)
        if limit is not None:
            state["limit"] = int(limit)
        if max_auto_resumes is not None:
            state["max_auto_resumes"] = int(max_auto_resumes)
        if policy_profile is not None:
            clean_profile = str(policy_profile or "").strip().lower() or "balanced"
            if clean_profile not in {"conservative", "balanced", "power", "custom"}:
                clean_profile = "balanced"
            state["policy_profile"] = clean_profile
            if clean_profile != "custom":
                state.update(self._desktop_recovery_policy_defaults(clean_profile))
        if allow_high_risk is not None:
            state["allow_high_risk"] = bool(allow_high_risk)
            state["policy_profile"] = "custom"
        if allow_critical_risk is not None:
            state["allow_critical_risk"] = bool(allow_critical_risk)
            state["policy_profile"] = "custom"
        if allow_admin_clearance is not None:
            state["allow_admin_clearance"] = bool(allow_admin_clearance)
            state["policy_profile"] = "custom"
        if allow_destructive is not None:
            state["allow_destructive"] = bool(allow_destructive)
            state["policy_profile"] = "custom"
        if mission_status is not None:
            state["mission_status"] = str(mission_status or "").strip()
        if mission_kind is not None:
            state["mission_kind"] = str(mission_kind or "").strip()
        if app_name is not None:
            state["app_name"] = str(app_name or "").strip()
        if stop_reason_code is not None:
            state["stop_reason_code"] = str(stop_reason_code or "").strip()
        if resume_force is not None:
            state["resume_force"] = bool(resume_force)
        state["last_config_source"] = "api"
        state["updated_at"] = "2026-03-15T10:30:00+00:00"
        return self.desktop_recovery_supervisor_status(history_limit=history_limit)

    def trigger_desktop_recovery_supervisor(
        self,
        *,
        limit: int | None = None,
        max_auto_resumes: int | None = None,
        policy_profile: str | None = None,
        allow_high_risk: bool | None = None,
        allow_critical_risk: bool | None = None,
        allow_admin_clearance: bool | None = None,
        allow_destructive: bool | None = None,
        mission_status: str | None = None,
        mission_kind: str | None = None,
        app_name: str | None = None,
        stop_reason_code: str | None = None,
        resume_force: bool | None = None,
        history_limit: int = 6,
    ) -> Dict[str, Any]:
        state = self.desktop_recovery_supervisor_state
        effective_policy_profile = str(
            policy_profile if policy_profile is not None else state.get("policy_profile", "") or "balanced"
        ).strip().lower() or "balanced"
        if effective_policy_profile not in {"conservative", "balanced", "power", "custom"}:
            effective_policy_profile = "balanced"
        defaults = self._desktop_recovery_policy_defaults(effective_policy_profile)
        effective_allow_high_risk = bool(defaults["allow_high_risk"] if allow_high_risk is None else allow_high_risk)
        effective_allow_critical_risk = bool(
            defaults["allow_critical_risk"] if allow_critical_risk is None else allow_critical_risk
        )
        effective_allow_admin_clearance = bool(
            defaults["allow_admin_clearance"] if allow_admin_clearance is None else allow_admin_clearance
        )
        effective_allow_destructive = bool(
            defaults["allow_destructive"] if allow_destructive is None else allow_destructive
        )
        if any(value is not None for value in (allow_high_risk, allow_critical_risk, allow_admin_clearance, allow_destructive)):
            effective_policy_profile = "custom"
        snapshot = self.desktop_mission_status(
            limit=int(limit or state.get("limit", 12) or 12),
            status=str(mission_status if mission_status is not None else state.get("mission_status", "") or "").strip(),
            mission_kind=str(mission_kind if mission_kind is not None else state.get("mission_kind", "") or "").strip(),
            app_name=str(app_name if app_name is not None else state.get("app_name", "") or "").strip(),
            stop_reason_code=str(
                stop_reason_code if stop_reason_code is not None else state.get("stop_reason_code", "") or ""
            ).strip(),
        )
        ready_items = [
            item for item in snapshot.get("items", [])
            if isinstance(item, dict) and bool(item.get("resume_ready", False))
        ] if isinstance(snapshot.get("items", []), list) else []
        bounded_limit = max(0, int(max_auto_resumes if max_auto_resumes is not None else state.get("max_auto_resumes", 2) or 2))
        triggered_items = ready_items[:bounded_limit]
        for item in triggered_items:
            mission_id = str(item.get("mission_id", "") or "").strip()
            for row in self.desktop_mission_items:
                if str(row.get("mission_id", "") or "").strip() != mission_id:
                    continue
                row["status"] = "completed"
                row["latest_result_status"] = "success"
                row["latest_result_message"] = "Desktop recovery daemon resumed this mission."
                row["completed_at"] = "2026-03-15T10:32:00+00:00"
                row["updated_at"] = "2026-03-15T10:32:00+00:00"
                row["resume_attempts"] = int(row.get("resume_attempts", 0) or 0) + 1
                break
        state["run_count"] = int(state.get("run_count", 0) or 0) + 1
        state["manual_trigger_count"] = int(state.get("manual_trigger_count", 0) or 0) + 1
        state["last_trigger_source"] = "manual_api"
        state["last_trigger_at"] = "2026-03-15T10:32:00+00:00"
        state["last_tick_at"] = "2026-03-15T10:32:00+00:00"
        state["last_success_at"] = "2026-03-15T10:32:00+00:00"
        state["last_result_status"] = "success" if triggered_items else "idle"
        state["last_result_message"] = (
            f"Desktop recovery daemon resumed {len(triggered_items)} paused mission"
            f"{'' if len(triggered_items) == 1 else 's'}."
            if triggered_items
            else "Desktop recovery daemon did not find resumable paused missions."
        )
        state["last_summary"] = {
            "status": state["last_result_status"],
            "auto_resume_triggered_count": len(triggered_items),
            "resume_ready_count": len(ready_items),
            "blocked_count": int(snapshot.get("manual_attention_count", 0) or 0),
            "policy_blocked_count": 0,
            "stop_reason": "auto_resume_triggered" if triggered_items else "desktop_recovery_idle",
        }
        state["updated_at"] = "2026-03-15T10:32:00+00:00"
        self.desktop_recovery_watchdog_runs.insert(
            0,
            {
                "run_id": f"desktop_watchdog_{len(self.desktop_recovery_watchdog_runs) + 1}",
                "status": state["last_result_status"],
                "message": state["last_result_message"],
                "source": "manual_api",
                "trigger_source": "manual_api",
                "limit": int(limit or state.get("limit", 12) or 12),
                "max_auto_resumes": bounded_limit,
                "policy_profile": effective_policy_profile,
                "allow_high_risk": effective_allow_high_risk,
                "allow_critical_risk": effective_allow_critical_risk,
                "allow_admin_clearance": effective_allow_admin_clearance,
                "allow_destructive": effective_allow_destructive,
                "mission_status": str(mission_status if mission_status is not None else state.get("mission_status", "") or "").strip(),
                "mission_kind": str(mission_kind if mission_kind is not None else state.get("mission_kind", "") or "").strip(),
                "app_name": str(app_name if app_name is not None else state.get("app_name", "") or "").strip(),
                "stop_reason_code": str(
                    stop_reason_code if stop_reason_code is not None else state.get("stop_reason_code", "") or ""
                ).strip(),
                "resume_force": bool(resume_force if resume_force is not None else state.get("resume_force", False)),
                "evaluated_count": int(snapshot.get("count", 0) or 0),
                "auto_resume_attempted_count": len(triggered_items),
                "auto_resume_triggered_count": len(triggered_items),
                "resume_ready_count": max(0, len(ready_items) - len(triggered_items)),
                "manual_attention_count": int(snapshot.get("manual_attention_count", 0) or 0),
                "blocked_count": int(snapshot.get("manual_attention_count", 0) or 0),
                "policy_blocked_count": 0,
                "idle_count": 0,
                "error_count": 0,
                "stop_reason": "auto_resume_triggered" if triggered_items else "desktop_recovery_idle",
                "triggered_mission_ids": [str(item.get("mission_id", "") or "").strip() for item in triggered_items],
                "ready_mission_ids": [
                    str(item.get("mission_id", "") or "").strip() for item in ready_items[len(triggered_items):]
                ],
                "blocked_mission_ids": [
                    str(item.get("mission_id", "") or "").strip()
                    for item in snapshot.get("items", [])
                    if isinstance(item, dict) and bool(item.get("manual_attention_required", False))
                ],
                "latest_triggered_mission_id": str(triggered_items[0].get("mission_id", "") or "").strip() if triggered_items else "",
                "filters": {
                    "status": str(mission_status if mission_status is not None else state.get("mission_status", "") or "").strip(),
                    "mission_kind": str(mission_kind if mission_kind is not None else state.get("mission_kind", "") or "").strip(),
                    "app_name": str(app_name if app_name is not None else state.get("app_name", "") or "").strip(),
                    "stop_reason_code": str(
                        stop_reason_code if stop_reason_code is not None else state.get("stop_reason_code", "") or ""
                    ).strip(),
                },
                "created_at": "2026-03-15T10:32:00+00:00",
                "updated_at": "2026-03-15T10:32:00+00:00",
            },
        )
        return {
            "status": state["last_result_status"],
            "message": state["last_result_message"],
            "result": {
                "status": state["last_result_status"],
                "message": state["last_result_message"],
                "auto_resume_triggered_count": len(triggered_items),
                "resume_ready_count": len(ready_items),
                "blocked_count": int(snapshot.get("manual_attention_count", 0) or 0),
                "policy_profile": effective_policy_profile,
                "policy_blocked_count": 0,
                "evaluated_count": int(snapshot.get("count", 0) or 0),
                "triggered_mission_ids": [str(item.get("mission_id", "") or "").strip() for item in triggered_items],
                "stop_reason": "auto_resume_triggered" if triggered_items else "desktop_recovery_idle",
            },
            "supervisor": self.desktop_recovery_supervisor_status(history_limit=history_limit),
        }

    def desktop_action_advice(
        self,
        *,
        action: str = "",
        app_name: str = "",
        window_title: str = "",
        query: str = "",
        text: str = "",
        mission_id: str = "",
        mission_kind: str = "",
        candidate_id: str = "",
        branch_action: str = "",
        keys: list[str] | None = None,
        ensure_app_launch: bool | None = None,
        focus_first: bool | None = None,
        press_enter: bool | None = None,
        verify_after_action: bool | None = None,
        verify_text: str = "",
        retry_on_verification_failure: bool | None = None,
        max_strategy_attempts: int | None = None,
        exploration_limit: int | None = None,
        max_exploration_steps: int | None = None,
        max_descendant_chain_steps: int | None = None,
        max_branch_family_switches: int | None = None,
        max_branch_cascade_steps: int | None = None,
        max_wizard_pages: int | None = None,
        allow_warning_pages: bool | None = None,
        max_form_pages: int | None = None,
        allow_destructive_forms: bool | None = None,
        attempted_targets: list[Dict[str, Any]] | None = None,
        surface_signature_history: list[str] | None = None,
        branch_history: list[Dict[str, Any]] | None = None,
        resume_contract: Dict[str, Any] | None = None,
        blocking_surface: Dict[str, Any] | None = None,
        resume_force: bool | None = None,
    ) -> Dict[str, Any]:
        return {
            "status": "success",
            "action": action,
            "app_name": app_name,
            "window_title": window_title,
            "query": query,
            "text": text,
            "mission_id": mission_id,
            "mission_kind": mission_kind,
            "candidate_id": candidate_id,
            "branch_action": branch_action,
            "keys": list(keys or []),
            "ensure_app_launch": ensure_app_launch,
            "focus_first": focus_first,
            "press_enter": press_enter,
            "verify_after_action": verify_after_action,
            "verify_text": verify_text,
            "retry_on_verification_failure": retry_on_verification_failure,
            "max_strategy_attempts": max_strategy_attempts,
            "exploration_limit": exploration_limit,
            "max_exploration_steps": max_exploration_steps,
            "max_descendant_chain_steps": max_descendant_chain_steps,
            "max_branch_family_switches": max_branch_family_switches,
            "max_branch_cascade_steps": max_branch_cascade_steps,
            "max_wizard_pages": max_wizard_pages,
            "allow_warning_pages": allow_warning_pages,
            "max_form_pages": max_form_pages,
            "allow_destructive_forms": allow_destructive_forms,
            "attempted_targets": [dict(item) for item in attempted_targets or []],
            "surface_signature_history": [str(item) for item in surface_signature_history or []],
            "branch_history": [dict(item) for item in branch_history or []],
            "resume_contract": dict(resume_contract or {}),
            "blocking_surface": dict(blocking_surface or {}),
            "resume_force": resume_force,
        }

    def desktop_interact(
        self,
        *,
        action: str = "",
        app_name: str = "",
        window_title: str = "",
        query: str = "",
        text: str = "",
        mission_id: str = "",
        mission_kind: str = "",
        candidate_id: str = "",
        branch_action: str = "",
        keys: list[str] | None = None,
        ensure_app_launch: bool | None = None,
        focus_first: bool | None = None,
        press_enter: bool | None = None,
        verify_after_action: bool | None = None,
        verify_text: str = "",
        retry_on_verification_failure: bool | None = None,
        max_strategy_attempts: int | None = None,
        exploration_limit: int | None = None,
        max_exploration_steps: int | None = None,
        max_descendant_chain_steps: int | None = None,
        max_branch_family_switches: int | None = None,
        max_branch_cascade_steps: int | None = None,
        max_wizard_pages: int | None = None,
        allow_warning_pages: bool | None = None,
        max_form_pages: int | None = None,
        allow_destructive_forms: bool | None = None,
        attempted_targets: list[Dict[str, Any]] | None = None,
        surface_signature_history: list[str] | None = None,
        branch_history: list[Dict[str, Any]] | None = None,
        resume_contract: Dict[str, Any] | None = None,
        blocking_surface: Dict[str, Any] | None = None,
        resume_force: bool | None = None,
        approval_id: str = "",
    ) -> Dict[str, Any]:
        return {
            "status": "success",
            "action": action,
            "app_name": app_name,
            "window_title": window_title,
            "query": query,
            "text": text,
            "mission_id": mission_id,
            "mission_kind": mission_kind,
            "candidate_id": candidate_id,
            "branch_action": branch_action,
            "keys": list(keys or []),
            "ensure_app_launch": ensure_app_launch,
            "focus_first": focus_first,
            "press_enter": press_enter,
            "verify_after_action": verify_after_action,
            "verify_text": verify_text,
            "retry_on_verification_failure": retry_on_verification_failure,
            "max_strategy_attempts": max_strategy_attempts,
            "exploration_limit": exploration_limit,
            "max_exploration_steps": max_exploration_steps,
            "max_descendant_chain_steps": max_descendant_chain_steps,
            "max_branch_family_switches": max_branch_family_switches,
            "max_branch_cascade_steps": max_branch_cascade_steps,
            "max_wizard_pages": max_wizard_pages,
            "allow_warning_pages": allow_warning_pages,
            "max_form_pages": max_form_pages,
            "allow_destructive_forms": allow_destructive_forms,
            "attempted_targets": [dict(item) for item in attempted_targets or []],
            "surface_signature_history": [str(item) for item in surface_signature_history or []],
            "branch_history": [dict(item) for item in branch_history or []],
            "resume_contract": dict(resume_contract or {}),
            "blocking_surface": dict(blocking_surface or {}),
            "resume_force": resume_force,
            "approval_id": approval_id,
        }

    def desktop_workflows(
        self,
        *,
        query: str = "",
        category: str = "",
        app_name: str = "",
        window_title: str = "",
        limit: int = 200,
    ) -> Dict[str, Any]:
        rows = [dict(item) for item in self.desktop_workflow_catalog_items]
        if category:
            rows = [
                row
                for row in rows
                if category.lower() in [str(value).lower() for value in row.get("category_hints", [])]
            ]
        if query:
            rows = [
                row
                for row in rows
                if query.lower() in str(row.get("action", "")).lower()
                or query.lower() in str(row.get("title", "")).lower()
            ]
        selected = rows[: max(1, int(limit))]
        profile: Dict[str, Any] = {}
        if app_name.lower() in {"chrome", "google chrome"}:
            profile = {"status": "success", "category": "browser", "name": "Google Chrome"}
        elif app_name.lower() in {"vscode", "visual studio code"}:
            profile = {"status": "success", "category": "code_editor", "name": "Microsoft Visual Studio Code"}
        return {
            "status": "success",
            "count": len(selected),
            "total": len(rows),
            "items": selected,
            "profile": profile,
            "filters": {
                "query": query,
                "category": category,
                "app_name": app_name,
                "window_title": window_title,
            },
        }

    def desktop_surface_snapshot(
        self,
        *,
        app_name: str = "",
        window_title: str = "",
        query: str = "",
        limit: int = 24,
        include_observation: bool = True,
        include_elements: bool = True,
        include_workflow_probes: bool = True,
    ) -> Dict[str, Any]:
        clean_app = app_name.strip().lower()
        profile: Dict[str, Any] = {}
        workflow_surfaces: list[Dict[str, Any]] = []
        elements: list[Dict[str, Any]] = []
        observation: Dict[str, Any] = {}
        surface_flags: Dict[str, bool] = {}
        recommended_actions: list[str] = []

        if clean_app in {"chrome", "google chrome"}:
            profile = {"status": "success", "category": "browser", "name": "Google Chrome"}
            workflow_surfaces = [
                {
                    "action": "open_history",
                    "title": "Open History",
                    "supported": True,
                    "primary_hotkey": ["ctrl", "h"],
                    "matched": True,
                    "match_count": 1,
                    "matches": [{"query": "history", "match_source": "accessibility"}],
                    "recommended_followups": ["navigate", "open_bookmarks"],
                },
                {
                    "action": "open_bookmarks",
                    "title": "Open Bookmarks",
                    "supported": True,
                    "primary_hotkey": ["ctrl", "shift", "o"],
                    "matched": "bookmark" in query.lower(),
                    "match_count": 1 if "bookmark" in query.lower() else 0,
                    "matches": [{"query": "bookmarks", "match_source": "accessibility"}] if "bookmark" in query.lower() else [],
                    "recommended_followups": ["navigate", "search"],
                },
            ]
            elements = [{"name": "History", "control_type": "Document"}, {"name": "Bookmarks", "control_type": "Link"}][: max(1, int(limit))]
            observation = {
                "status": "success" if include_observation else "skipped",
                "screen_hash": "chrome-surface",
                "text": "History Bookmarks Recent Tabs" if include_observation else "",
                "screenshot_path": "E:/tmp/chrome-surface.png" if include_observation else "",
            }
            surface_flags = {
                "window_targeted": True,
                "window_active": True,
                "history_visible": True,
                "bookmarks_visible": "bookmark" in query.lower(),
            }
            recommended_actions = ["navigate", "search", "open_bookmarks"]
        elif clean_app in {"explorer", "file explorer", "windows explorer"}:
            profile = {"status": "success", "category": "file_manager", "name": "File Explorer"}
            workflow_surfaces = [
                {
                    "action": "focus_address_bar",
                    "title": "Focus Address Bar",
                    "supported": True,
                    "primary_hotkey": ["ctrl", "l"],
                    "matched": True,
                    "match_count": 1,
                    "matches": [{"query": "address", "match_source": "accessibility"}],
                    "recommended_followups": ["navigate", "new_folder"],
                },
                {
                    "action": "new_folder",
                    "title": "New Folder",
                    "supported": True,
                    "primary_hotkey": ["ctrl", "shift", "n"],
                    "matched": False,
                    "match_count": 0,
                    "matches": [],
                    "recommended_followups": ["refresh_view"],
                },
            ]
            elements = [{"name": "Address", "control_type": "Edit"}, {"name": "Documents", "control_type": "ListItem"}][: max(1, int(limit))]
            observation = {
                "status": "success" if include_observation else "skipped",
                "screen_hash": "explorer-surface",
                "text": "Address Documents Downloads" if include_observation else "",
                "screenshot_path": "E:/tmp/explorer-surface.png" if include_observation else "",
            }
            surface_flags = {
                "window_targeted": True,
                "window_active": True,
                "address_bar_ready": True,
                "file_manager_ready": True,
            }
            recommended_actions = ["navigate", "new_folder", "search"]

        if not include_elements:
            elements = []
        if not include_workflow_probes:
            workflow_surfaces = []
            recommended_actions = []
        return {
            "status": "success",
            "app_profile": profile,
            "profile_defaults_applied": {},
            "capabilities": {
                "accessibility": {"available": True},
                "vision": {"available": True},
            },
            "active_window": {"title": window_title or profile.get("name", "")},
            "target_window": {"title": window_title or profile.get("name", "")},
            "candidate_windows": [{"title": window_title or profile.get("name", ""), "hwnd": 1}] if profile else [],
            "elements": {"status": "success" if include_elements else "skipped", "count": len(elements), "items": elements},
            "query_targets": elements[:1] if include_elements else [],
            "query_related_candidates": elements[1:2] if include_elements else [],
            "selection_candidates": elements[:2] if include_elements else [],
            "control_inventory": {
                str(item.get("control_type", "") or "unknown").strip().lower(): sum(
                    1
                    for row in elements
                    if str(row.get("control_type", "") or "unknown").strip().lower()
                    == str(item.get("control_type", "") or "unknown").strip().lower()
                )
                for item in elements
                if isinstance(item, dict)
            },
            "target_control_state": elements[0] if include_elements and elements else {},
            "target_group_state": {
                "group_role": "list_group" if clean_app in {"explorer", "file explorer", "windows explorer"} else "generic_options",
                "option_count": len(elements),
                "options": elements[:2],
            } if include_elements else {},
            "observation": observation if include_observation else {"status": "skipped", "screen_hash": "", "text": "", "screenshot_path": ""},
            "workflow_surfaces": workflow_surfaces,
            "surface_flags": surface_flags,
            "recommended_actions": recommended_actions,
            "filters": {
                "app_name": app_name,
                "window_title": window_title,
                "query": query,
                "limit": limit,
                "include_observation": include_observation,
                "include_elements": include_elements,
                "include_workflow_probes": include_workflow_probes,
            },
        }

    def desktop_surface_exploration(
        self,
        *,
        app_name: str = "",
        window_title: str = "",
        query: str = "",
        limit: int = 8,
        include_observation: bool = True,
        include_elements: bool = True,
        include_workflow_probes: bool = True,
    ) -> Dict[str, Any]:
        snapshot = self.desktop_surface_snapshot(
            app_name=app_name,
            window_title=window_title,
            query=query,
            limit=max(12, int(limit or 8) * 2),
            include_observation=include_observation,
            include_elements=include_elements,
            include_workflow_probes=include_workflow_probes,
        )
        elements = snapshot.get("elements", {}).get("items", []) if isinstance(snapshot.get("elements", {}), dict) else []
        top_target = {}
        if isinstance(elements, list):
            normalized_query = str(query or "").strip().lower()
            for row in elements:
                if not isinstance(row, dict):
                    continue
                if normalized_query and normalized_query in str(row.get("name", "") or "").strip().lower():
                    top_target = dict(row)
                    break
            if not top_target and elements:
                top_target = dict(elements[0]) if isinstance(elements[0], dict) else {}
        target_label = str(top_target.get("name", "") or query or app_name or "surface target").strip()
        suggested_action = "select_list_item" if str(top_target.get("control_type", "") or "").strip().lower() == "listitem" else "click"
        return {
            "status": "success",
            "profile_name": str(snapshot.get("app_profile", {}).get("name", "") or "").strip(),
            "category": str(snapshot.get("app_profile", {}).get("category", "") or "").strip(),
            "surface_mode": "list_navigation" if suggested_action == "select_list_item" else "generic_surface",
            "automation_ready": True,
            "manual_attention_required": False,
            "attention_signals": [],
            "hypothesis_count": 1 if top_target else 0,
            "branch_action_count": len(snapshot.get("recommended_actions", [])) if isinstance(snapshot.get("recommended_actions", []), list) else 0,
            "top_hypotheses": [
                {
                    "candidate_id": str(top_target.get("element_id", "") or "candidate_1"),
                    "label": target_label,
                    "control_type": str(top_target.get("control_type", "") or "").strip(),
                    "source": "query_target",
                    "surface_mode": "list_navigation" if suggested_action == "select_list_item" else "generic_surface",
                    "score": 0.91,
                    "confidence": 0.91,
                    "query_match_score": 1.0 if query else 0.75,
                    "suggested_action": suggested_action,
                    "action_payload": {
                        "action": suggested_action,
                        "app_name": app_name,
                        "window_title": window_title or str(snapshot.get("target_window", {}).get("title", "") or "").strip(),
                        "query": target_label,
                    },
                    "recommended_path": [
                        {
                            "action": suggested_action,
                            "args": {
                                "action": suggested_action,
                                "query": target_label,
                            },
                            "phase": "recon_action",
                            "optional": False,
                            "reason": "Act on the surfaced control target.",
                        }
                    ],
                    "state_tags": ["enabled", "visible"],
                    "already_active": False,
                    "manual_attention_required": False,
                    "reason": f"{target_label} directly matched the requested surface query.",
                    "candidate_state": top_target,
                }
            ] if top_target else [],
            "branch_actions": [
                {
                    "action": str(action_name or "").strip(),
                    "title": str(action_name or "").strip().replace("_", " ").title(),
                    "matched": False,
                    "supported": True,
                    "confidence": 0.7,
                    "reason": "The current surface recommends this next action.",
                    "action_payload": {"action": str(action_name or "").strip(), "app_name": app_name},
                    "recommended_followups": [],
                }
                for action_name in (snapshot.get("recommended_actions", []) if isinstance(snapshot.get("recommended_actions", []), list) else [])[: max(1, int(limit or 8))]
            ],
            "top_path": [
                {
                    "action": suggested_action,
                    "args": {"action": suggested_action, "query": target_label},
                    "phase": "recon_action",
                    "optional": False,
                    "reason": "Act on the surfaced control target.",
                }
            ] if top_target else [],
            "surface_snapshot": snapshot,
            "filters": {
                "app_name": app_name,
                "window_title": window_title,
                "query": query,
                "limit": limit,
            },
            "message": f"Top target: {target_label} via {suggested_action}.",
        }


@pytest.fixture()
def api_server() -> tuple[str, FakeDesktopService]:
    service = FakeDesktopService()
    server = JarvisHTTPServer(("127.0.0.1", 0), JarvisAPIHandler, service)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    host, port = server.server_address
    base_url = f"http://{host}:{port}"
    try:
        yield base_url, service
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_get_health_and_tools(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, health = request_json("GET", f"{base_url}/health")
    assert status == 200
    assert health["status"] == "ok"

    status, tools = request_json("GET", f"{base_url}/tools")
    assert status == 200
    assert "time_now" in tools


def test_model_connector_get_routes(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, models_payload = request_json(
        "GET",
        f"{base_url}/models",
    )
    assert status == 200
    assert models_payload["runtime_supervisors"]["status"] == "success"

    status, operations = request_json(
        "GET",
        f"{base_url}/models/operations?stack_name=voice&preferred_model_name=qwen&requires_offline=1&privacy_mode=1&mission_profile=privacy&runtime_limit=5&limit_per_task=3",
    )
    assert status == 200
    assert operations["status"] == "success"
    assert operations["stack_name"] == "voice"
    assert operations["requires_offline"] is True
    assert operations["privacy_mode"] is True
    assert operations["runtime_supervisors"]["reasoning"]["candidate_count"] >= 1
    assert operations["route_bundle"]["stack_name"] == "voice"
    assert operations["connector_diagnostics"]["route_plan"]["preferred_provider"] == "local"
    assert operations["connector_diagnostics"]["voice_route_policy"]["mission_id"] == "voice-mission-1"

    status, supervisors = request_json(
        "GET",
        f"{base_url}/models/runtime-supervisors?preferred_model_name=qwen&limit=3",
    )
    assert status == 200
    assert supervisors["status"] == "success"
    assert supervisors["preferred_model_name"] == "qwen"
    assert supervisors["reasoning"]["candidate_count"] >= 1
    assert supervisors["reasoning"]["bridge"]["ready"] is True
    assert supervisors["vision"]["loaded_count"] >= 1

    status, reasoning_bridge = request_json(
        "GET",
        f"{base_url}/models/runtime-supervisors/reasoning/bridge?probe=1",
    )
    assert status == 200
    assert reasoning_bridge["status"] == "success"
    assert reasoning_bridge["ready"] is True
    assert int(reasoning_bridge.get("probe_attempts", 0)) >= 1

    status, local_inventory = request_json(
        "GET",
        f"{base_url}/models/local-inventory?task=reasoning&limit=12",
    )
    assert status == 200
    assert local_inventory["status"] == "success"
    assert local_inventory["inventory"]["task"] == "reasoning"
    assert int(local_inventory["inventory"].get("count", 0)) >= 1
    assert int(local_inventory.get("launch_summary", {}).get("ready_profile_count", 0)) >= 1

    status, bridge_profiles = request_json(
        "GET",
        f"{base_url}/models/bridge-profiles?task=reasoning&limit=8",
    )
    assert status == 200
    assert bridge_profiles["status"] == "success"
    assert int(bridge_profiles.get("count", 0)) >= 1
    assert bridge_profiles["profiles"][0]["bridge_kind"] == "reasoning"
    assert bridge_profiles["cloud_readiness"]["groq"] is True
    assert int(bridge_profiles["profiles"][0].get("launch_ready_count", 0)) >= 1
    assert isinstance(bridge_profiles.get("launch_health_summary", {}), dict)
    assert len(bridge_profiles["profiles"][0].get("launch_templates", [])) >= 1

    status, tts_bridge_profiles = request_json(
        "GET",
        f"{base_url}/models/bridge-profiles?task=tts&limit=8",
    )
    assert status == 200
    assert tts_bridge_profiles["status"] == "success"
    assert int(tts_bridge_profiles.get("count", 0)) >= 1
    assert tts_bridge_profiles["profiles"][0]["bridge_kind"] == "tts"
    assert tts_bridge_profiles["profiles"][0]["apply_supported"] is True
    assert int(tts_bridge_profiles["profiles"][0].get("launch_ready_count", 0)) >= 1
    assert len(tts_bridge_profiles["profiles"][0].get("launch_templates", [])) >= 1

    status, stt_bridge_profiles = request_json(
        "GET",
        f"{base_url}/models/bridge-profiles?task=stt&limit=8",
    )
    assert status == 200
    assert stt_bridge_profiles["status"] == "success"
    assert int(stt_bridge_profiles.get("count", 0)) >= 1
    assert stt_bridge_profiles["profiles"][0]["bridge_kind"] == "stt"
    assert stt_bridge_profiles["profiles"][0]["apply_supported"] is True
    assert stt_bridge_profiles["stt_runtime_profile"]["task"] == "stt"

    status, vision_bridge_profiles = request_json(
        "GET",
        f"{base_url}/models/bridge-profiles?task=vision&limit=8",
    )
    assert status == 200
    assert vision_bridge_profiles["status"] == "success"
    assert int(vision_bridge_profiles.get("count", 0)) >= 1
    assert vision_bridge_profiles["profiles"][0]["bridge_kind"] == "vision"
    assert int(vision_bridge_profiles["profiles"][0].get("launch_ready_count", 0)) >= 1
    assert vision_bridge_profiles["vision_runtime_profile"]["task"] == "vision"

    status, capabilities = request_json(
        "GET",
        f"{base_url}/models/capabilities?limit_per_task=3",
    )
    assert status == 200
    assert capabilities["status"] == "success"
    assert int(capabilities.get("task_count", 0)) >= 1

    status, bundle = request_json(
        "GET",
        f"{base_url}/models/route-bundle?stack_name=voice&privacy_mode=1&requires_offline=1",
    )
    assert status == 200
    assert bundle["status"] == "success"
    assert bundle["stack_name"] == "voice"
    assert "stt" in bundle.get("selected_local_paths", {})
    assert int(bundle.get("launch_policy_summary", {}).get("policy_monitored_task_count", 0)) >= 2
    stt_route = next(item for item in bundle.get("items", []) if str(item.get("task", "")) == "stt")
    assert stt_route.get("route_policy", {}).get("matched") is True
    assert stt_route.get("route_policy", {}).get("local_route_viable") is True

    status, diagnostics = request_json(
        "GET",
        f"{base_url}/models/connectors/diagnostics?include_route_plan=1&privacy_mode=1&mission_profile=automation_safe",
    )
    assert status == 200
    assert diagnostics["status"] == "success"
    assert isinstance(diagnostics.get("policy", {}), dict)
    route_plan = diagnostics.get("route_plan", {})
    assert route_plan.get("preferred_provider") == "local"


def test_model_setup_workspace_routes(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, service = api_server
    manifest_path = "E:/scopes/demo/JARVIS_BACKEND/Models_manifest.txt"
    workspace_root = "E:/scopes/demo"

    status, workspace = request_json(
        "GET",
        f"{base_url}/models/setup/workspace?refresh_provider_credentials=1&limit=24&manifest_path={manifest_path}&workspace_root={workspace_root}",
    )
    assert status == 200
    assert workspace["status"] == "success"
    assert workspace["workspace_root"] == workspace_root
    assert workspace["manifest_path"] == manifest_path
    assert int(workspace["summary"]["missing_directory_count"]) == 1
    assert int(workspace["summary"]["missing_required_provider_count"]) == 1
    assert len(workspace["directory_actions"]) == 1
    assert any(
        call["route"] == "model_setup_workspace"
        and call["manifest_path"] == manifest_path
        and call["workspace_root"] == workspace_root
        for call in service.model_setup_scope_calls
    )

    status, preview = request_json(
        "POST",
        f"{base_url}/models/setup/workspace/scaffold",
        {"dry_run": True, "limit": 24, "manifest_path": manifest_path, "workspace_root": workspace_root},
    )
    assert status == 200
    assert preview["status"] == "success"
    assert preview["dry_run"] is True
    assert int(preview["action_count"]) == 1
    assert preview["actions"][0]["status"] == "planned"
    assert preview["workspace"]["manifest_path"] == manifest_path

    status, applied = request_json(
        "POST",
        f"{base_url}/models/setup/workspace/scaffold",
        {"dry_run": False, "limit": 24, "manifest_path": manifest_path, "workspace_root": workspace_root},
    )
    assert status == 200
    assert applied["status"] == "success"
    assert applied["dry_run"] is False
    assert int(applied["created_count"]) == 1
    assert applied["actions"][0]["status"] == "created"
    assert applied["workspace"]["workspace_root"] == workspace_root


def test_model_setup_mission_routes(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, service = api_server
    manifest_path = "E:/scopes/mission/JARVIS_BACKEND/Models_manifest.txt"
    workspace_root = "E:/scopes/mission"

    status, mission = request_json(
        "GET",
        f"{base_url}/models/setup/mission?refresh_provider_credentials=1&limit=24&manifest_path={manifest_path}&workspace_root={workspace_root}",
    )
    assert status == 200
    assert mission["status"] == "success"
    assert mission["mission_status"] == "ready"
    assert int(mission["summary"]["ready_action_count"]) == 2
    assert len(mission["actions"]) >= 2
    assert mission["stored_mission"]["mission_id"] == "msm_demo_scope"
    assert mission["stored_mission"]["manifest_path"] == manifest_path
    assert mission["stored_mission"]["workspace_root"] == workspace_root
    assert mission["mission_history"]["manual_attention_count"] == 1
    assert mission["resume_advice"]["status"] == "blocked"
    assert mission["resume_advice"]["resume_blockers"] == ["provider_credentials"]
    assert any(
        call["route"] == "model_setup_mission"
        and call["manifest_path"] == manifest_path
        and call["workspace_root"] == workspace_root
        for call in service.model_setup_scope_calls
    )

    status, history = request_json(
        "GET",
        f"{base_url}/models/setup/mission/history?limit=12&current_scope=1&manifest_path={manifest_path}&workspace_root={workspace_root}",
    )
    assert status == 200
    assert history["status"] == "success"
    assert history["items"][0]["mission_id"] == "msm_demo_scope"
    assert history["filters"]["manifest_path"] == manifest_path
    assert history["filters"]["workspace_root"] == workspace_root
    assert history["auto_resume_candidate_count"] == 1
    assert history["latest_auto_resume_candidate"]["mission_id"] == "msm_auto_scope"

    status, advice = request_json(
        "GET",
        f"{base_url}/models/setup/mission/resume-advice?mission_id=msm_demo_scope&current_scope=0&limit=24",
    )
    assert status == 200
    assert advice["status"] == "blocked"
    assert advice["mission_id"] == "msm_demo_scope"

    status, advice_ready = request_json(
        "GET",
        f"{base_url}/models/setup/mission/resume-advice?mission_id=msm_auto_scope&current_scope=0&limit=24",
    )
    assert status == 200
    assert advice_ready["status"] == "ready"
    assert advice_ready["can_auto_resume_now"] is True
    assert advice_ready["selected_action_ids"] == ["launch_setup_install:auto"]

    status, preview = request_json(
        "POST",
        f"{base_url}/models/setup/mission/launch",
        {"dry_run": True, "continue_on_error": True, "limit": 24, "manifest_path": manifest_path, "workspace_root": workspace_root},
    )
    assert status == 200
    assert preview["status"] == "planned"
    assert preview["dry_run"] is True
    assert int(preview["executed_count"]) == 2
    assert preview["items"][0]["status"] == "planned"

    status, applied = request_json(
        "POST",
        f"{base_url}/models/setup/mission/launch",
        {"dry_run": False, "continue_on_error": True, "limit": 24, "manifest_path": manifest_path, "workspace_root": workspace_root},
    )
    assert status == 200
    assert applied["status"] == "success"
    assert applied["dry_run"] is False
    assert int(applied["executed_count"]) == 2
    assert applied["items"][0]["status"] == "success"
    assert applied["mission_record"]["mission_id"] == "msm_demo_scope"

    status, resumed = request_json(
        "POST",
        f"{base_url}/models/setup/mission/resume",
        {
            "mission_id": "msm_demo_scope",
            "continue_on_error": True,
            "limit": 24,
            "manifest_path": manifest_path,
            "workspace_root": workspace_root,
        },
    )
    assert status == 200
    assert resumed["status"] == "success"
    assert resumed["resolved_mission"]["mission_id"] == "msm_demo_scope"
    assert resumed["message"] == "resumed setup mission"
    assert resumed["resume_advice"]["status"] == "blocked"

    status, auto_resumed = request_json(
        "POST",
        f"{base_url}/models/setup/mission/auto-resume",
        {
            "mission_id": "msm_auto_scope",
            "continue_on_error": True,
            "limit": 24,
            "current_scope": False,
            "manifest_path": manifest_path,
            "workspace_root": workspace_root,
        },
    )
    assert status == 200
    assert auto_resumed["status"] == "success"
    assert auto_resumed["auto_resume_attempted"] is True
    assert auto_resumed["auto_resume_triggered"] is True
    assert auto_resumed["resume_advice"]["mission_id"] == "msm_auto_scope"
    assert auto_resumed["continue_followup_actions_status"] == "success"
    assert auto_resumed["continued_action_ids"] == ["verify_provider:huggingface"]

    status, recovery_sweep = request_json(
        "POST",
        f"{base_url}/models/setup/mission/recovery-sweep",
        {
            "mission_id": "msm_auto_scope",
            "continue_on_error": True,
            "continue_followup_actions": True,
            "max_auto_resume_passes": 2,
            "max_followup_waves": 3,
            "limit": 24,
            "current_scope": False,
            "manifest_path": manifest_path,
            "workspace_root": workspace_root,
        },
    )
    assert status == 200
    assert recovery_sweep["status"] == "success"
    assert recovery_sweep["auto_resume_attempted_count"] == 1
    assert recovery_sweep["auto_resume_triggered_count"] == 1
    assert recovery_sweep["continue_followup_actions_requested"] is True
    assert recovery_sweep["continued_action_ids"] == ["verify_provider:huggingface"]
    assert recovery_sweep["passes"][0]["continue_followup_actions_status"] == "success"
    assert recovery_sweep["final_payload"]["auto_resume_triggered"] is True

    status, recovery_watchdog = request_json(
        "POST",
        f"{base_url}/models/setup/mission/recovery-watchdog",
        {
            "mission_id": "msm_auto_scope",
            "continue_on_error": True,
            "continue_followup_actions": True,
            "max_missions": 4,
            "max_auto_resumes": 2,
            "max_followup_waves": 3,
            "limit": 24,
            "current_scope": False,
            "manifest_path": manifest_path,
            "workspace_root": workspace_root,
        },
    )
    assert status == 200
    assert recovery_watchdog["status"] == "success"
    assert recovery_watchdog["auto_resume_attempted_count"] == 1
    assert recovery_watchdog["auto_resume_triggered_count"] == 1
    assert recovery_watchdog["evaluated_count"] == 1
    assert recovery_watchdog["latest_triggered_payload"]["auto_resume_triggered"] is True
    assert recovery_watchdog["results"][0]["classification_after"] == "idle"
    assert recovery_watchdog["watchdog_run"]["status"] == "success"
    assert recovery_watchdog["watchdog_history"]["triggered_run_count"] == 1

    status, watchdog_history = request_json(
        "GET",
        f"{base_url}/models/setup/mission/recovery-watchdog/history?current_scope=0&manifest_path={urllib.parse.quote(manifest_path)}&workspace_root={urllib.parse.quote(workspace_root)}",
    )
    assert status == 200
    assert watchdog_history["status"] == "success"
    assert watchdog_history["count"] == 1
    assert watchdog_history["latest_run"]["run_id"] == "mswd_demo"

    status, watchdog_cleared = request_json(
        "POST",
        f"{base_url}/models/setup/mission/recovery-watchdog/reset",
        {
            "run_id": "mswd_demo",
            "manifest_path": manifest_path,
            "workspace_root": workspace_root,
        },
    )
    assert status == 200
    assert watchdog_cleared["status"] == "success"
    assert watchdog_cleared["removed"] == 1

    status, watchdog_supervisor = request_json(
        "GET",
        f"{base_url}/models/setup/mission/recovery-watchdog/supervisor?history_limit=6",
    )
    assert status == 200
    assert watchdog_supervisor["status"] == "success"
    assert watchdog_supervisor["enabled"] is True
    assert watchdog_supervisor["watchdog_history"]["triggered_run_count"] == 1

    status, watchdog_supervisor_updated = request_json(
        "POST",
        f"{base_url}/models/setup/mission/recovery-watchdog/supervisor",
        {
            "enabled": False,
            "interval_s": 60,
            "manifest_path": manifest_path,
            "workspace_root": workspace_root,
        },
    )
    assert status == 200
    assert watchdog_supervisor_updated["status"] == "success"
    assert watchdog_supervisor_updated["enabled"] is False
    assert watchdog_supervisor_updated["interval_s"] == 60.0

    status, watchdog_triggered = request_json(
        "POST",
        f"{base_url}/models/setup/mission/recovery-watchdog/supervisor/trigger",
        {
            "current_scope": False,
            "manifest_path": manifest_path,
            "workspace_root": workspace_root,
            "max_missions": 4,
            "max_auto_resumes": 1,
        },
    )
    assert status == 200
    assert watchdog_triggered["status"] == "success"
    assert watchdog_triggered["result"]["auto_resume_triggered_count"] == 1
    assert watchdog_triggered["supervisor"]["status"] == "success"

    status, cleared = request_json(
        "POST",
        f"{base_url}/models/setup/mission/reset",
        {"mission_id": "msm_demo_scope"},
    )
    assert status == 200
    assert cleared["status"] == "success"
    assert cleared["removed"] == 1

    status, route = request_json(
        "GET",
        f"{base_url}/models/connectors/route-plan?requires_offline=1&mission_profile=privacy",
    )
    assert status == 200
    assert route["status"] == "success"
    assert route["preferred_provider"] == "local"
    assert isinstance(route.get("fallback_providers", []), list)

    status, runtime_health = request_json(
        "GET",
        f"{base_url}/runtime/health?stack_name=voice&preferred_model_name=qwen&requires_offline=1&privacy_mode=1&mission_profile=privacy&probe_tts_bridge=1",
    )
    assert status == 200
    assert runtime_health["status"] in {"success", "degraded"}
    assert float(runtime_health.get("score", 0.0) or 0.0) > 0.0
    assert runtime_health["subsystems"]["reasoning"]["candidate_count"] >= 1
    assert runtime_health["subsystems"]["reasoning_bridge"]["ready"] is True
    assert runtime_health["reasoning_bridge"]["ready"] is True
    assert runtime_health["bridge_status"]["ready_for_workflow"] is True
    assert runtime_health["tts_local_neural_bridge"]["ready"] is True
    assert int(runtime_health["subsystems"]["routing"].get("policy_monitored_count", 0)) >= 2
    assert "route_policy_summary" in runtime_health
    assert "blacklisted_count" in runtime_health["subsystems"]["launch_templates"]

    status, runtime_history = request_json(
        "GET",
        f"{base_url}/runtime/health/history?limit=12&stack_name=desktop_agent&refresh=1",
    )
    assert status == 200
    assert runtime_history["status"] == "success"
    assert int(runtime_history.get("count", 0)) >= 1
    assert isinstance(runtime_history.get("items", []), list)
    assert isinstance(runtime_history.get("diagnostics", {}), dict)


def test_model_setup_manual_and_install_routes_forward_scope(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, service = api_server
    manifest_path = "E:/scopes/runs/JARVIS_BACKEND/Models_manifest.txt"
    workspace_root = "E:/scopes/runs"

    status, manual_runs = request_json(
        "GET",
        f"{base_url}/models/setup/manual-pipeline/runs?limit=7&manifest_path={manifest_path}&workspace_root={workspace_root}",
    )
    assert status == 200
    assert manual_runs["status"] == "success"
    assert manual_runs["filters"]["manifest_path"] == manifest_path
    assert manual_runs["filters"]["workspace_root"] == workspace_root
    assert manual_runs["items"][0]["scope_key"] == f"{workspace_root.lower()}::{manifest_path.lower()}"

    status, manual_launch = request_json(
        "POST",
        f"{base_url}/models/setup/manual-pipeline/run",
        {
            "task": "reasoning",
            "item_keys": ["manual-convert-qwen"],
            "step_ids": ["convert"],
            "dry_run": True,
            "limit": 12,
            "manifest_path": manifest_path,
            "workspace_root": workspace_root,
        },
    )
    assert status == 200
    assert manual_launch["status"] == "planned"
    assert manual_launch["run"]["manifest_path"] == manifest_path
    assert manual_launch["run"]["workspace_root"] == workspace_root

    status, install_runs = request_json(
        "GET",
        f"{base_url}/models/setup/install/runs?limit=7&manifest_path={manifest_path}&workspace_root={workspace_root}",
    )
    assert status == 200
    assert install_runs["status"] == "success"
    assert install_runs["filters"]["manifest_path"] == manifest_path
    assert install_runs["filters"]["workspace_root"] == workspace_root
    assert install_runs["items"][0]["scope_key"] == f"{workspace_root.lower()}::{manifest_path.lower()}"

    status, install_launch = request_json(
        "POST",
        f"{base_url}/models/setup/install/launch",
        {
            "task": "reasoning",
            "item_keys": ["reasoning-llama"],
            "dry_run": False,
            "limit": 12,
            "manifest_path": manifest_path,
            "workspace_root": workspace_root,
        },
    )
    assert status == 200
    assert install_launch["status"] == "success"
    assert install_launch["run"]["manifest_path"] == manifest_path
    assert install_launch["run"]["workspace_root"] == workspace_root
    assert any(
        call["route"] == "model_setup_install_launch"
        and call["manifest_path"] == manifest_path
        and call["workspace_root"] == workspace_root
        for call in service.model_setup_scope_calls
    )


def test_provider_credential_routes_support_recovery_options(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, service = api_server

    status, saved = request_json(
        "POST",
        f"{base_url}/providers/credentials",
        {
            "provider": "huggingface",
            "api_key": "hf_" + ("A1b2C3d4E5f6G7h8" * 2),
            "verify_after_update": True,
            "task": "reasoning",
            "limit": 24,
            "include_present": True,
            "item_keys": ["reasoning-llama"],
            "continue_setup_recovery": True,
            "continue_on_error": False,
            "continue_followup_actions": True,
            "max_followup_waves": 4,
            "include_coworker_status": True,
            "refresh_remote": True,
            "timeout_s": 12.0,
        },
    )
    assert status == 200
    assert saved["status"] == "success"
    assert saved["verification_requested"] is True
    assert saved["setup_recovery"]["launchable_count"] == 1
    assert saved["continue_setup_recovery_requested"] is True
    assert saved["continue_setup_recovery_status"] == "success"
    assert saved["continue_followup_actions_requested"] is True
    assert saved["continue_followup_actions_status"] == "success"
    assert saved["recovery_launch"]["selected_action_ids"] == ["install:reasoning-llama"]
    assert saved["recovery_launch"]["continued_action_ids"] == ["launch_setup_install:auto"]
    assert saved["coworker_stack"]["status"] == "success"
    assert service.provider_update_calls[-1]["verify_after_update"] is True
    assert service.provider_update_calls[-1]["item_keys"] == ["reasoning-llama"]
    assert service.provider_update_calls[-1]["continue_setup_recovery"] is True
    assert service.provider_update_calls[-1]["continue_on_error"] is False
    assert service.provider_update_calls[-1]["continue_followup_actions"] is True
    assert service.provider_update_calls[-1]["max_followup_waves"] == 4
    assert service.provider_update_calls[-1]["include_coworker_status"] is True
    assert service.provider_update_calls[-1]["refresh_remote"] is True

    status, verified = request_json(
        "POST",
        f"{base_url}/providers/credentials/verify",
        {
            "provider": "huggingface",
            "task": "reasoning",
            "limit": 24,
            "include_present": True,
            "item_keys": ["reasoning-llama"],
            "force_refresh": True,
            "continue_setup_recovery": True,
            "continue_on_error": False,
            "continue_followup_actions": True,
            "max_followup_waves": 4,
            "include_coworker_status": True,
            "refresh_remote": True,
            "timeout_s": 12.0,
        },
    )
    assert status == 200
    assert verified["status"] == "success"
    assert verified["affected_item_keys"] == ["reasoning-llama"]
    assert verified["setup_recovery"]["next_action"]["kind"] == "launch_setup_install"
    assert verified["continue_setup_recovery_requested"] is True
    assert verified["continue_setup_recovery_status"] == "success"
    assert verified["continue_followup_actions_requested"] is True
    assert verified["continue_followup_actions_status"] == "success"
    assert verified["recovery_launch"]["selected_action_ids"] == ["install:reasoning-llama"]
    assert verified["recovery_launch"]["continued_action_ids"] == ["launch_setup_install:auto"]
    assert verified["coworker_recovery"]["status"] == "success"
    assert service.provider_verify_calls[-1]["continue_setup_recovery"] is True
    assert service.provider_verify_calls[-1]["continue_on_error"] is False
    assert service.provider_verify_calls[-1]["continue_followup_actions"] is True
    assert service.provider_verify_calls[-1]["max_followup_waves"] == 4
    assert service.provider_verify_calls[-1]["include_coworker_status"] is True
    assert service.provider_verify_calls[-1]["refresh_remote"] is True
    assert service.provider_verify_calls[-1]["item_keys"] == ["reasoning-llama"]


def test_provider_recovery_route_supports_launch_options(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, service = api_server

    status, recovered = request_json(
        "POST",
        f"{base_url}/providers/credentials/recover",
        {
            "provider": "huggingface",
            "task": "reasoning",
            "limit": 24,
            "include_present": True,
            "item_keys": ["reasoning-llama"],
            "selected_action_ids": ["install:reasoning-llama"],
            "dry_run": True,
            "continue_on_error": False,
            "continue_followup_actions": True,
            "max_followup_waves": 5,
            "refresh_provider_credentials": True,
            "refresh_remote": True,
            "timeout_s": 12.0,
        },
    )

    assert status == 200
    assert recovered["status"] == "planned"
    assert recovered["selected_action_ids"] == ["install:reasoning-llama"]
    assert recovered["continue_followup_actions_requested"] is True
    assert recovered["continue_followup_actions_status"] == "planned"
    assert recovered["setup_recovery"]["auto_runnable_ready_action_ids"] == ["install:reasoning-llama"]
    assert recovered["coworker_stack"]["status"] == "success"
    assert service.provider_recovery_calls[-1]["provider"] == "huggingface"
    assert service.provider_recovery_calls[-1]["item_keys"] == ["reasoning-llama"]
    assert service.provider_recovery_calls[-1]["selected_action_ids"] == ["install:reasoning-llama"]
    assert service.provider_recovery_calls[-1]["dry_run"] is True
    assert service.provider_recovery_calls[-1]["continue_on_error"] is False
    assert service.provider_recovery_calls[-1]["continue_followup_actions"] is True
    assert service.provider_recovery_calls[-1]["max_followup_waves"] == 5
    assert service.provider_recovery_calls[-1]["refresh_provider_credentials"] is True
    assert service.provider_recovery_calls[-1]["refresh_remote"] is True


def test_model_connector_post_routes(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, reasoning_warm = request_json(
        "POST",
        f"{base_url}/models/runtime-supervisors/reasoning/warm",
        payload={"preferred_model_name": "qwen", "load_all": False, "force_reload": True},
    )
    assert status == 200
    assert reasoning_warm["status"] == "success"
    assert reasoning_warm["runtime"]["loaded_count"] >= 1

    status, reasoning_probe = request_json(
        "POST",
        f"{base_url}/models/runtime-supervisors/reasoning/probe",
        payload={"preferred_model_name": "qwen", "prompt": "Summarize runtime readiness.", "force_reload": True},
    )
    assert status == 200
    assert reasoning_probe["status"] == "success"
    assert reasoning_probe["model"] == "local-auto-reasoning-qwen3-14b"
    assert reasoning_probe["runtime"]["probe_healthy_count"] >= 1

    status, reasoning_reset = request_json(
        "POST",
        f"{base_url}/models/runtime-supervisors/reasoning/reset",
        payload={"model_name": "qwen", "clear_all": False},
    )
    assert status == 200
    assert reasoning_reset["status"] == "success"
    assert reasoning_reset["removed_count"] >= 1

    status, reasoning_restart = request_json(
        "POST",
        f"{base_url}/models/runtime-supervisors/reasoning/restart",
        payload={
            "preferred_model_name": "qwen",
            "prompt": "Summarize runtime readiness after restart.",
            "load_all": False,
            "force_reload": True,
            "probe": True,
            "restart_bridge": True,
        },
    )
    assert status == 200
    assert reasoning_restart["status"] in {"success", "degraded"}
    assert reasoning_restart["recovered"] is True
    assert reasoning_restart["bridge"]["ready"] is True
    assert reasoning_restart["runtime"]["reasoning"]["candidate_count"] >= 1
    assert any(str(stage.get("stage", "")) == "probe" for stage in reasoning_restart.get("stages", []))
    assert any(str(stage.get("stage", "")) == "bridge_restart" for stage in reasoning_restart.get("stages", []))

    status, reasoning_bridge_start = request_json(
        "POST",
        f"{base_url}/models/runtime-supervisors/reasoning/bridge/start",
        payload={"wait_ready": True, "timeout_s": 18, "reason": "api-test", "force": True},
    )
    assert status == 200
    assert reasoning_bridge_start["status"] == "success"
    assert reasoning_bridge_start["ready"] is True

    status, reasoning_bridge_probe = request_json(
        "POST",
        f"{base_url}/models/runtime-supervisors/reasoning/bridge/probe",
        payload={"force": True},
    )
    assert status == 200
    assert reasoning_bridge_probe["status"] == "success"
    assert int(reasoning_bridge_probe.get("probe_attempts", 0)) >= 1

    status, reasoning_bridge_stop = request_json(
        "POST",
        f"{base_url}/models/runtime-supervisors/reasoning/bridge/stop",
        payload={"reason": "api-test"},
    )
    assert status == 200
    assert reasoning_bridge_stop["status"] == "success"
    assert reasoning_bridge_stop["running"] is False

    status, reasoning_bridge_restart = request_json(
        "POST",
        f"{base_url}/models/runtime-supervisors/reasoning/bridge/restart",
        payload={"wait_ready": True, "timeout_s": 18, "reason": "api-test", "force": True},
    )
    assert status == 200
    assert reasoning_bridge_restart["status"] == "success"
    assert reasoning_bridge_restart["ready"] is True

    status, reasoning_bridge_profile = request_json(
        "POST",
        f"{base_url}/models/runtime-supervisors/reasoning/bridge/profile",
        payload={
            "profile_id": "reasoning-bridge-local-auto-reasoning-qwen3-14b",
            "replace": True,
            "restart": True,
            "wait_ready": True,
            "force": True,
        },
    )
    assert status == 200
    assert reasoning_bridge_profile["status"] == "success"
    assert reasoning_bridge_profile["profile_id"] == "reasoning-bridge-local-auto-reasoning-qwen3-14b"
    assert reasoning_bridge_profile["bridge"]["active_profile_id"] == "reasoning-bridge-local-auto-reasoning-qwen3-14b"
    assert reasoning_bridge_profile["bridge"]["active_template_id"] == ""
    assert reasoning_bridge_profile["bridge"]["ready"] is True

    status, tts_bridge_profile = request_json(
        "POST",
        f"{base_url}/tts/local-neural/profile",
        payload={
            "profile_id": "tts-bridge-orpheus-3b-tts-f16",
            "replace": True,
            "restart": True,
            "wait_ready": True,
            "force": True,
        },
    )
    assert status == 200
    assert tts_bridge_profile["status"] == "success"
    assert tts_bridge_profile["profile_id"] == "tts-bridge-orpheus-3b-tts-f16"
    assert tts_bridge_profile["bridge"]["active_profile_id"] == "tts-bridge-orpheus-3b-tts-f16"
    assert tts_bridge_profile["bridge"]["active_template_id"] == ""
    assert tts_bridge_profile["ready"] is True
    assert tts_bridge_profile["neural_runtime"]["active_profile_id"] == "tts-bridge-orpheus-3b-tts-f16"
    assert tts_bridge_profile["neural_runtime"]["active_template_id"] == ""

    status, reasoning_launch_template = request_json(
        "POST",
        f"{base_url}/models/launch-template/execute",
        payload={
            "profile_id": "reasoning-bridge-local-auto-reasoning-qwen3-14b",
            "template_id": "reasoning-llama-server-local-auto-reasoning-qwen3-14b",
            "replace": True,
            "wait_ready": True,
            "force": True,
            "probe": True,
        },
    )
    assert status == 200
    assert reasoning_launch_template["status"] == "success"
    assert reasoning_launch_template["bridge_kind"] == "reasoning"
    assert reasoning_launch_template["template_id"] == "reasoning-llama-server-local-auto-reasoning-qwen3-14b"
    assert reasoning_launch_template["bridge"]["active_template_id"] == "reasoning-llama-server-local-auto-reasoning-qwen3-14b"
    assert reasoning_launch_template["ready"] is True
    assert reasoning_launch_template["history_record"]["template_id"] == "reasoning-llama-server-local-auto-reasoning-qwen3-14b"
    assert isinstance(reasoning_launch_template.get("template_health", {}), dict)
    assert isinstance(reasoning_launch_template.get("retry_policy", {}), dict)

    status, tts_launch_template = request_json(
        "POST",
        f"{base_url}/models/launch-template/execute",
        payload={
            "profile_id": "tts-bridge-orpheus-3b-tts-f16",
            "template_id": "tts-existing-endpoint-orpheus-3b-tts-f16",
            "replace": True,
            "wait_ready": True,
            "force": True,
            "probe": True,
        },
    )
    assert status == 200
    assert tts_launch_template["status"] == "success"
    assert tts_launch_template["bridge_kind"] == "tts"
    assert tts_launch_template["template_id"] == "tts-existing-endpoint-orpheus-3b-tts-f16"
    assert tts_launch_template["bridge"]["active_template_id"] == "tts-existing-endpoint-orpheus-3b-tts-f16"
    assert tts_launch_template["neural_runtime"]["active_template_id"] == "tts-existing-endpoint-orpheus-3b-tts-f16"
    assert tts_launch_template["ready"] is True
    assert tts_launch_template["history_record"]["template_id"] == "tts-existing-endpoint-orpheus-3b-tts-f16"

    status, stt_launch_template = request_json(
        "POST",
        f"{base_url}/models/launch-template/execute",
        payload={
            "profile_id": "stt-runtime-whisper-large-v3",
            "template_id": "stt-local-runtime-whisper-large-v3",
            "replace": True,
            "wait_ready": True,
            "force": True,
            "probe": True,
        },
    )
    assert status == 200
    assert stt_launch_template["status"] == "success"
    assert stt_launch_template["bridge_kind"] == "stt"
    assert stt_launch_template["template_id"] == "stt-local-runtime-whisper-large-v3"
    assert stt_launch_template["runtime_profile"]["template_id"] == "stt-local-runtime-whisper-large-v3"
    assert stt_launch_template["ready"] is True
    assert stt_launch_template["history_record"]["template_id"] == "stt-local-runtime-whisper-large-v3"

    status, vision_launch_template = request_json(
        "POST",
        f"{base_url}/models/launch-template/execute",
        payload={
            "profile_id": "vision-runtime-yolov10x",
            "template_id": "vision-reload-yolo-yolov10x",
            "replace": True,
            "wait_ready": True,
            "force": True,
            "probe": True,
        },
    )
    assert status == 200
    assert vision_launch_template["status"] == "success"
    assert vision_launch_template["bridge_kind"] == "vision"
    assert vision_launch_template["template_id"] == "vision-reload-yolo-yolov10x"
    assert vision_launch_template["runtime_profile"]["template_id"] == "vision-reload-yolo-yolov10x"
    assert vision_launch_template["ready"] is True
    assert vision_launch_template["history_record"]["template_id"] == "vision-reload-yolo-yolov10x"

    status, vision_warm = request_json(
        "POST",
        f"{base_url}/models/runtime-supervisors/vision/warm",
        payload={"models": ["clip", "sam"], "force_reload": True},
    )
    assert status == 200
    assert vision_warm["status"] == "success"
    assert int(vision_warm.get("count", 0)) == 2

    status, vision_reset = request_json(
        "POST",
        f"{base_url}/models/runtime-supervisors/vision/reset",
        payload={"models": ["clip"], "clear_cache": True},
    )
    assert status == 200
    assert vision_reset["status"] == "success"
    assert "clip" in vision_reset.get("removed", [])
    assert vision_reset["clear_cache"] is True

    status, probe = request_json(
        "POST",
        f"{base_url}/models/connectors/probe",
        payload={"active_probe": True, "timeout_s": 3.5},
    )
    assert status == 200
    assert probe["status"] == "success"
    assert probe["active_probe"] is True
    assert int(probe.get("count", 0)) >= 1

    status, policy = request_json(
        "POST",
        f"{base_url}/models/connectors/policy",
        payload={"updates": {"readiness_weight": 2.05, "latency_weight": 1.4}},
    )
    assert status == 200
    assert policy["status"] == "success"
    assert int(policy.get("count", 0)) == 2
    changed = policy.get("changed", {})
    assert float(changed.get("readiness_weight", 0.0)) == 2.05


def test_model_launch_history_demotes_unstable_template(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, service = api_server
    template_id = "reasoning-llama-server-local-auto-reasoning-qwen3-14b"
    service.launch_template_forced_status_by_id[template_id] = "error"

    for _ in range(5):
        status, payload = request_json(
            "POST",
            f"{base_url}/models/launch-template/execute",
            payload={
                "profile_id": "reasoning-bridge-local-auto-reasoning-qwen3-14b",
                "template_id": template_id,
                "replace": True,
                "wait_ready": True,
                "force": True,
                "probe": True,
                "retry_on_failure": False,
                "auto_fallback": False,
            },
        )
        assert status == 400
        assert payload["status"] == "error"
        assert payload["template_health"]["failure_count"] >= 1

    status, payload = request_json(
        "POST",
        f"{base_url}/models/launch-template/execute",
        payload={
            "profile_id": "reasoning-bridge-local-auto-reasoning-qwen3-14b",
            "template_id": template_id,
            "replace": True,
            "wait_ready": True,
            "force": True,
            "probe": True,
        },
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["fallback_applied"] is True
    assert payload["retry_applied"] is False
    assert payload["requested_template_id"] == template_id
    assert payload["template_id"] == "reasoning-endpoint-local-auto-reasoning-qwen3-14b"
    assert payload["execution_diff"]["template_changed"] is True
    assert "launcher" in payload["execution_diff"]["changed_fields"]

    status, profiles = request_json(
        "GET",
        f"{base_url}/models/bridge-profiles?task=reasoning&limit=8",
    )
    assert status == 200
    assert profiles["launch_health_summary"]["demoted_template_count"] >= 1
    assert int(profiles["launch_health_summary"].get("suppressed_template_count", 0) or 0) >= 0
    assert profiles["launch_health_summary"]["blacklisted_template_count"] >= 1
    profile = profiles["profiles"][0]
    assert profile["recommended_launch_template_id"] == "reasoning-endpoint-local-auto-reasoning-qwen3-14b"
    failing_template = next(
        row for row in profile["launch_templates"] if row["template_id"] == template_id
    )
    assert failing_template["health"]["demoted"] is True
    assert failing_template["blacklisted"] is True
    assert int(failing_template["cooldown_hint_s"]) > 0
    assert "suppressed" in failing_template
    assert failing_template["recommended"] is False

    status, history = request_json(
        "GET",
        f"{base_url}/models/launch-template/history?bridge_kind=reasoning&profile_id=reasoning-bridge-local-auto-reasoning-qwen3-14b&failure_like=1&limit=12",
    )
    assert status == 200
    assert history["failure_count"] >= 2
    assert history["bridge_kind_counts"]["reasoning"] >= 2
    assert history["filters"]["failure_like"] is True
    assert isinstance(history.get("retry_strategy_counts", {}), dict)
    assert history["retry_strategy_counts"]["stabilized_backoff"] >= 1
    assert isinstance(history.get("strategy_outcomes", []), list)
    assert isinstance(history.get("retry_profile_trend", []), list)
    assert isinstance(history.get("strategy_score_timeline", []), list)
    assert isinstance(history.get("degradation_timeline", []), list)


def test_model_launch_execute_retries_failed_template_chain(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, service = api_server
    requested_template_id = "reasoning-llama-server-local-auto-reasoning-qwen3-14b"
    executed_template_id = "reasoning-endpoint-local-auto-reasoning-qwen3-14b"
    service.launch_template_forced_status_by_id[requested_template_id] = "error"

    status, payload = request_json(
        "POST",
        f"{base_url}/models/launch-template/execute",
        payload={
            "profile_id": "reasoning-bridge-local-auto-reasoning-qwen3-14b",
            "template_id": requested_template_id,
            "replace": True,
            "wait_ready": True,
            "force": True,
            "probe": True,
            "retry_on_failure": True,
            "max_attempts": 3,
            "auto_fallback": False,
            "retry_profile": "stabilized",
            "retry_base_delay_ms": 150,
            "retry_max_delay_ms": 1200,
            "retry_jitter_ms": 40,
            "retry_prefer_recommended": True,
        },
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["requested_template_id"] == requested_template_id
    assert payload["root_requested_template_id"] == requested_template_id
    assert payload["attempt_requested_template_id"] == executed_template_id
    assert payload["template_id"] == executed_template_id
    assert payload["retry_applied"] is True
    assert payload["retry_reason"] == "prior_attempt_failure"
    assert payload["retry_target_template_id"] == executed_template_id
    assert isinstance(payload.get("retry_policy", {}), dict)
    assert payload["retry_policy"]["profile"] == "stabilized"
    assert payload["retry_policy"]["strategy"] == "stabilized_backoff"
    assert payload["retry_policy"]["escalation_mode"] == "breadth_first"
    assert int(payload.get("retry_delay_ms", 0) or 0) >= 150
    assert payload["retry_backoff_applied"] is True
    assert int(payload.get("attempt_count", 0)) == 2
    assert payload["execution_diff"]["template_changed"] is True
    assert len(payload.get("attempt_chain", [])) == 2
    assert payload["attempt_chain"][0]["status"] == "error"
    assert payload["attempt_chain"][1]["status"] == "success"
    assert payload["attempt_chain"][0]["retry_strategy"] == "stabilized_backoff"
    assert payload["attempt_chain"][0]["retry_escalation_mode"] == "recommended_first"
    assert payload["attempt_chain"][1]["retry_escalation_mode"] == "breadth_first"
    assert payload["attempt_chain"][0]["execution_diff"]["template_changed"] is False
    assert payload["attempt_chain"][1]["execution_diff"]["template_changed"] is False
    assert payload["attempt_chain"][0]["attempt_chain_id"] == payload["attempt_chain"][1]["attempt_chain_id"]

    status, history = request_json(
        "GET",
        f"{base_url}/models/launch-template/history?bridge_kind=reasoning&profile_id=reasoning-bridge-local-auto-reasoning-qwen3-14b&limit=12",
    )
    assert status == 200
    assert history["retry_chain_count"] >= 1
    assert history["max_attempt_depth"] >= 2
    assert history["retry_strategy_counts"]["stabilized_backoff"] >= 1
    assert sum(int(value or 0) for value in history.get("retry_escalation_counts", {}).values()) >= 1
    assert history["retry_escalation_counts"]["breadth_first"] >= 1
    assert int(history.get("retry_delay_total_ms", 0) or 0) >= 150
    assert int(history.get("max_retry_delay_ms", 0) or 0) >= 150
    assert str(history.get("recommended_retry_profile", "") or "").strip() != ""
    assert isinstance(history.get("retry_profile_trend", []), list)
    assert isinstance(history.get("strategy_score_timeline", []), list)
    assert isinstance(history.get("degradation_timeline", []), list)

    status, detail = request_json(
        "GET",
        f"{base_url}/models/launch-template/event?event_id={int(payload['history_record']['event_id'])}&sibling_limit=4",
    )
    assert status == 200
    assert detail["status"] == "success"
    assert int(detail["chain_summary"]["attempt_count"]) == 2
    assert detail["root_execution_diff"]["template_changed"] is True
    assert len(detail.get("attempt_chain", [])) == 2
    assert isinstance(detail.get("strategy_outcomes", []), list)
    assert any(
        str(item.get("template_id", "")) == executed_template_id
        and int(item.get("attempt_index", 0) or 0) == 2
        and str(item.get("requested_template_id", "")) == executed_template_id
        for item in history["items"]
    )


def test_post_goals_validates_required_text(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, body = request_json("POST", f"{base_url}/goals", payload={})
    assert status == 400
    assert "text is required" in body.get("message", "")


def test_post_goals_wait_returns_goal_payload(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, body = request_json(
        "POST",
        f"{base_url}/goals",
        payload={"text": "what time is it in UTC", "wait": True, "timeout_s": 1},
    )
    assert status == 200
    assert "goal_id" in body
    assert body["goal"]["status"] == "completed"


def test_post_actions_and_approval_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, first = request_json(
        "POST",
        f"{base_url}/actions",
        payload={"action": "copy_file", "args": {"source": "a.txt", "destination": "b.txt"}},
    )
    assert status == 200
    assert first["status"] == "blocked"
    assert first["output"]["approval_required"] is True

    approval_id = first["output"]["approval"]["approval_id"]
    status, approved = request_json("POST", f"{base_url}/approvals/{approval_id}/approve", payload={"note": "ok"})
    assert status == 200
    assert approved["status"] == "success"

    status, second = request_json(
        "POST",
        f"{base_url}/actions",
        payload={
            "action": "copy_file",
            "args": {"source": "a.txt", "destination": "b.txt"},
            "approval_id": approval_id,
        },
    )
    assert status == 200
    assert second["status"] == "success"


def test_get_goal_not_found_returns_404(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, body = request_json("GET", f"{base_url}/goals/does-not-exist")
    assert status == 404
    assert body["status"] == "error"


def test_list_goals_route_with_status_filter(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, created_running = request_json("POST", f"{base_url}/goals", payload={"text": "long running task"})
    assert status == 202
    assert isinstance(created_running.get("goal_id"), str)

    status, created_done = request_json("POST", f"{base_url}/goals", payload={"text": "what time is it in UTC"})
    assert status == 202
    assert isinstance(created_done.get("goal_id"), str)

    status, listed_running = request_json("GET", f"{base_url}/goals?status=running&limit=5")
    assert status == 200
    assert listed_running["count"] >= 1
    assert all(str(item.get("status", "")).lower() == "running" for item in listed_running["items"])

    status, listed_all = request_json("GET", f"{base_url}/goals?limit=20")
    assert status == 200
    assert listed_all["count"] >= listed_running["count"]
    assert listed_all["total"] >= listed_all["count"]
    assert all("goal_id" in item for item in listed_all["items"])


def test_goal_cancel_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, created = request_json("POST", f"{base_url}/goals", payload={"text": "long running task"})
    assert status == 202
    goal_id = created.get("goal_id")
    assert isinstance(goal_id, str) and goal_id

    status, cancelled = request_json(
        "POST",
        f"{base_url}/goals/{goal_id}/cancel",
        payload={"reason": "stop now"},
    )
    assert status == 200
    assert cancelled["status"] == "success"
    assert cancelled["goal"]["status"] == "cancelled"


def test_plan_preview_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "POST",
        f"{base_url}/plans/preview",
        payload={"text": "what time is it in UTC"},
    )
    assert status == 200
    assert payload["status"] == "success"
    plan = payload.get("plan", {})
    assert plan.get("step_count") == 1
    steps = plan.get("steps", [])
    assert isinstance(steps, list) and steps
    assert steps[0].get("action") == "time_now"


def test_policy_profiles_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json("GET", f"{base_url}/policy/profiles")
    assert status == 200
    assert payload["count"] >= 1
    assert payload["default_profile"] == "interactive"
    assert payload["items"][0]["default_max_runtime_s"] >= 10
    assert payload["items"][0]["default_max_steps"] >= 1


def test_recovery_profiles_routes(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json("GET", f"{base_url}/recovery/profiles")
    assert status == 200
    assert payload["status"] == "success"
    assert payload["default_profile"] == "balanced"
    assert payload["count"] >= 3

    status, updated = request_json(
        "POST",
        f"{base_url}/recovery/profiles/default",
        payload={"profile": "safe"},
    )
    assert status == 200
    assert updated["status"] == "success"
    assert updated["default_profile"] == "safe"
    profiles = updated.get("profiles", {})
    assert profiles.get("default_profile") == "safe"


def test_telemetry_events_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json("GET", f"{base_url}/telemetry/events?event=goal.completed&after_id=1&limit=10")
    assert status == 200
    assert payload["count"] == 1
    assert payload["latest_event_id"] == 4
    assert payload["items"][0]["event"] == "goal.completed"


def test_telemetry_stream_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server
    request = urllib.request.Request(
        url=f"{base_url}/telemetry/stream?event=goal.completed&limit=10&timeout_s=1&heartbeat_s=1",
        method="GET",
        headers={"Accept": "text/event-stream"},
    )
    with urllib.request.urlopen(request, timeout=6) as response:
        assert "text/event-stream" in str(response.headers.get("Content-Type", ""))
        raw = response.read().decode("utf-8")

    assert "event: ready" in raw
    assert "event: telemetry" in raw
    assert '"event": "goal.completed"' in raw
    assert "event: done" in raw


def test_voice_session_stream_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server
    request = urllib.request.Request(
        url=f"{base_url}/voice/session/stream?events=voice.*&include_state=1&limit=20&timeout_s=1&heartbeat_s=1&state_interval_s=0.3",
        method="GET",
        headers={"Accept": "text/event-stream"},
    )
    with urllib.request.urlopen(request, timeout=6) as response:
        assert "text/event-stream" in str(response.headers.get("Content-Type", ""))
        raw = response.read().decode("utf-8")

    assert "event: ready" in raw
    assert "event: voice_event" in raw
    assert '"event": "voice.transcribed"' in raw
    assert "event: transcript" in raw
    assert "event: state" in raw
    assert "event: done" in raw


def test_trigger_routes_create_get_list_pause_resume_run_now_cancel(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, created = request_json(
        "POST",
        f"{base_url}/triggers",
        payload={"text": "system snapshot", "interval_s": 120},
    )
    assert status == 200
    assert created["status"] == "success"
    trigger = created["trigger"]
    trigger_id = trigger["trigger_id"]

    status, listed = request_json("GET", f"{base_url}/triggers")
    assert status == 200
    assert listed["count"] >= 1

    status, fetched = request_json("GET", f"{base_url}/triggers/{trigger_id}")
    assert status == 200
    assert fetched["trigger"]["trigger_id"] == trigger_id

    status, paused = request_json("POST", f"{base_url}/triggers/{trigger_id}/pause", payload={})
    assert status == 200
    assert paused["trigger"]["status"] == "paused"

    status, resumed = request_json("POST", f"{base_url}/triggers/{trigger_id}/resume", payload={})
    assert status == 200
    assert resumed["trigger"]["status"] == "active"

    status, run_now = request_json("POST", f"{base_url}/triggers/{trigger_id}/run-now", payload={})
    assert status == 200
    assert run_now["status"] == "success"

    status, cancelled = request_json("POST", f"{base_url}/triggers/{trigger_id}/cancel", payload={})
    assert status == 200
    assert cancelled["trigger"]["status"] == "cancelled"


def test_macro_routes_list_get_run(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, listed = request_json("GET", f"{base_url}/macros?query=time&limit=10")
    assert status == 200
    assert listed["count"] >= 1
    macro = listed["items"][0]
    macro_id = macro["macro_id"]

    status, fetched = request_json("GET", f"{base_url}/macros/{macro_id}")
    assert status == 200
    assert fetched["status"] == "success"
    assert fetched["macro"]["macro_id"] == macro_id

    status, ran = request_json(
        "POST",
        f"{base_url}/macros/{macro_id}/run",
        payload={"source": "desktop-macro", "metadata": {"policy_profile": "interactive"}},
    )
    assert status == 200
    assert ran["status"] == "success"
    goal_id = ran.get("goal_id")
    assert isinstance(goal_id, str)
    assert ran["macro"]["macro_id"] == macro_id

    status, goal = request_json("GET", f"{base_url}/goals/{goal_id}")
    assert status == 200
    assert goal["source"] == "desktop-macro"
    assert goal["metadata"]["policy_profile"] == "interactive"


def test_schedule_routes_create_get_list_cancel(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, created = request_json(
        "POST",
        f"{base_url}/schedules",
        payload={
            "text": "what time is it in UTC",
            "run_at": "2026-02-23T12:00:00+00:00",
            "max_attempts": 2,
            "retry_delay_s": 30,
            "repeat_interval_s": 300,
        },
    )
    assert status == 200
    assert created["status"] == "success"
    schedule = created["schedule"]
    schedule_id = schedule["schedule_id"]
    assert schedule["repeat_interval_s"] == 300

    status, listed = request_json("GET", f"{base_url}/schedules")
    assert status == 200
    assert listed["count"] >= 1

    status, fetched = request_json("GET", f"{base_url}/schedules/{schedule_id}")
    assert status == 200
    assert fetched["schedule"]["schedule_id"] == schedule_id

    status, cancelled = request_json("POST", f"{base_url}/schedules/{schedule_id}/cancel", payload={})
    assert status == 200
    assert cancelled["status"] == "success"
    assert cancelled["schedule"]["status"] == "cancelled"


def test_schedule_routes_pause_resume_run_now(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server
    status, created = request_json(
        "POST",
        f"{base_url}/schedules",
        payload={"text": "system snapshot", "run_at": "2026-02-23T12:00:00+00:00"},
    )
    assert status == 200
    schedule_id = created["schedule"]["schedule_id"]

    status, paused = request_json("POST", f"{base_url}/schedules/{schedule_id}/pause", payload={})
    assert status == 200
    assert paused["status"] == "success"
    assert paused["schedule"]["status"] == "paused"

    status, resumed = request_json("POST", f"{base_url}/schedules/{schedule_id}/resume", payload={})
    assert status == 200
    assert resumed["status"] == "success"
    assert resumed["schedule"]["status"] == "pending"

    status, immediate = request_json("POST", f"{base_url}/schedules/{schedule_id}/run-now", payload={})
    assert status == 200
    assert immediate["status"] == "success"
    assert immediate["schedule"]["status"] == "pending"
    assert isinstance(immediate["schedule"].get("next_run_at"), str)


def test_memory_query_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, body = request_json("GET", f"{base_url}/memory?query=utc&limit=5")
    assert status == 200
    assert body["query"] == "utc"
    assert body["count"] >= 1


def test_memory_query_invalid_mode_returns_400(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, body = request_json("GET", f"{base_url}/memory?query=utc&mode=invalid")
    assert status == 400
    assert body["status"] == "error"


def test_memory_query_route_parses_policy_filters(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, body = request_json(
        "GET",
        (
            f"{base_url}/memory?query=utc&mode=semantic&min_score=0.2"
            "&must_tag=source:desktop-ui&prefer_tag=action:time_now"
            "&exclude_goal_id=goal-1&diversify_by_goal=0"
        ),
    )
    assert status == 200
    filters = body.get("stats", {}).get("filters", {})
    assert filters.get("min_score") == 0.2
    assert filters.get("must_tags") == ["source:desktop-ui"]
    assert filters.get("prefer_tags") == ["action:time_now"]
    assert filters.get("exclude_goal_ids") == ["goal-1"]
    assert filters.get("diversify_by_goal") is False


def test_memory_strategy_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, body = request_json("GET", f"{base_url}/memory/strategy?query=email+followup&limit=9&min_score=0.2")
    assert status == 200
    assert body["status"] == "success"
    assert body["query"] == "email followup"
    assert body["sample_count"] >= 1
    assert body["recommended_actions"][0]["action"] == "external_email_send"
    assert body["avoid_actions"][0]["action"] == "browser_read_dom"
    assert body["limit"] == 9
    assert body["min_score"] == 0.2


def test_task_routes_list_create_update(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, listed = request_json(
        "GET",
        f"{base_url}/tasks?provider=google&query=roadmap&max_results=5&include_completed=0&status=not_started",
    )
    assert status == 200
    assert listed["status"] == "success"
    assert listed["provider"] == "google"
    assert listed["query"] == "roadmap"
    assert listed["include_completed"] is False
    assert isinstance(listed.get("items"), list)

    status, created = request_json(
        "POST",
        f"{base_url}/tasks/create",
        payload={
            "provider": "google",
            "title": "Ship desktop wrapper",
            "notes": "Run QA checklist",
            "source": "desktop-ui",
            "metadata": {"ticket": "JVS-101"},
        },
    )
    assert status == 200
    assert created["status"] == "success"
    assert created["task_id"].startswith("task-")
    assert created["title"] == "Ship desktop wrapper"

    status, updated = request_json(
        "POST",
        f"{base_url}/tasks/update",
        payload={
            "provider": "graph",
            "task_id": created["task_id"],
            "status": "completed",
            "source": "desktop-ui",
            "metadata": {"ticket": "JVS-101"},
        },
    )
    assert status == 200
    assert updated["status"] == "success"
    assert updated["task_id"] == created["task_id"]
    assert updated["status_value"] == "completed"


def test_task_routes_validate_required_fields(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, create_error = request_json("POST", f"{base_url}/tasks/create", payload={"provider": "auto"})
    assert status == 400
    assert "title is required" in str(create_error.get("message", ""))

    status, update_error = request_json("POST", f"{base_url}/tasks/update", payload={"status": "completed"})
    assert status == 400
    assert "task_id is required" in str(update_error.get("message", ""))


def test_external_connector_preflight_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, failed = request_json(
        "POST",
        f"{base_url}/external/connectors/preflight",
        payload={"action": "external_email_send", "provider": "google", "args": {}},
    )
    assert status == 400
    assert failed["status"] == "error"
    contract = failed.get("contract_diagnostic", {})
    assert isinstance(contract, dict)
    assert contract.get("code") == "missing_fields"
    hints = failed.get("remediation_hints", [])
    assert isinstance(hints, list)
    assert hints and isinstance(hints[0], dict)
    assert isinstance(hints[0].get("args_patch"), dict)
    orchestration_failed = failed.get("orchestration_diagnostics", {})
    assert isinstance(orchestration_failed, dict)
    assert float(orchestration_failed.get("reliability_pressure", 0.0) or 0.0) >= 0.0
    route_advisor_failed = orchestration_failed.get("route_weight_advisor", {})
    assert isinstance(route_advisor_failed, dict)
    assert isinstance(route_advisor_failed.get("weight_rows", []), list)
    cooldown_explainer_failed = orchestration_failed.get("cooldown_outage_explainer", {})
    assert isinstance(cooldown_explainer_failed, dict)
    assert isinstance(cooldown_explainer_failed.get("recommended_actions", []), list)

    status, passed = request_json(
        "POST",
        f"{base_url}/external/connectors/preflight",
        payload={
            "action": "external_email_send",
            "provider": "google",
            "args": {"to": ["alice@example.com"], "subject": "Status update"},
        },
    )
    assert status == 200
    assert passed["status"] == "success"
    checks = passed.get("checks", [])
    assert isinstance(checks, list)
    assert checks and isinstance(checks[0], dict)
    assert str(checks[0].get("name", "")) == "provider_available"
    orchestration = passed.get("orchestration_diagnostics", {})
    assert isinstance(orchestration, dict)
    assert str(orchestration.get("provider_requested", "")) == "google"
    route_advisor = orchestration.get("route_weight_advisor", {})
    assert isinstance(route_advisor, dict)
    assert isinstance(route_advisor.get("recommended_actions", []), list)
    cooldown_explainer = orchestration.get("cooldown_outage_explainer", {})
    assert isinstance(cooldown_explainer, dict)
    assert isinstance(cooldown_explainer.get("affected_rows", []), list)
    assert isinstance(passed.get("execution_candidates", []), list)
    assert isinstance(passed.get("approval_summary", {}), dict)
    candidate = passed["execution_candidates"][0]
    assert isinstance(candidate, dict)
    assert isinstance(candidate.get("approval_preview", {}), dict)
    assert isinstance(passed.get("advisor_simulation_template", {}), dict)


def test_external_connector_preflight_route_requires_action(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, body = request_json("POST", f"{base_url}/external/connectors/preflight", payload={"provider": "auto"})
    assert status == 400
    assert body["status"] == "error"
    assert "action is required" in str(body.get("message", ""))


def test_external_connector_preflight_simulation_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, body = request_json(
        "POST",
        f"{base_url}/external/connectors/preflight/simulate",
        payload={
            "action": "external_email_send",
            "providers": ["google", "graph", "smtp"],
            "args": {"to": ["alice@example.com"], "subject": "status"},
            "max_runs": 12,
            "scenarios": [
                {"id": "baseline"},
                {"id": "degraded_payload", "args_patch": {"subject": ""}},
            ],
        },
    )
    assert status == 200
    assert body["status"] == "success"
    assert body["action"] == "external_email_send"
    assert body["provider_count"] >= 1
    assert body["total_runs"] >= 1
    assert isinstance(body.get("results"), list)
    assert body["results"] and isinstance(body["results"][0], dict)
    assert isinstance(body.get("recommended_provider"), str)
    assert isinstance(body.get("fallback_chain"), list)
    assert isinstance(body.get("recommendation_confidence"), float)
    assert isinstance(body.get("approval_summary", {}), dict)
    assert isinstance(body.get("promotion_preview", {}), dict)
    assert isinstance(body.get("advisor_context", {}), dict)
    assert int(body.get("execution_candidate_count", 0) or 0) >= 0


def test_external_connector_preflight_simulation_route_requires_action(
    api_server: tuple[str, FakeDesktopService]
) -> None:
    base_url, _ = api_server

    status, body = request_json("POST", f"{base_url}/external/connectors/preflight/simulate", payload={"provider": "auto"})
    assert status == 400
    assert body["status"] == "error"
    assert "action is required" in str(body.get("message", ""))


def test_external_connector_preflight_simulation_templates_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, body = request_json(
        "GET",
        f"{base_url}/external/connectors/preflight/simulation/templates?action=external_email_send&provider=google",
    )
    assert status == 200
    assert body["status"] == "success"
    assert body["action"] == "external_email_send"
    assert body["provider"] == "google"
    assert isinstance(body.get("templates"), list)
    assert int(body.get("count", 0)) >= 1
    assert isinstance(body.get("recommended_template_id"), str)
    assert isinstance(body.get("execution_handoff", {}), dict)


def test_external_connector_preflight_simulation_templates_route_requires_action(
    api_server: tuple[str, FakeDesktopService]
) -> None:
    base_url, _ = api_server

    status, body = request_json(
        "GET",
        f"{base_url}/external/connectors/preflight/simulation/templates?provider=google",
    )
    assert status == 400
    assert body["status"] == "error"
    assert "action is required" in str(body.get("message", ""))


def test_external_connector_preflight_simulation_history_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, first = request_json(
        "POST",
        f"{base_url}/external/connectors/preflight/simulate",
        payload={
            "action": "external_email_send",
            "provider": "google",
            "providers": ["google", "graph"],
            "args": {"to": ["alice@example.com"], "subject": "status"},
            "max_runs": 8,
            "scenarios": [{"id": "baseline"}],
        },
    )
    assert status == 200
    assert first["status"] == "success"

    status, second = request_json(
        "POST",
        f"{base_url}/external/connectors/preflight/simulate",
        payload={
            "action": "external_email_send",
            "provider": "graph",
            "providers": ["graph", "smtp"],
            "args": {"to": ["alice@example.com"], "subject": ""},
            "max_runs": 8,
            "advisor_context": {"source": "preflight_execution_handoff", "candidate_count": 1},
            "scenarios": [
                {"id": "contract_gap", "args_patch": {"subject": ""}},
                {"id": "candidate_graph", "provider": "graph", "candidate_id": "candidate_graph", "execution_candidate": True},
            ],
        },
    )
    assert status == 200
    assert second["status"] == "success"

    status, history = request_json(
        "GET",
        f"{base_url}/external/connectors/preflight/simulations?action=external_email_send&limit=10",
    )
    assert status == 200
    assert history["status"] == "success"
    assert int(history.get("count", 0)) >= 2
    assert isinstance(history.get("items"), list)
    assert history["items"] and isinstance(history["items"][0], dict)
    assert "results" not in history["items"][0]
    assert isinstance(history["items"][0].get("advisor_context", {}), dict)
    assert isinstance(history["items"][0].get("approval_summary", {}), dict)

    status, history_raw = request_json(
        "GET",
        f"{base_url}/external/connectors/preflight/simulations?action=external_email_send&limit=1&include_results=1",
    )
    assert status == 200
    assert history_raw["status"] == "success"
    assert isinstance(history_raw.get("items"), list)
    assert history_raw["items"] and isinstance(history_raw["items"][0], dict)
    assert isinstance(history_raw["items"][0].get("results"), list)


def test_external_connector_preflight_simulation_compare_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, left = request_json(
        "POST",
        f"{base_url}/external/connectors/preflight/simulate",
        payload={
            "action": "external_email_send",
            "provider": "google",
            "providers": ["google", "graph"],
            "args": {"to": ["alice@example.com"], "subject": "Status"},
            "max_runs": 8,
            "scenarios": [{"id": "baseline"}],
        },
    )
    assert status == 200
    assert left["status"] == "success"
    left_id = str(left.get("simulation_id", ""))
    assert left_id

    status, right = request_json(
        "POST",
        f"{base_url}/external/connectors/preflight/simulate",
        payload={
            "action": "external_email_send",
            "provider": "graph",
            "providers": ["graph", "smtp"],
            "args": {"to": ["alice@example.com"], "subject": ""},
            "max_runs": 8,
            "advisor_context": {"source": "preflight_execution_handoff", "candidate_count": 1},
            "scenarios": [
                {"id": "contract_gap", "args_patch": {"subject": ""}},
                {"id": "candidate_graph", "provider": "graph", "candidate_id": "candidate_graph", "execution_candidate": True},
            ],
        },
    )
    assert status == 200
    assert right["status"] == "success"
    right_id = str(right.get("simulation_id", ""))
    assert right_id
    assert left_id != right_id

    status, compared = request_json(
        "GET",
        f"{base_url}/external/connectors/preflight/simulations/compare?left_id={left_id}&right_id={right_id}",
    )
    assert status == 200
    assert compared["status"] == "success"
    assert str(compared.get("left", {}).get("simulation_id", "")) == left_id
    assert str(compared.get("right", {}).get("simulation_id", "")) == right_id
    comparison = compared.get("comparison", {})
    assert isinstance(comparison, dict)
    assert str(comparison.get("winner", "")) in {"left", "right", "tie"}
    assert isinstance(comparison.get("advisor_replay"), bool)


def test_external_connector_preflight_simulation_compare_route_requires_ids(
    api_server: tuple[str, FakeDesktopService]
) -> None:
    base_url, _ = api_server

    status, body = request_json(
        "GET",
        f"{base_url}/external/connectors/preflight/simulations/compare?left_id=sim_fake_01",
    )
    assert status == 400
    assert body["status"] == "error"
    assert "left_id and right_id are required" in str(body.get("message", ""))


def test_external_connector_preflight_simulation_trends_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, _ = request_json(
        "POST",
        f"{base_url}/external/connectors/preflight/simulate",
        payload={
            "action": "external_email_send",
            "provider": "google",
            "providers": ["google", "graph", "smtp"],
            "args": {"to": ["alice@example.com"], "subject": "status"},
            "max_runs": 12,
            "scenarios": [{"id": "baseline"}],
        },
    )
    assert status == 200

    status, trends = request_json(
        "GET",
        (
            f"{base_url}/external/connectors/preflight/simulations/trends"
            "?action=external_email_send&limit=120&recent_window=8&baseline_window=24"
        ),
    )
    assert status == 200
    assert trends["status"] == "success"
    assert int(trends.get("count", 0)) >= 1
    assert isinstance(trends.get("recent"), dict)
    assert isinstance(trends.get("baseline"), dict)
    assert isinstance(trends.get("deltas"), dict)
    assert isinstance(trends.get("stability"), dict)
    assert isinstance(trends.get("advisor_usage"), dict)
    assert isinstance(trends.get("approval_pressure"), dict)
    assert isinstance(trends.get("execution_readiness"), dict)
    assert isinstance(trends.get("promotion_readiness"), dict)
    assert isinstance(trends.get("provider_mix"), list)
    assert str(trends.get("recommended_profile", "")) in {"strict", "balanced", "aggressive"}
    controls = trends.get("recommended_controls", {})
    assert isinstance(controls, dict)
    assert isinstance(controls.get("allow_high_risk"), bool)


def test_external_connector_preflight_simulation_promote_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _service = api_server

    status, created = request_json(
        "POST",
        f"{base_url}/external/connectors/preflight/simulate",
        payload={
            "action": "external_email_send",
            "provider": "google",
            "providers": ["google", "graph"],
            "args": {"to": ["alice@example.com"], "subject": "status"},
            "max_runs": 8,
            "advisor_context": {"source": "preflight_execution_handoff", "candidate_count": 1},
            "scenarios": [
                {"id": "baseline"},
                {"id": "candidate_google", "provider": "google", "candidate_id": "candidate_google", "execution_candidate": True},
            ],
        },
    )
    assert status == 200
    simulation_id = str(created.get("simulation_id", ""))
    assert simulation_id

    status, promoted = request_json(
        "POST",
        f"{base_url}/external/connectors/preflight/simulations/promote",
        payload={
            "simulation_id": simulation_id,
            "dry_run": False,
            "require_compare": False,
            "mission_mode": "stable",
            "reason": "integration_test_promote",
        },
    )
    assert status == 200
    assert promoted["status"] == "applied"
    assert promoted["simulation_id"] == simulation_id
    assert promoted["applied"] is True
    assert isinstance(promoted.get("promotion", {}), dict)
    assert isinstance(promoted.get("apply", {}), dict)
    assert isinstance(promoted.get("entry", {}), dict)
    assert isinstance(promoted.get("execution_contract", {}), dict)
    assert isinstance(promoted.get("execution_contract_entry", {}), dict)

    status, promotions = request_json(
        "GET",
        (
            f"{base_url}/external/connectors/preflight/simulations/promotions"
            "?action=external_email_send&provider=google&mission_mode=stable&status=applied&applied_only=1"
        ),
    )
    assert status == 200
    assert promotions["status"] == "success"
    assert int(promotions.get("count", 0)) >= 1
    items = promotions.get("items", [])
    assert isinstance(items, list)
    assert items and isinstance(items[0], dict)
    assert items[0]["simulation_id"] == simulation_id
    assert items[0]["promotion_status"] == "applied"
    assert int(items[0].get("event_id", 0) or 0) >= 1

    status, contract = request_json(
        "GET",
        (
            f"{base_url}/external/connectors/execution-contract"
            "?action=external_email_send&provider=google&mission_mode=stable&include_history=1"
        ),
    )
    assert status == 200
    assert contract["status"] == "success"
    assert isinstance(contract.get("entry", {}), dict)
    assert isinstance(contract.get("history", []), list)
    assert str(contract.get("entry", {}).get("simulation_id", "")) == simulation_id


def test_external_connector_remediation_policy_routes(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, service = api_server

    status, missing = request_json("GET", f"{base_url}/external/connectors/remediation/policy?provider=google")
    assert status == 400
    assert missing["status"] == "error"
    assert "action is required" in str(missing.get("message", ""))

    status, rec = request_json(
        "POST",
        f"{base_url}/external/connectors/remediation/policy/recommend",
        payload={
            "action": "external_email_send",
            "provider": "google",
            "mission_mode": "degraded",
            "recent_window": 10,
            "baseline_window": 40,
        },
    )
    assert status == 200
    assert rec["status"] == "success"
    recommendation = rec.get("recommendation", {})
    assert isinstance(recommendation, dict)
    assert str(recommendation.get("profile", "")) in {"strict", "balanced", "aggressive"}
    assert isinstance(recommendation.get("controls"), dict)

    status, applied = request_json(
        "POST",
        f"{base_url}/external/connectors/remediation/policy/apply",
        payload={
            "action": "external_email_send",
            "provider": "google",
            "mission_mode": "*",
            "use_recommendation": True,
            "source": "desktop-ui",
            "reason": "integration_test_apply",
        },
    )
    assert status == 200
    assert applied["status"] == "success"
    assert applied["scope_key"] == "external_email_send|google|*"
    assert applied["mission_mode"] == "*"
    entry = applied.get("entry", {})
    assert isinstance(entry, dict)
    assert str(entry.get("profile", "")) in {"strict", "balanced", "aggressive"}
    assert isinstance(entry.get("controls"), dict)
    assert entry.get("mission_mode") == "*"
    assert int(applied.get("event_id", 0) or 0) >= 1
    assert isinstance(applied.get("history_event", {}), dict)

    status, updated = request_json(
        "POST",
        f"{base_url}/external/connectors/remediation/policy/apply",
        payload={
            "action": "external_email_send",
            "provider": "google",
            "mission_mode": "*",
            "profile": "strict",
            "controls": {
                "allow_high_risk": False,
                "max_steps": 4,
                "require_compare": True,
                "stop_on_blocked": True,
            },
            "use_recommendation": False,
            "source": "desktop-ui",
            "reason": "integration_test_apply_strict",
        },
    )
    assert status == 200
    assert updated["status"] == "success"
    assert str(updated.get("entry", {}).get("profile", "")) == "strict"

    status, policy = request_json(
        "GET",
        f"{base_url}/external/connectors/remediation/policy?action=external_email_send&provider=google&mission_mode=stable&include_history=1",
    )
    assert status == 200
    assert policy["status"] == "success"
    assert policy["action"] == "external_email_send"
    assert policy["provider"] == "google"
    assert policy["mission_mode"] == "stable"
    assert isinstance(policy.get("profile"), str)
    assert isinstance(policy.get("controls"), dict)
    assert isinstance(policy.get("history"), list)
    assert isinstance(policy.get("alerts"), list)
    assert isinstance(policy.get("alert_count"), int)

    status, policies = request_json(
        "GET",
        f"{base_url}/external/connectors/remediation/policies?action=external_email_send&provider=google&mission_mode=*&include_history=1",
    )
    assert status == 200
    assert policies["status"] == "success"
    assert int(policies.get("count", 0)) >= 1
    assert isinstance(policies.get("items"), list)
    assert policies["items"] and isinstance(policies["items"][0], dict)
    assert isinstance(policies.get("history"), list)

    status, preview = request_json(
        "POST",
        f"{base_url}/external/connectors/remediation/policy/restore",
        payload={
            "event_id": int(applied.get("event_id", 0) or 0),
            "dry_run": True,
            "source": "desktop-ui",
            "reason": "integration_test_restore_preview",
        },
    )
    assert status == 200
    assert preview["status"] == "dry_run"
    assert preview["applied"] is False
    assert isinstance(preview.get("diff", {}), dict)
    assert bool(preview.get("diff", {}).get("profile_changed", False)) is True

    status, restored = request_json(
        "POST",
        f"{base_url}/external/connectors/remediation/policy/restore",
        payload={
            "event_id": int(applied.get("event_id", 0) or 0),
            "dry_run": False,
            "force": True,
            "source": "desktop-ui",
            "reason": "integration_test_restore_apply",
        },
    )
    assert status == 200
    assert restored["status"] == "applied"
    assert restored["applied"] is True
    assert isinstance(restored.get("apply", {}), dict)
    assert isinstance(restored.get("entry", {}), dict)
    assert str(restored.get("entry", {}).get("profile", "")) in {"strict", "balanced", "aggressive"}

    contract_scope_key = "external_email_send|google|stable"
    assert isinstance(service.connector_execution_contract_state.get("contracts", {}), dict)
    service.connector_execution_contract_state["contracts"][contract_scope_key] = {
        "scope_key": contract_scope_key,
        "legacy_scope_key": "external_email_send|google",
        "action": "external_email_send",
        "provider": "google",
        "mission_mode": "stable",
        "simulation_id": "sim_fake_contract",
        "compare_against_simulation_id": "",
        "selected_provider": "graph",
        "recommended_provider": "graph",
        "profile": "balanced",
        "controls": {"allow_high_risk": False, "max_steps": 6, "require_compare": False, "stop_on_blocked": True},
        "candidate_count": 1,
        "ready_execute_count": 0,
        "candidate_summary": [{"candidate_id": "candidate_graph", "route_signature": "graph:candidate_graph"}],
        "approval_summary": {},
        "recommended_tool_action": {},
        "recommended_args_patch": {},
        "fallback_chain": [],
        "promotion_score": 0.44,
        "promotion": {"eligible": True},
        "route_signature": "graph:candidate_graph",
        "updated_at": "2026-03-07T00:00:00+00:00",
        "updated_by_source": "desktop-ui",
        "reason": "manual_contract_override",
        "version": 99,
    }
    service.connector_execution_contract_state["history"] = [
        {
            "event_id": 7,
            "created_at": "2026-03-07T00:00:00+00:00",
            "event_type": "promotion",
            "scope_key": contract_scope_key,
            "legacy_scope_key": "external_email_send|google",
            "action": "external_email_send",
            "provider": "google",
            "mission_mode": "stable",
            "simulation_id": "sim_fake_03",
            "compare_against_simulation_id": "",
            "selected_provider": "google",
            "recommended_provider": "google",
            "profile": "balanced",
            "controls": {"allow_high_risk": False, "max_steps": 6, "require_compare": False, "stop_on_blocked": True},
            "candidate_count": 1,
            "ready_execute_count": 0,
            "candidate_summary": [{"candidate_id": "candidate_google", "route_signature": "google:candidate_google"}],
            "approval_summary": {},
            "recommended_tool_action": {},
            "recommended_args_patch": {},
            "fallback_chain": [],
            "promotion_score": 0.71,
            "promotion": {"eligible": True},
            "route_signature": "google:candidate_google",
            "updated_at": "2026-03-06T00:00:00+00:00",
            "updated_by_source": "desktop-ui",
            "reason": "integration_seed",
            "version": 1,
        }
    ]

    status, contract_preview = request_json(
        "POST",
        f"{base_url}/external/connectors/execution-contract/restore",
        payload={
            "event_id": 7,
            "dry_run": True,
            "source": "desktop-ui",
            "reason": "integration_test_contract_restore_preview",
        },
    )
    assert status == 200
    assert contract_preview["status"] == "dry_run"
    assert isinstance(contract_preview.get("diff", {}), dict)
    assert int(contract_preview.get("diff", {}).get("changed_field_count", 0) or 0) >= 1

    status, contract_restored = request_json(
        "POST",
        f"{base_url}/external/connectors/execution-contract/restore",
        payload={
            "event_id": 7,
            "dry_run": False,
            "force": True,
            "source": "desktop-ui",
            "reason": "integration_test_contract_restore_apply",
        },
    )
    assert status == 200
    assert contract_restored["status"] == "applied"
    assert contract_restored["applied"] is True
    assert isinstance(contract_restored.get("apply", {}), dict)
    assert isinstance(contract_restored.get("entry", {}), dict)
    assert str(contract_restored.get("entry", {}).get("selected_provider", "")) == "google"

    status, autotune_status = request_json(
        "GET",
        f"{base_url}/external/connectors/remediation/policy/autotune?limit=10",
    )
    assert status == 200
    assert autotune_status["status"] == "success"
    assert isinstance(autotune_status.get("config"), dict)
    assert isinstance(autotune_status.get("history"), list)

    status, autotune = request_json(
        "POST",
        f"{base_url}/external/connectors/remediation/policy/autotune",
        payload={
            "action": "external_email_send",
            "provider": "google",
            "mission_mode": "stable",
            "source": "desktop-ui",
            "reason": "integration_test_autotune",
            "dry_run": True,
            "recent_window": 8,
            "baseline_window": 24,
        },
    )
    assert status == 200
    assert autotune["status"] in {"dry_run", "skip", "applied"}
    assert autotune["action"] == "external_email_send"
    assert autotune["provider"] == "google"
    assert isinstance(autotune.get("decision"), dict)

    status, scan = request_json(
        "POST",
        f"{base_url}/external/connectors/remediation/policy/autotune/scan",
        payload={
            "max_pairs": 4,
            "mission_mode": "degraded",
            "dry_run": True,
            "recent_window": 8,
            "baseline_window": 24,
        },
    )
    assert status == 200
    assert scan["status"] in {"success", "skip"}
    if scan["status"] == "success":
        assert isinstance(scan.get("items"), list)
        assert int(scan.get("count", 0)) >= 1


def test_computer_click_target_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, clicked = request_json(
        "POST",
        f"{base_url}/computer/click-target",
        payload={"query": "Submit", "target_mode": "auto", "verify_mode": "state_or_visibility"},
    )
    assert status == 200
    assert clicked["status"] == "success"
    assert clicked["query"] == "Submit"
    assert clicked["method"] == "accessibility"

    status, error_body = request_json("POST", f"{base_url}/computer/click-target", payload={})
    assert status == 400
    assert "query is required" in str(error_body.get("message", ""))


def test_runtime_circuit_breakers_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, body = request_json("GET", f"{base_url}/runtime/circuit-breakers?limit=20")
    assert status == 200
    assert body["status"] == "success"
    assert body["count"] >= 1
    assert body["items"][0]["action"] == "browser_read_dom"


def test_rust_health_and_request_routes(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, health = request_json("GET", f"{base_url}/rust/health")
    assert status == 200
    assert health["status"] == "success"
    assert health["available"] is True

    status, requested = request_json(
        "POST",
        f"{base_url}/rust/request",
        payload={"event": "health_check", "payload": {}},
    )
    assert status == 200
    assert requested["status"] == "success"
    assert requested["event"] == "health_check"
    assert requested.get("data", {}).get("status") == "ok"

    status, safety = request_json(
        "POST",
        f"{base_url}/rust/request",
        payload={"event": "safety_status", "payload": {}},
    )
    assert status == 200
    assert safety["status"] == "success"
    assert safety["event"] == "safety_status"
    assert safety.get("data", {}).get("safe_mode") is True


def test_rust_request_route_validates_event(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, missing = request_json("POST", f"{base_url}/rust/request", payload={})
    assert status == 400
    assert missing["status"] == "error"
    assert "event is required" in str(missing.get("message", ""))

    status, denied = request_json(
        "POST",
        f"{base_url}/rust/request",
        payload={"event": "file_write_text", "payload": {"path": "notes.txt", "text": "x"}},
    )
    assert status == 400
    assert denied["status"] == "error"
    assert "not allowed" in str(denied.get("message", ""))


def test_rust_request_route_supports_audio_and_automation_events(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, audio = request_json(
        "POST",
        f"{base_url}/rust/request",
        payload={"event": "audio_probe", "payload": {"path": "voice.wav"}},
    )
    assert status == 200
    assert audio["status"] == "success"
    assert audio["event"] == "audio_probe"
    assert audio.get("data", {}).get("format") == "wav"

    status, automation = request_json(
        "POST",
        f"{base_url}/rust/request",
        payload={
            "event": "automation_plan_execute",
            "payload": {"tasks": [{"id": "step-1"}, {"id": "step-2", "depends_on": ["step-1"]}]},
        },
    )
    assert status == 200
    assert automation["status"] == "success"
    assert automation["event"] == "automation_plan_execute"
    assert automation.get("data", {}).get("completed") == 2


def test_rust_request_route_supports_desktop_context_and_batch(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, context_payload = request_json(
        "POST",
        f"{base_url}/rust/request",
        payload={"event": "desktop_context", "payload": {}},
    )
    assert status == 200
    assert context_payload["status"] == "success"
    assert context_payload["event"] == "desktop_context"
    data = context_payload.get("data", {})
    assert data.get("status") == "success"
    assert isinstance(data.get("system"), dict)
    assert isinstance(data.get("input"), dict)

    status, batch_payload = request_json(
        "POST",
        f"{base_url}/rust/request",
        payload={
            "event": "batch_execute",
            "payload": {
                "continue_on_error": True,
                "requests": [
                    {"event": "echo", "payload": {"x": 1}},
                    {"event": "unsupported_event", "payload": {}},
                    {"event": "safety_status", "payload": {}},
                ],
            },
        },
    )
    assert status == 200
    assert batch_payload["status"] == "success"
    assert batch_payload["event"] == "batch_execute"
    batch_data = batch_payload.get("data", {})
    assert batch_data.get("status") in {"partial", "success"}
    assert int(batch_data.get("count", 0)) >= 2


def test_rust_diagnostics_and_specialized_routes(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, diagnostics = request_json("GET", f"{base_url}/rust/diagnostics")
    assert status == 200
    assert diagnostics["status"] == "success"
    runtime = diagnostics.get("runtime", {})
    assert runtime.get("running") is True
    assert isinstance(runtime.get("requests_total"), int)

    status, desktop_context = request_json(
        "POST",
        f"{base_url}/rust/desktop-context",
        payload={"timeout_s": 7.5},
    )
    assert status == 200
    assert desktop_context["status"] == "success"
    assert desktop_context["event"] == "desktop_context"

    status, batch_result = request_json(
        "POST",
        f"{base_url}/rust/batch-execute",
        payload={
            "requests": [
                {"event": "echo", "payload": {"x": 1}},
                {"event": "unsupported_event", "payload": {}},
            ],
            "continue_on_error": True,
            "include_timing": True,
            "max_steps": 12,
            "timeout_s": 10.0,
        },
    )
    assert status == 200
    assert batch_result["status"] == "success"
    assert batch_result["event"] == "batch_execute"
    assert int(batch_result.get("data", {}).get("count", 0)) == 2

    status, automation_result = request_json(
        "POST",
        f"{base_url}/rust/automation-plan",
        payload={
            "tasks": [{"id": "step-1"}, {"id": "step-2", "depends_on": ["step-1"]}],
            "options": {"max_parallel": 2, "fail_fast": False},
            "timeout_s": 30.0,
        },
    )
    assert status == 200
    assert automation_result["status"] == "success"
    assert automation_result["event"] == "automation_plan_execute"
    assert automation_result.get("data", {}).get("completed") == 2


def test_rust_specialized_routes_validate_required_fields(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, missing_requests = request_json("POST", f"{base_url}/rust/batch-execute", payload={})
    assert status == 400
    assert missing_requests["status"] == "error"
    assert "requests (non-empty list) is required" in str(missing_requests.get("message", ""))

    status, missing_tasks = request_json("POST", f"{base_url}/rust/automation-plan", payload={})
    assert status == 400
    assert missing_tasks["status"] == "error"
    assert "tasks (non-empty list) is required" in str(missing_tasks.get("message", ""))


def test_stt_route_transcribe_and_goal_submission(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, body = request_json(
        "POST",
        f"{base_url}/stt",
        payload={"duration_s": 3.0, "submit_goal": True, "speak_reply": True, "source": "desktop-stt"},
    )
    assert status == 200
    assert body["status"] == "success"
    assert body["text"] == "what time is it in UTC"
    assert isinstance(body.get("goal_id"), str)
    assert body.get("speak", {}).get("status") == "success"


def test_stt_route_accepts_stream_vad_parameters(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, body = request_json(
        "POST",
        f"{base_url}/stt",
        payload={
            "duration_s": 6.0,
            "stt_mode": "stream",
            "vad_frame_s": 0.25,
            "vad_energy_threshold": 0.02,
            "vad_silence_s": 1.2,
            "vad_min_speech_s": 0.5,
        },
    )
    assert status == 200
    assert body["status"] == "success"
    assert body["stt_mode"] == "stream"
    capture = body.get("capture", {})
    assert capture.get("vad_frame_s") == 0.25
    assert capture.get("vad_energy_threshold") == 0.02


def test_voice_session_routes_start_status_trigger_stop(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, started = request_json(
        "POST",
        f"{base_url}/voice/session/start",
        payload={"wakeword_enabled": False, "auto_submit": True},
    )
    assert status == 200
    assert started["status"] == "success"
    assert started["voice"]["running"] is True

    status, state = request_json("GET", f"{base_url}/voice/session")
    assert status == 200
    assert state["running"] is True

    status, triggered = request_json("POST", f"{base_url}/voice/session/trigger", payload={"trigger_type": "manual"})
    assert status == 200
    assert triggered["status"] == "success"
    assert triggered["voice"]["last_trigger_type"] == "manual"

    status, stopped = request_json("POST", f"{base_url}/voice/session/stop", payload={})
    assert status == 200
    assert stopped["status"] == "success"
    assert stopped["voice"]["running"] is False


def test_voice_session_continuous_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "POST",
        f"{base_url}/voice/session/continuous",
        payload={
            "duration_s": 20,
            "max_turns": 2,
            "stop_on_idle_s": 5,
            "stop_after": True,
            "config": {"auto_submit": True, "auto_tts": True},
        },
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["captured_turns"] >= 1
    assert isinstance(payload.get("turns"), list)
    assert payload["stop_after"] is True
    assert payload["end_reason"] in {"max_turns", "idle_timeout", "timeout", "cancelled"}
    assert isinstance(payload.get("checkpoints"), list)
    assert int(payload.get("route_policy_pause_count", 0) or 0) >= 1
    assert int(payload.get("route_policy_resume_count", 0) or 0) >= 1
    assert isinstance(payload.get("route_policy_pause_events", []), list)
    assert isinstance(payload.get("wakeword_supervision_snapshot", {}), dict)


def test_voice_session_continuous_background_routes(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, started = request_json(
        "POST",
        f"{base_url}/voice/session/continuous/start",
        payload={"duration_s": 12, "max_turns": 2, "stop_after": True},
    )
    assert status == 200
    assert started["status"] == "success"
    session_id = started.get("session_id", "")
    assert isinstance(session_id, str) and session_id

    status, listed = request_json("GET", f"{base_url}/voice/session/continuous?limit=10")
    assert status == 200
    assert listed["status"] == "success"
    assert listed["count"] >= 1

    status, fetched = request_json("GET", f"{base_url}/voice/session/continuous/{session_id}")
    assert status == 200
    assert fetched["status"] == "success"
    assert fetched["session"]["session_id"] == session_id
    assert isinstance(fetched["session"].get("wakeword_supervision_snapshot", {}), dict)

    status, cancelled = request_json(
        "POST",
        f"{base_url}/voice/session/continuous/{session_id}/cancel",
        payload={"reason": "test-cancel"},
    )
    assert status == 200
    assert cancelled["status"] == "success"
    assert cancelled["session"]["status"] == "cancelled"


def test_tts_stop_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server
    status, payload = request_json(
        "POST",
        f"{base_url}/tts/stop",
        payload={"source": "desktop-ui", "session_id": "tts-123", "provider": "local"},
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["stopped"] is True
    assert payload["session_id"] == "tts-123"
    assert payload["provider"] == "local"


def test_tts_route_accepts_provider_overrides(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server
    status, payload = request_json(
        "POST",
        f"{base_url}/tts",
        payload={
            "text": "hello from diagnostics route",
            "provider": "local",
            "voice": "zira",
            "rate": 190,
            "volume": 0.9,
            "allow_text_fallback": False,
        },
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["requested_provider"] == "local"
    assert payload["voice"] == "zira"
    assert payload["rate"] == 190
    assert payload["volume"] == 0.9
    assert payload["allow_text_fallback"] is False


def test_tts_and_voice_diagnostics_routes(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, tts_payload = request_json(
        "GET",
        f"{base_url}/tts/diagnostics?history_limit=12&requires_offline=1&privacy_mode=1&policy_profile=privacy",
    )
    assert status == 200
    assert tts_payload["status"] == "success"
    assert tts_payload["history_limit"] == 12
    assert tts_payload["recommended_provider"] == "local"
    assert isinstance(tts_payload.get("remediation_hints"), list)
    providers = tts_payload.get("providers", {})
    assert isinstance(providers, dict)
    assert "local" in providers
    assert "elevenlabs" in providers
    assert tts_payload.get("model_route", {}).get("selected_provider") == "local"
    assert isinstance(tts_payload.get("model_route", {}).get("route_policy", {}), dict)
    assert "route_policy_summary" in tts_payload
    assert "tts" in tts_payload.get("route_bundle", {}).get("selected_local_paths", {})
    assert isinstance(tts_payload.get("provider_credentials", {}).get("providers", {}), dict)

    status, voice_payload = request_json("GET", f"{base_url}/voice/diagnostics?history_limit=9")
    assert status == 200
    assert voice_payload["status"] == "success"
    assert voice_payload["history_limit"] == 9
    assert isinstance(voice_payload.get("tts"), dict)
    assert voice_payload["tts"]["status"] == "success"
    assert "providers" in voice_payload["tts"]
    assert isinstance(voice_payload["tts"].get("route_bundle", {}), dict)
    assert isinstance(voice_payload.get("stt_policy"), dict)
    assert voice_payload["stt_policy"]["status"] == "success"
    assert isinstance(voice_payload["stt_policy"].get("model_route", {}), dict)
    assert isinstance(voice_payload.get("route_policy_summary", {}), dict)
    assert isinstance(voice_payload.get("route_policy_timeline", {}), dict)
    assert isinstance(voice_payload.get("route_policy_timeline", {}).get("items", []), list)
    assert isinstance(voice_payload.get("route_policy_history", {}), dict)
    assert isinstance(voice_payload.get("route_policy_history", {}).get("diagnostics", {}), dict)
    assert isinstance(voice_payload.get("wakeword_supervision_history", {}), dict)
    assert isinstance(voice_payload.get("wakeword_supervision_history", {}).get("diagnostics", {}), dict)
    assert isinstance(voice_payload.get("wakeword_restart_history", {}), dict)
    assert isinstance(voice_payload.get("wakeword_restart_history", {}).get("diagnostics", {}), dict)
    assert "recovery_expiry_events" in voice_payload.get("wakeword_restart_history", {}).get("diagnostics", {})
    assert "exhaustion_transition_count" in voice_payload.get("wakeword_restart_history", {}).get("diagnostics", {})
    assert isinstance(voice_payload.get("tts_bridge"), dict)
    assert voice_payload["tts_bridge"]["status"] == "success"
    assert voice_payload["planner_voice_route_policy"]["mission_id"] == "mission-voice-1"


def test_voice_route_policy_timeline_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "GET",
        f"{base_url}/voice/route-policy/timeline?history_limit=12&force_refresh=1",
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["available"] is True
    assert payload["history_limit"] == 12
    assert isinstance(payload.get("items", []), list)
    assert isinstance(payload.get("current", {}), dict)
    assert isinstance(payload.get("current", {}).get("stt", {}), dict)
    assert isinstance(payload.get("current", {}).get("wakeword", {}), dict)
    assert isinstance(payload.get("diagnostics", {}), dict)


def test_voice_route_policy_history_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "GET",
        f"{base_url}/voice/route-policy/history?history_limit=24&task=wakeword&status=recovery&refresh=1",
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["limit"] == 24
    assert payload["task"] == "wakeword"
    assert payload["status_filter"] == "recovery"
    assert isinstance(payload.get("items", []), list)
    assert isinstance(payload.get("current", {}), dict)
    assert isinstance(payload.get("diagnostics", {}), dict)
    assert isinstance(payload.get("diagnostics", {}).get("timeline_buckets", []), list)


def test_voice_mission_reliability_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "GET",
        f"{base_url}/voice/mission-reliability?mission_id=mission-voice-77&limit=8",
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["mission_id"] == "mission-voice-77"
    assert payload["count"] >= 1
    assert payload["total"] >= 1
    assert isinstance(payload.get("items", []), list)
    assert isinstance(payload.get("current", {}), dict)
    assert payload.get("current", {}).get("mission_id") == "mission-voice-77"


def test_voice_wakeword_supervision_history_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "GET",
        f"{base_url}/voice/wakeword-supervision/history?history_limit=24&status=hybrid_polling&refresh=1",
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["limit"] == 24
    assert payload["status_filter"] == "hybrid_polling"
    assert isinstance(payload.get("items", []), list)
    assert isinstance(payload.get("current", {}), dict)
    assert isinstance(payload.get("diagnostics", {}), dict)
    assert isinstance(payload.get("diagnostics", {}).get("timeline_buckets", []), list)


def test_voice_wakeword_restart_history_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "GET",
        f"{base_url}/voice/wakeword-restart/history?history_limit=24&event_type=restart_backoff&refresh=1",
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["limit"] == 24
    assert payload["event_type_filter"] == "restart_backoff"
    assert isinstance(payload.get("items", []), list)
    assert isinstance(payload.get("current", {}), dict)
    assert isinstance(payload.get("diagnostics", {}), dict)
    assert isinstance(payload.get("diagnostics", {}).get("timeline_buckets", []), list)
    diagnostics = payload.get("diagnostics", {})
    assert "recovery_expiry_events" in diagnostics
    assert "exhaustion_transition_count" in diagnostics


def test_voice_wakeword_restart_policy_history_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "GET",
        f"{base_url}/voice/wakeword-restart/policy-history?history_limit=24&refresh=1",
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["limit"] == 24
    assert isinstance(payload.get("items", []), list)
    assert isinstance(payload.get("current", {}), dict)
    assert isinstance(payload.get("diagnostics", {}), dict)
    diagnostics = payload.get("diagnostics", {})
    assert "avg_threshold_bias" in diagnostics
    assert "avg_cooldown_scale" in diagnostics
    assert "drift_score" in diagnostics
    assert "recommended_profile" in diagnostics
    assert isinstance(diagnostics.get("profile_shift_timeline", []), list)
    assert isinstance(diagnostics.get("runtime_posture", {}), dict)
    assert str(diagnostics.get("runtime_posture", {}).get("runtime_mode", "")) == "recovered_wakeword"
    assert isinstance(diagnostics.get("timeline_buckets", []), list)


def test_local_neural_tts_bridge_routes(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, bridge_payload = request_json("GET", f"{base_url}/tts/local-neural/bridge?probe=1")
    assert status == 200
    assert bridge_payload["status"] == "success"
    assert bridge_payload["endpoint_configured"] is True

    status, started = request_json(
        "POST",
        f"{base_url}/tts/local-neural/bridge/start",
        payload={"wait_ready": True, "timeout_s": 22, "reason": "voice_panel", "force": True},
    )
    assert status == 200
    assert started["status"] == "success"
    assert started["running"] is True
    assert started["force"] is True

    status, probed = request_json(
        "POST",
        f"{base_url}/tts/local-neural/bridge/probe",
        payload={"force": True},
    )
    assert status == 200
    assert probed["status"] == "success"
    assert probed["force"] is True

    status, stopped = request_json(
        "POST",
        f"{base_url}/tts/local-neural/bridge/stop",
        payload={"reason": "voice_panel"},
    )
    assert status == 200
    assert stopped["status"] == "success"
    assert stopped["running"] is False


def test_tts_policy_routes_status_and_update(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "GET",
        f"{base_url}/tts/policy?limit=30&risk_level=high&policy_profile=privacy&privacy_mode=1",
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["recommended_provider"] == "local"
    assert isinstance(payload.get("route_bias"), dict)

    status, updated = request_json(
        "POST",
        f"{base_url}/tts/policy",
        payload={
            "enabled": True,
            "learning_enabled": True,
            "alpha": 0.31,
            "failure_weight": 2.7,
            "latency_weight": 0.55,
            "route_bias": {"local": 0.42, "elevenlabs": 0.12},
            "recommended_provider": "local",
        },
    )
    assert status == 200
    assert updated["status"] == "success"
    assert updated["updated"] is True
    assert updated["policy"]["route_bias"]["local"] == 0.42
    assert updated["policy"]["recommended_provider"] == "local"


def test_stt_policy_routes_status_and_update(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "GET",
        f"{base_url}/stt/policy?history_limit=90&source=desktop-ui-voice-panel",
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["available"] is True
    assert payload["history_limit"] == 90
    assert payload["provider_failure_streak_threshold"] == 3
    assert isinstance(payload.get("providers"), dict)
    assert isinstance(payload.get("autotune"), dict)
    assert isinstance(payload.get("model_route", {}), dict)
    assert isinstance(payload.get("route_policy_summary", {}), dict)
    assert isinstance(payload.get("route_bundle", {}), dict)

    status, updated = request_json(
        "POST",
        f"{base_url}/stt/policy",
        payload={
            "source": "desktop-ui-voice-panel",
            "provider_failure_streak_threshold": 4,
            "provider_cooldown_s": 14.0,
            "policy_failure_streak_threshold": 5,
            "providers": {"groq": {"enabled": False}},
            "autotune_enabled": True,
            "autotune_alpha": 0.3,
            "autotune_min_samples": 11,
            "autotune_bad_threshold": 0.62,
            "autotune_good_threshold": 0.27,
            "autotune_apply_cooldown_s": 12,
            "autotune_persist_every": 5,
        },
    )
    assert status == 200
    assert updated["status"] == "success"
    assert updated["updated"] is True
    assert updated["changed"]["provider_failure_streak_threshold"] == 4
    assert updated["changed"]["provider_cooldown_s"] == 14.0
    assert updated["policy"]["providers"]["groq"]["enabled"] is False
    assert updated["policy"]["autotune"]["enabled"] is True
    assert float(updated["policy"]["autotune"]["alpha"]) == 0.3
    assert int(updated["policy"]["autotune"]["min_samples"]) == 11


def test_context_routes_status_snapshot_activity_patterns_and_lifecycle(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, current = request_json("GET", f"{base_url}/context/status")
    assert status == 200
    assert current["status"] == "success"
    assert current["running"] is False

    status, started = request_json(
        "POST",
        f"{base_url}/context/start",
        payload={
            "monitoring_interval_s": 2.5,
            "pattern_detection_enabled": True,
            "proactive_suggestions_enabled": False,
        },
    )
    assert status == 200
    assert started["status"] == "success"
    assert started["running"] is True
    assert float(started.get("monitoring_interval_s", 0.0)) == 2.5
    assert started["proactive_suggestions_enabled"] is False

    status, snapshot = request_json("GET", f"{base_url}/context/snapshot")
    assert status == 200
    assert snapshot["status"] == "success"
    assert snapshot["has_snapshot"] is True
    assert snapshot["snapshot"]["active_application"] == "notepad"

    status, activity = request_json("GET", f"{base_url}/context/activity?duration_minutes=90")
    assert status == 200
    assert activity["status"] == "success"
    assert activity["duration_minutes"] == 90

    status, patterns = request_json("GET", f"{base_url}/context/patterns?limit=5")
    assert status == 200
    assert patterns["status"] == "success"
    assert patterns["count"] == 1
    assert patterns["items"][0]["pattern_id"] == "pattern-1"

    status, opportunities = request_json("GET", f"{base_url}/context/opportunities?limit=3")
    assert status == 200
    assert opportunities["status"] == "success"
    assert opportunities["count"] == 1
    assert opportunities["items"][0]["opportunity_id"] == "opp-1"

    status, stopped = request_json("POST", f"{base_url}/context/stop", payload={})
    assert status == 200
    assert stopped["status"] == "success"
    assert stopped["running"] is False


def test_context_opportunity_policy_records_runs_and_manual_execute(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, policy = request_json("GET", f"{base_url}/context/opportunities/policy")
    assert status == 200
    assert policy["status"] == "success"
    assert policy["autorun"] is False

    status, updated = request_json(
        "POST",
        f"{base_url}/context/opportunities/policy",
        payload={
            "autorun": True,
            "min_priority": 6,
            "min_confidence": 0.8,
            "cooldown_s": 45,
            "max_workers": 3,
            "fairness_window_s": 120,
            "per_type_max_in_window": 2,
            "class_weights": {"automation": 1.2, "recovery": 1.4},
            "multiobjective_enabled": True,
            "deadline_weight": 3.0,
            "starvation_override_s": 90,
            "self_tune_enabled": True,
        },
    )
    assert status == 200
    assert updated["status"] == "success"
    assert updated["autorun"] is True
    assert updated["min_priority"] == 6
    assert updated["max_workers"] == 3
    assert updated["fairness_window_s"] == 120
    assert updated["per_type_max_in_window"] == 2
    assert float(updated["class_weights"]["automation"]) == 1.2
    assert updated["multiobjective_enabled"] is True
    assert updated["deadline_weight"] == 3.0
    assert updated["starvation_override_s"] == 90

    status, records = request_json("GET", f"{base_url}/context/opportunities/records?limit=10")
    assert status == 200
    assert records["status"] == "success"
    assert records["count"] >= 1
    assert records["items"][0]["opportunity_id"] == "opp-1"

    status, runs_before = request_json("GET", f"{base_url}/context/opportunities/runs?limit=10")
    assert status == 200
    assert runs_before["status"] == "success"

    status, executed = request_json(
        "POST",
        f"{base_url}/context/opportunities/execute",
        payload={"opportunity_id": "opp-1", "reason": "manual_test", "force": True, "wait": True},
    )
    assert status == 200
    assert executed["status"] == "success"
    assert executed["run"]["opportunity_id"] == "opp-1"

    status, runs_after = request_json("GET", f"{base_url}/context/opportunities/runs?limit=10")
    assert status == 200
    assert runs_after["status"] == "success"
    assert runs_after["count"] >= 1
    assert runs_after["items"][0]["opportunity_id"] == "opp-1"

    status, contract = request_json(
        "POST",
        f"{base_url}/context/opportunities/contract",
        payload={"opportunity_id": "opp-1"},
    )
    assert status == 200
    assert contract["status"] == "success"
    assert contract["contract"]["status"] == "success"
    assert contract["contract"]["ready"] is True


def test_rbac_status_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server
    status, payload = request_json("GET", f"{base_url}/rbac/status")
    assert status == 200
    assert payload["status"] == "success"
    assert payload["default_role"] == "developer"
    assert payload["mission_rbac_enabled"] is True
    assert payload["mission_autonomy_adapt_enabled"] is True


def test_rbac_autonomy_learning_routes(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server
    status, payload = request_json("GET", f"{base_url}/rbac/autonomy-learning?limit=20")
    assert status == 200
    assert payload["status"] == "success"
    assert payload["enabled"] is True
    assert isinstance(payload.get("dynamic_profile_by_risk"), dict)

    status, reset = request_json("POST", f"{base_url}/rbac/autonomy-learning/reset", payload={})
    assert status == 200
    assert reset["status"] == "success"


def test_desktop_state_routes_latest_history_diff(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, latest = request_json("GET", f"{base_url}/state/latest")
    assert status == 200
    assert latest["status"] == "success"
    assert "state_hash" in latest

    status, history = request_json("GET", f"{base_url}/state/history?limit=5&include_normalized=1")
    assert status == 200
    assert history["status"] == "success"
    assert history["count"] >= 1
    assert "normalized" in history["items"][0]

    status, diff = request_json(
        "POST",
        f"{base_url}/state/diff",
        payload={"from_hash": "hash_1", "to_hash": "hash_2"},
    )
    assert status == 200
    assert diff["status"] == "success"
    assert diff["change_count"] >= 1


def test_desktop_anchor_routes_status_and_quarantine_reset(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, anchors = request_json("GET", f"{base_url}/runtime/desktop-anchors?action=computer_click_target&query=submit")
    assert status == 200
    assert anchors["status"] == "success"
    assert anchors["count"] >= 1
    assert int(anchors.get("quarantine_count", 0) or 0) >= 1

    status, quarantine = request_json(
        "GET",
        f"{base_url}/runtime/desktop-anchors/quarantine?action=computer_click_target&query=submit",
    )
    assert status == 200
    assert quarantine["status"] == "success"
    assert quarantine["count"] >= 1

    status, cleared = request_json(
        "POST",
        f"{base_url}/runtime/desktop-anchors/quarantine/reset",
        payload={"action": "computer_click_target", "query": "submit"},
    )
    assert status == 200
    assert cleared["status"] == "success"
    assert int(cleared.get("removed", 0) or 0) >= 1


def test_desktop_workflow_memory_routes_status_and_reset(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, workflows = request_json(
        "GET",
        f"{base_url}/runtime/desktop-workflows?action=command&app_name=vscode&intent=settings",
    )
    assert status == 200
    assert workflows["status"] == "success"
    assert workflows["count"] == 1
    assert workflows["items"][0]["profile_id"] == "microsoft-visual-studio-code"
    assert isinstance(workflows.get("summary", {}), dict)

    status, cleared = request_json(
        "POST",
        f"{base_url}/runtime/desktop-workflows/reset",
        payload={"action": "command", "app_name": "vscode"},
    )
    assert status == 200
    assert cleared["status"] == "success"
    assert int(cleared.get("removed", 0) or 0) >= 1

    status, remaining = request_json(
        "GET",
        f"{base_url}/runtime/desktop-workflows?action=terminal_command&app_name=powershell",
    )
    assert status == 200
    assert remaining["status"] == "success"
    assert remaining["count"] == 1
    assert remaining["items"][0]["profile_id"] == "powershell"


def test_desktop_evaluation_catalog_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "GET",
        (
            f"{base_url}/runtime/evaluations/desktop-benchmarks"
            "?pack=unsupported_and_recovery&mission_family=exploration&app_name=settings"
        ),
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["count"] == 1
    assert payload["items"][0]["name"] == "unsupported_child_dialog_chain"
    assert payload["filters"]["pack"] == "unsupported_and_recovery"
    assert payload["summary"]["mission_family_counts"]["exploration"] == 1
    assert payload["summary"]["recovery_expected_count"] == 1
    assert payload["summary"]["native_hybrid_focus_count"] == 1
    assert payload["latest_run"]["status"] == "success"


def test_desktop_evaluation_run_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "POST",
        f"{base_url}/runtime/evaluations/desktop-benchmarks/run",
        payload={
            "pack": "installer_and_governance",
            "risk_level": "high",
            "mission_family": "recovery",
            "app_name": "installer",
            "limit": 4,
        },
    )
    assert status == 200
    assert payload["status"] == "success"
    assert len(payload["items"]) == 1
    assert payload["items"][0]["scenario"] == "installer_resume_after_prompt"
    assert payload["summary"]["count"] == 1
    assert payload["summary"]["recovery_readiness"]["weight"] > 0
    assert payload["summary"]["native_hybrid_coverage"]["weight"] > 0
    assert payload["regression"]["status"] == "stable"
    assert payload["filters"]["risk_level"] == "high"
    assert payload["filters"]["mission_family"] == "recovery"


def test_desktop_evaluation_history_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "GET",
        f"{base_url}/runtime/evaluations/desktop-benchmarks/history?limit=3",
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["count"] >= 1
    assert payload["limit"] == 3
    assert payload["latest_run"]["status"] == "success"
    assert payload["items"][0]["scenario_count"] >= 1


def test_desktop_evaluation_guidance_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "GET",
        f"{base_url}/runtime/evaluations/desktop-benchmarks/guidance",
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["benchmark_ready"] is True
    assert payload["weakest_pack"] == "unsupported_and_recovery"
    assert payload["weakest_capability"] == "surface_exploration"
    assert payload["control_biases"]["dialog_resolution"] > 0.8


def test_desktop_evaluation_native_targets_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "GET",
        (
            f"{base_url}/runtime/evaluations/desktop-benchmarks/native-targets"
            "?pack=unsupported_and_recovery&app_name=settings&limit=4&history_limit=3"
        ),
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["benchmark_ready"] is True
    assert payload["target_apps"][0]["app_name"] == "settings"
    assert payload["filters"]["pack"] == "unsupported_and_recovery"
    assert payload["strongest_tactics"]["descendant_focus"] > 0.8


def test_desktop_evaluation_lab_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "GET",
        (
            f"{base_url}/runtime/evaluations/desktop-benchmarks/lab"
            "?pack=long_horizon_and_replay&history_limit=4"
        ),
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["filters"]["pack"] == "long_horizon_and_replay"
    assert payload["coverage"]["long_horizon"]["count"] >= 1
    assert payload["history_trend"]["run_count"] >= 1
    assert payload["replay_candidates"][0]["replay_query"]["scenario_name"] == "vscode_long_horizon_debug_loop"
    assert payload["installed_app_coverage"]["benchmarked_installed_app_count"] >= 1


def test_desktop_evaluation_lab_sessions_routes(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, created = request_json(
        "POST",
        f"{base_url}/runtime/evaluations/desktop-benchmarks/lab/sessions",
        payload={
            "pack": "unsupported_and_recovery",
            "mission_family": "exploration",
            "app_name": "settings",
            "history_limit": 4,
            "source": "http_test",
        },
    )
    assert status == 200
    assert created["status"] == "success"
    assert created["session"]["session_id"].startswith("benchlab-test-")
    assert created["session"]["replay_candidate_count"] >= 1
    assert created["native_targets"]["target_apps"][0]["app_name"] == "settings"

    status, sessions = request_json(
        "GET",
        f"{base_url}/runtime/evaluations/desktop-benchmarks/lab/sessions?limit=4",
    )
    assert status == 200
    assert sessions["status"] == "success"
    assert sessions["count"] >= 1
    assert sessions["latest_session"]["session_id"] == created["session"]["session_id"]
    assert sessions["summary"]["pending_replays"] >= 1


def test_desktop_evaluation_lab_session_replay_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, created = request_json(
        "POST",
        f"{base_url}/runtime/evaluations/desktop-benchmarks/lab/sessions",
        payload={
            "pack": "long_horizon_and_replay",
            "app_name": "vscode",
        },
    )
    assert status == 200
    session_id = str(created["session"]["session_id"])
    scenario_name = str(created["session"]["replay_candidates"][0]["scenario"])

    status, replayed = request_json(
        "POST",
        f"{base_url}/runtime/evaluations/desktop-benchmarks/lab/sessions/replay",
        payload={
            "session_id": session_id,
            "scenario_name": scenario_name,
        },
    )
    assert status == 200
    assert replayed["status"] == "success"
    assert replayed["session"]["session_id"] == session_id
    assert replayed["updated_candidate"]["scenario"] == scenario_name
    assert replayed["updated_candidate"]["replay_status"] == "completed"
    assert replayed["replay_result"]["status"] == "success"


def test_desktop_mission_routes_status_and_reset(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, missions = request_json(
        "GET",
        f"{base_url}/runtime/desktop-missions?status=paused&mission_kind=wizard&app_name=installer",
    )
    assert status == 200
    assert missions["status"] == "success"
    assert missions["count"] == 1
    assert missions["items"][0]["mission_id"] == "dm_pause_wizard_1"
    assert missions["items"][0]["resume_contract"]["mission_id"] == "dm_pause_wizard_1"
    assert missions["filters"]["mission_kind"] == "wizard"
    assert missions["status_counts"] == {"paused": 1}
    assert missions["mission_kind_counts"] == {"wizard": 1}
    assert missions["approval_kind_counts"] == {"elevation_consent": 1}
    assert missions["recovery_profile_counts"] == {"admin_review": 1}
    assert missions["app_counts"] == {"installer": 1}
    assert missions["stop_reason_counts"] == {"elevation_consent_required": 1}
    assert missions["resume_ready_count"] == 0
    assert missions["manual_attention_count"] == 1
    assert missions["latest_paused"]["mission_id"] == "dm_pause_wizard_1"
    assert missions["items"][0]["recovery_profile"] == "admin_review"
    assert missions["items"][0]["resume_ready"] is False

    status, cleared = request_json(
        "POST",
        f"{base_url}/runtime/desktop-missions/reset",
        payload={"mission_kind": "form", "status": "completed"},
    )
    assert status == 200
    assert cleared["status"] == "success"
    assert int(cleared.get("removed", 0) or 0) == 1

    status, remaining = request_json("GET", f"{base_url}/runtime/desktop-missions?limit=10")
    assert status == 200
    assert remaining["status"] == "success"
    assert remaining["count"] == 1
    assert remaining["items"][0]["mission_id"] == "dm_pause_wizard_1"
    assert remaining["status_counts"] == {"paused": 1}
    assert remaining["recovery_profile_counts"] == {"admin_review": 1}


def test_desktop_recovery_daemon_routes_status_update_and_trigger(
    api_server: tuple[str, FakeDesktopService]
) -> None:
    base_url, _ = api_server

    status, initial = request_json("GET", f"{base_url}/runtime/desktop-missions/recovery-daemon")
    assert status == 200
    assert initial["status"] == "success"
    assert initial["enabled"] is False
    assert initial["snapshot"]["status"] == "success"
    assert initial["snapshot"]["count"] == 1

    status, updated = request_json(
        "POST",
        f"{base_url}/runtime/desktop-missions/recovery-daemon",
        payload={
            "enabled": True,
            "interval_s": 30,
            "limit": 6,
            "max_auto_resumes": 1,
            "policy_profile": "power",
            "mission_status": "paused",
            "mission_kind": "wizard",
            "app_name": "installer",
            "resume_force": True,
        },
    )
    assert status == 200
    assert updated["status"] == "success"
    assert updated["enabled"] is True
    assert updated["interval_s"] == 30.0
    assert updated["limit"] == 6
    assert updated["max_auto_resumes"] == 1
    assert updated["policy_profile"] == "power"
    assert updated["allow_high_risk"] is True
    assert updated["allow_critical_risk"] is True
    assert updated["mission_kind"] == "wizard"
    assert updated["app_name"] == "installer"
    assert updated["resume_force"] is True

    status, triggered = request_json(
        "POST",
        f"{base_url}/runtime/desktop-missions/recovery-daemon/trigger",
        payload={
            "limit": 4,
            "max_auto_resumes": 1,
            "policy_profile": "conservative",
            "mission_status": "paused",
            "mission_kind": "wizard",
            "app_name": "installer",
        },
    )
    assert status == 200
    assert triggered["status"] == "idle"
    assert triggered["supervisor"]["status"] == "success"
    assert triggered["supervisor"]["run_count"] == 1
    assert triggered["supervisor"]["manual_trigger_count"] == 1
    assert triggered["supervisor"]["snapshot"]["status"] == "success"
    assert triggered["supervisor"]["watchdog_history"]["count"] == 1
    assert triggered["result"]["policy_profile"] == "conservative"


def test_desktop_recovery_daemon_history_routes(
    api_server: tuple[str, FakeDesktopService]
) -> None:
    base_url, _ = api_server

    status, _ = request_json(
        "POST",
        f"{base_url}/runtime/desktop-missions/recovery-daemon/trigger",
        payload={
            "limit": 4,
            "max_auto_resumes": 1,
            "mission_status": "paused",
            "mission_kind": "wizard",
            "app_name": "installer",
        },
    )
    assert status == 200

    status, history = request_json(
        "GET",
        f"{base_url}/runtime/desktop-missions/recovery-daemon/history?limit=4&app_name=installer&mission_kind=wizard",
    )
    assert status == 200
    assert history["status"] == "success"
    assert history["count"] == 1
    assert history["latest_run"]["app_name"] == "installer"
    assert history["latest_run"]["mission_kind"] == "wizard"

    status, reset_payload = request_json(
        "POST",
        f"{base_url}/runtime/desktop-missions/recovery-daemon/history/reset",
        payload={"app_name": "installer", "mission_kind": "wizard"},
    )
    assert status == 200
    assert reset_payload["status"] == "success"
    assert reset_payload["removed"] == 1

    status, empty_history = request_json(
        "GET",
        f"{base_url}/runtime/desktop-missions/recovery-daemon/history?limit=4",
    )
    assert status == 200
    assert empty_history["count"] == 0


def test_desktop_governance_routes_status_and_update(
    api_server: tuple[str, FakeDesktopService]
) -> None:
    base_url, _ = api_server

    status, initial = request_json("GET", f"{base_url}/runtime/desktop-governance")
    assert status == 200
    assert initial["status"] == "success"
    assert initial["policy_profile"] == "balanced"
    assert initial["allow_desktop_approval_reuse"] is True
    assert initial["desktop_recovery_daemon"]["policy_profile"] == "balanced"

    status, updated = request_json(
        "POST",
        f"{base_url}/runtime/desktop-governance",
        payload={
            "policy_profile": "power",
            "desktop_approval_reuse_window_s": 180,
            "sync_desktop_recovery_daemon": True,
        },
    )
    assert status == 200
    assert updated["status"] == "success"
    assert updated["policy_profile"] == "custom"
    assert updated["allow_high_risk"] is True
    assert updated["allow_critical_risk"] is True
    assert updated["desktop_approval_reuse_window_s"] == 180
    assert updated["desktop_recovery_daemon"]["policy_profile"] == "custom"
    assert updated["desktop_recovery_daemon"]["allow_critical_risk"] is True


def test_desktop_workflow_catalog_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "GET",
        f"{base_url}/desktop/workflows?category=browser&app_name=chrome&query=history",
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["count"] == 1
    assert payload["profile"]["category"] == "browser"
    assert payload["items"][0]["action"] == "open_history"
    assert payload["items"][0]["primary_hotkey"] == ["ctrl", "h"]


def test_desktop_action_routes_forward_wizard_flow_parameters(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, advice = request_json(
        "GET",
        f"{base_url}/desktop/action-advice?action=complete_wizard_flow&app_name=installer&max_wizard_pages=7&allow_warning_pages=1",
    )
    assert status == 200
    assert advice["status"] == "success"
    assert advice["action"] == "complete_wizard_flow"
    assert advice["app_name"] == "installer"
    assert advice["max_wizard_pages"] == 7
    assert advice["allow_warning_pages"] is True

    status, interact = request_json(
        "POST",
        f"{base_url}/desktop/interact",
        payload={
            "action": "complete_wizard_flow",
            "app_name": "installer",
            "max_wizard_pages": 8,
            "allow_warning_pages": True,
        },
    )
    assert status == 200
    assert interact["status"] == "success"
    assert interact["action"] == "complete_wizard_flow"
    assert interact["max_wizard_pages"] == 8
    assert interact["allow_warning_pages"] is True


def test_desktop_action_routes_forward_form_flow_parameters(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, advice = request_json(
        "GET",
        f"{base_url}/desktop/action-advice?action=complete_form_flow&app_name=settings&max_form_pages=6&allow_destructive_forms=1",
    )
    assert status == 200
    assert advice["status"] == "success"
    assert advice["action"] == "complete_form_flow"
    assert advice["app_name"] == "settings"
    assert advice["max_form_pages"] == 6
    assert advice["allow_destructive_forms"] is True

    status, interact = request_json(
        "POST",
        f"{base_url}/desktop/interact",
        payload={
            "action": "complete_form_flow",
            "app_name": "settings",
            "max_form_pages": 7,
            "allow_destructive_forms": True,
        },
    )
    assert status == 200
    assert interact["status"] == "success"
    assert interact["action"] == "complete_form_flow"
    assert interact["max_form_pages"] == 7
    assert interact["allow_destructive_forms"] is True


def test_desktop_action_routes_forward_mission_reference_parameters(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, advice = request_json(
        "GET",
        f"{base_url}/desktop/action-advice?action=resume_mission&mission_id=dm_pause_wizard_1&mission_kind=wizard&resume_force=1",
    )
    assert status == 200
    assert advice["status"] == "success"
    assert advice["action"] == "resume_mission"
    assert advice["mission_id"] == "dm_pause_wizard_1"
    assert advice["mission_kind"] == "wizard"
    assert advice["resume_force"] is True

    status, interact = request_json(
        "POST",
        f"{base_url}/desktop/interact",
        payload={
            "action": "resume_mission",
            "mission_id": "dm_pause_wizard_1",
            "mission_kind": "wizard",
            "resume_force": True,
        },
    )
    assert status == 200
    assert interact["status"] == "success"
    assert interact["action"] == "resume_mission"
    assert interact["mission_id"] == "dm_pause_wizard_1"
    assert interact["mission_kind"] == "wizard"
    assert interact["resume_force"] is True


def test_desktop_interact_route_forwards_resume_mission_payload(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "POST",
        f"{base_url}/desktop/interact",
        payload={
            "action": "resume_mission",
            "resume_contract": {
                "mission_kind": "wizard",
                "resume_action": "complete_wizard_flow",
                "resume_signature": "resume-1234",
                "resume_payload": {
                    "action": "complete_wizard_flow",
                    "app_name": "installer",
                    "max_wizard_pages": 6,
                },
            },
            "blocking_surface": {
                "approval_kind": "elevation_consent",
                "window_title": "User Account Control",
            },
            "resume_force": True,
        },
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["action"] == "resume_mission"
    assert payload["resume_contract"]["resume_action"] == "complete_wizard_flow"
    assert payload["resume_contract"]["resume_payload"]["app_name"] == "installer"
    assert payload["blocking_surface"]["approval_kind"] == "elevation_consent"
    assert payload["resume_force"] is True


def test_desktop_interact_route_forwards_approval_id(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "POST",
        f"{base_url}/desktop/interact",
        payload={
            "action": "complete_form_flow",
            "app_name": "settings",
            "approval_id": "approval-ticket-123",
        },
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["action"] == "complete_form_flow"
    assert payload["approval_id"] == "approval-ticket-123"


def test_desktop_interact_route_forwards_surface_exploration_flow_payload(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "POST",
        f"{base_url}/desktop/interact",
        payload={
            "action": "complete_surface_exploration_flow",
            "app_name": "settings",
            "query": "bluetooth",
            "ensure_app_launch": True,
            "focus_first": True,
            "verify_after_action": False,
            "max_strategy_attempts": 3,
            "exploration_limit": 8,
            "max_exploration_steps": 4,
            "max_branch_family_switches": 1,
            "max_branch_cascade_steps": 2,
        },
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["action"] == "complete_surface_exploration_flow"
    assert payload["app_name"] == "settings"
    assert payload["query"] == "bluetooth"
    assert payload["max_strategy_attempts"] == 3
    assert payload["exploration_limit"] == 8
    assert payload["max_exploration_steps"] == 4
    assert payload["max_branch_family_switches"] == 1
    assert payload["max_branch_cascade_steps"] == 2


def test_desktop_interact_route_forwards_nested_exploration_history(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "POST",
        f"{base_url}/desktop/interact",
        payload={
            "action": "advance_surface_exploration",
            "app_name": "settings",
            "query": "bluetooth",
            "attempted_targets": [
                {
                    "candidate_id": "row_bluetooth",
                    "selected_action": "select_list_item",
                }
            ],
            "surface_signature_history": ["surface-before", "surface-after"],
            "branch_history": [
                {
                    "transition_kind": "child_window",
                    "selected_action": "select_list_item",
                    "selected_candidate_id": "row_bluetooth",
                    "selected_candidate_label": "Bluetooth",
                    "window_title": "Bluetooth & devices",
                    "surface_path_tail": ["Devices", "Bluetooth"],
                }
            ],
        },
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["action"] == "advance_surface_exploration"
    assert payload["attempted_targets"][0]["candidate_id"] == "row_bluetooth"
    assert payload["surface_signature_history"] == ["surface-before", "surface-after"]
    assert payload["branch_history"][0]["transition_kind"] == "child_window"


def test_desktop_surface_snapshot_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "GET",
        f"{base_url}/desktop/surfaces?app_name=chrome&query=bookmark&limit=10",
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["app_profile"]["category"] == "browser"
    assert payload["elements"]["count"] >= 1
    assert payload["query_targets"][0]["name"] == "History"
    assert payload["selection_candidates"][0]["name"] == "History"
    assert payload["control_inventory"]["document"] == 1
    assert payload["target_group_state"]["option_count"] >= 1
    assert payload["surface_flags"]["window_targeted"] is True
    assert any(item["action"] == "open_bookmarks" for item in payload["workflow_surfaces"])
    assert "navigate" in payload["recommended_actions"]


def test_desktop_surface_exploration_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, payload = request_json(
        "GET",
        f"{base_url}/desktop/surfaces/exploration?app_name=explorer&query=Documents&limit=6",
    )
    assert status == 200
    assert payload["status"] == "success"
    assert payload["surface_mode"] == "list_navigation"
    assert payload["automation_ready"] is True
    assert payload["hypothesis_count"] == 1
    assert payload["top_hypotheses"][0]["label"] == "Documents"
    assert payload["top_hypotheses"][0]["suggested_action"] == "select_list_item"
    assert payload["top_path"][0]["action"] == payload["top_hypotheses"][0]["suggested_action"]


def test_oauth_token_routes_upsert_list_refresh_revoke(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, upserted = request_json(
        "POST",
        f"{base_url}/oauth/tokens/upsert",
        payload={
            "provider": "google",
            "account_id": "default",
            "access_token": "token-1",
            "refresh_token": "refresh-1",
            "expires_in_s": 3600,
            "scopes": ["email"],
        },
    )
    assert status == 200
    assert upserted["status"] == "success"

    status, listed = request_json("GET", f"{base_url}/oauth/tokens?provider=google&account_id=default")
    assert status == 200
    assert listed["status"] == "success"
    assert listed["count"] == 1

    status, refreshed = request_json(
        "POST",
        f"{base_url}/oauth/tokens/refresh",
        payload={"provider": "google", "account_id": "default"},
    )
    assert status == 200
    assert refreshed["status"] == "success"

    status, maintained = request_json(
        "POST",
        f"{base_url}/oauth/tokens/maintain",
        payload={"provider": "google", "account_id": "default", "refresh_window_s": 120, "dry_run": False},
    )
    assert status == 200
    assert maintained["status"] == "success"
    assert maintained["candidate_count"] == 1
    assert maintained["refreshed_count"] == 1

    status, maintenance_state = request_json("GET", f"{base_url}/oauth/tokens/maintenance")
    assert status == 200
    assert maintenance_state["status"] == "success"
    assert maintenance_state["candidate_count"] == 1
    assert maintenance_state["refreshed_count"] == 1

    status, revoked = request_json(
        "POST",
        f"{base_url}/oauth/tokens/revoke",
        payload={"provider": "google", "account_id": "default"},
    )
    assert status == 200
    assert revoked["status"] == "success"


def test_oauth_authorize_exchange_and_flow_status_routes(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, providers = request_json("GET", f"{base_url}/oauth/providers")
    assert status == 200
    assert providers["status"] == "success"
    assert providers["count"] >= 1

    status, started = request_json(
        "POST",
        f"{base_url}/oauth/authorize",
        payload={"provider": "google", "account_id": "default", "scopes": ["openid", "email"]},
    )
    assert status == 200
    assert started["status"] == "success"
    flow = started.get("flow", {})
    session_id = str(flow.get("session_id", ""))
    state = str(flow.get("state", ""))
    assert session_id
    assert state
    assert str(started.get("authorize_url", "")).startswith("https://")

    status, pending = request_json("GET", f"{base_url}/oauth/flows/{session_id}")
    assert status == 200
    assert pending["status"] == "success"
    assert pending["flow"]["status"] == "pending"

    status, exchanged = request_json(
        "POST",
        f"{base_url}/oauth/exchange",
        payload={"session_id": session_id, "code": "fake-code"},
    )
    assert status == 200
    assert exchanged["status"] == "success"
    assert exchanged["flow"]["status"] == "completed"

    callback_request = urllib.request.Request(
        url=f"{base_url}/oauth/callback?state={state}&code=fake-code-2",
        method="GET",
    )
    with urllib.request.urlopen(callback_request, timeout=6) as response:
        assert "text/html" in str(response.headers.get("Content-Type", ""))
        html = response.read().decode("utf-8")
    assert "OAuth Callback" in html


def test_browser_session_routes_create_request_dom_links_close(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, created = request_json(
        "POST",
        f"{base_url}/browser/sessions",
        payload={"name": "qa-session", "base_url": "https://example.com"},
    )
    assert status == 200
    assert created["status"] == "success"
    session_id = created["session"]["session_id"]

    status, listed = request_json("GET", f"{base_url}/browser/sessions")
    assert status == 200
    assert listed["status"] == "success"
    assert listed["count"] >= 1

    status, requested = request_json(
        "POST",
        f"{base_url}/browser/sessions/{session_id}/request",
        payload={"url": "https://example.com/api", "method": "GET"},
    )
    assert status == 200
    assert requested["status"] == "success"
    assert requested["response"]["status_code"] == 200

    status, dom = request_json(
        "POST",
        f"{base_url}/browser/sessions/{session_id}/read-dom",
        payload={"url": "https://example.com"},
    )
    assert status == 200
    assert dom["status"] == "success"
    assert dom["title"] == "Fake Session Page"

    status, links = request_json(
        "POST",
        f"{base_url}/browser/sessions/{session_id}/extract-links",
        payload={"url": "https://example.com"},
    )
    assert status == 200
    assert links["status"] == "success"
    assert links["count"] == 1

    status, closed = request_json("POST", f"{base_url}/browser/sessions/{session_id}/close", payload={})
    assert status == 200
    assert closed["status"] == "success"


def test_mission_routes_list_get_resume(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, goal = request_json("POST", f"{base_url}/goals", payload={"text": "long running task"})
    assert status == 202
    goal_id = goal["goal_id"]

    status, goal_body = request_json("GET", f"{base_url}/goals/{goal_id}")
    assert status == 200
    mission_id = str(goal_body.get("mission_id", ""))
    assert mission_id

    status, listed = request_json("GET", f"{base_url}/missions")
    assert status == 200
    assert listed["status"] == "success"
    assert listed["count"] >= 1

    status, fetched = request_json("GET", f"{base_url}/missions/{mission_id}")
    assert status == 200
    assert fetched["status"] == "success"
    assert fetched["mission"]["mission_id"] == mission_id

    status, resumed = request_json(
        "POST",
        f"{base_url}/missions/{mission_id}/resume",
        payload={"source": "desktop-mission", "metadata": {"policy_profile": "interactive"}},
    )
    assert status == 200
    assert resumed["status"] == "success"
    assert isinstance(resumed.get("goal_id"), str)


def test_mission_timeline_and_resume_preview_routes(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, goal = request_json("POST", f"{base_url}/goals", payload={"text": "timeline mission"})
    assert status == 202
    goal_id = goal["goal_id"]

    status, goal_body = request_json("GET", f"{base_url}/goals/{goal_id}")
    assert status == 200
    mission_id = str(goal_body.get("mission_id", ""))
    assert mission_id

    status, timeline = request_json(
        "GET",
        f"{base_url}/missions/{mission_id}/timeline?limit=10&event=finished&descending=1",
    )
    assert status == 200
    assert timeline["status"] == "success"
    assert timeline["mission_id"] == mission_id
    assert timeline["count"] >= 1
    assert timeline["items"][0]["event"] == "finished"

    status, preview = request_json("GET", f"{base_url}/missions/{mission_id}/resume-preview")
    assert status == 200
    assert preview["status"] == "success"
    assert preview["remaining_steps"] == 1
    assert preview["resume_cursor"]["step_id"] == "step-2"
    assert isinstance(preview["resume_plan"]["steps"], list)


def test_mission_diagnostics_and_cancel_routes(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, goal = request_json("POST", f"{base_url}/goals", payload={"text": "long cancel mission"})
    assert status == 202
    goal_id = goal["goal_id"]

    status, goal_body = request_json("GET", f"{base_url}/goals/{goal_id}")
    assert status == 200
    mission_id = str(goal_body.get("mission_id", ""))
    assert mission_id

    status, diagnostics = request_json("GET", f"{base_url}/missions/{mission_id}/diagnostics?hotspot_limit=3")
    assert status == 200
    assert diagnostics["status"] == "success"
    assert diagnostics["mission_id"] == mission_id
    assert diagnostics["risk"]["level"] in {"low", "medium", "high"}
    assert isinstance(diagnostics["hotspots"]["retry"], list)

    status, cancelled = request_json(
        "POST",
        f"{base_url}/missions/{mission_id}/cancel",
        payload={"reason": "stop this mission"},
    )
    assert status == 200
    assert cancelled["status"] == "success"
    assert cancelled["mission_id"] == mission_id

    status, fetched = request_json("GET", f"{base_url}/missions/{mission_id}")
    assert status == 200
    assert fetched["mission"]["status"] == "cancelled"


def test_goal_explain_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, goal = request_json("POST", f"{base_url}/goals", payload={"text": "explain this goal"})
    assert status == 202
    goal_id = str(goal.get("goal_id", ""))
    assert goal_id

    status, payload = request_json("GET", f"{base_url}/goals/{goal_id}/explain?include_memory_hints=1")
    assert status == 200
    assert payload["status"] == "success"
    assert payload["goal_id"] == goal_id
    assert isinstance(payload.get("recommendations"), list)


def test_runtime_autonomy_routes(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, report = request_json("GET", f"{base_url}/runtime/autonomy?limit_recent_goals=120")
    assert status == 200
    assert report["status"] == "success"
    assert report["scores"]["tier"] in {"developing", "medium", "high"}

    status, tuned = request_json(
        "POST",
        f"{base_url}/runtime/autonomy/tune",
        payload={"dry_run": True, "reason": "api-test"},
    )
    assert status == 200
    assert tuned["status"] == "success"
    assert tuned["dry_run"] is True
    assert tuned["reason"] == "api-test"


def test_runtime_queue_routes_diagnostics_and_goal_prioritize(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, service = api_server

    status, created_running = request_json("POST", f"{base_url}/goals", payload={"text": "long running queue"})
    assert status == 202
    running_goal_id = str(created_running["goal_id"])

    status, created_done = request_json("POST", f"{base_url}/goals", payload={"text": "what time is it in UTC"})
    assert status == 202
    done_goal_id = str(created_done["goal_id"])
    assert done_goal_id

    status, queue_payload = request_json(
        "GET",
        f"{base_url}/runtime/queue?limit=20&include_terminal=1&source=desktop-ui",
    )
    assert status == 200
    assert queue_payload["status"] == "success"
    assert int(queue_payload.get("count", 0) or 0) >= 1
    assert isinstance(queue_payload.get("policy"), dict)

    status, prioritized = request_json(
        "POST",
        f"{base_url}/runtime/queue/prioritize",
        payload={"goal_id": running_goal_id, "priority": -7, "reason": "operator_hotpath"},
    )
    assert status == 200
    assert prioritized["status"] == "success"
    assert prioritized["goal_id"] == running_goal_id
    assert prioritized["priority"] == -7

    goal = service.goals[running_goal_id]
    metadata = goal.get("metadata", {})
    assert int(metadata.get("queue_priority", 0)) == -7
    assert str(metadata.get("queue_priority_reason", "")) == "operator_hotpath"

    status, missing = request_json(
        "POST",
        f"{base_url}/runtime/queue/prioritize",
        payload={"priority": -3},
    )
    assert status == 400
    assert missing["status"] == "error"


def test_mission_prioritize_route(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, service = api_server

    status, created = request_json("POST", f"{base_url}/goals", payload={"text": "long mission priority seed"})
    assert status == 202
    seed_goal_id = str(created["goal_id"])
    seed_goal = service.goals[seed_goal_id]
    mission_id = str(seed_goal.get("mission_id", ""))
    assert mission_id

    status, created_same_mission = request_json(
        "POST",
        f"{base_url}/goals",
        payload={
            "text": "long mission priority second",
            "metadata": {"__jarvis_mission_id": mission_id},
        },
    )
    assert status == 202
    second_goal_id = str(created_same_mission["goal_id"])
    assert second_goal_id

    status, created_other = request_json("POST", f"{base_url}/goals", payload={"text": "long other mission"})
    assert status == 202
    other_goal_id = str(created_other["goal_id"])
    assert other_goal_id

    status, prioritized = request_json(
        "POST",
        f"{base_url}/missions/{mission_id}/prioritize",
        payload={"priority": -6, "reason": "deadline_escalation", "demote_others": True},
    )
    assert status == 200
    assert prioritized["status"] == "success"
    assert prioritized["mission_id"] == mission_id
    assert int(prioritized.get("promoted_count", 0) or 0) >= 2
    assert int(prioritized.get("demoted_count", 0) or 0) >= 1

    seed_metadata = service.goals[seed_goal_id].get("metadata", {})
    second_metadata = service.goals[second_goal_id].get("metadata", {})
    other_metadata = service.goals[other_goal_id].get("metadata", {})
    assert int(seed_metadata.get("queue_priority", 0)) == -6
    assert int(second_metadata.get("queue_priority", 0)) == -6
    assert int(other_metadata.get("queue_priority", 0)) == -3


def test_external_reliability_routes(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, _ = api_server

    status, snapshot = request_json("GET", f"{base_url}/external/reliability?provider=google&limit=10")
    assert status == 200
    assert snapshot["status"] == "success"
    assert snapshot["count"] == 1
    assert snapshot["items"][0]["provider"] == "google"

    status, analysis = request_json(
        "GET",
        f"{base_url}/external/reliability/mission-analysis?provider_limit=120&history_limit=24",
    )
    assert status == 200
    assert analysis["status"] == "success"
    profile_analysis = analysis.get("profile_history_analysis", {})
    assert isinstance(profile_analysis, dict)
    assert str(profile_analysis.get("volatility_mode", "")) == "elevated"
    provider_analysis = analysis.get("provider_risk_analysis", {})
    assert isinstance(provider_analysis, dict)
    assert int(provider_analysis.get("at_risk_count", 0) or 0) >= 1

    status, history = request_json(
        "GET",
        f"{base_url}/external/reliability/mission-history?limit=12&window=16",
    )
    assert status == 200
    assert history["status"] == "success"
    diagnostics = history.get("diagnostics", {})
    assert isinstance(diagnostics, dict)
    assert str(diagnostics.get("mode", "")) == "worsening"

    status, mission_policy = request_json(
        "GET",
        f"{base_url}/external/reliability/mission-policy?provider_limit=12&history_limit=10&history_window=24",
    )
    assert status == 200
    assert mission_policy["status"] == "success"
    assert str(mission_policy.get("policy", {}).get("profile", "")) == "defensive"
    assert len(mission_policy.get("provider_biases", [])) >= 1
    assert bool(mission_policy.get("provider_policy_autotune", {}).get("enabled", False)) is True

    status, tuned = request_json(
        "POST",
        f"{base_url}/external/reliability/mission-policy/tune",
        payload={
            "dry_run": False,
            "reason": "api-test",
            "record_analysis": True,
            "tune_provider_policies": True,
            "provider_limit": 120,
            "history_limit": 24,
        },
    )
    assert status == 200
    assert tuned["status"] == "success"
    assert bool(tuned.get("changed", False)) is True
    assert str(tuned.get("tune", {}).get("profile", "")) == "defensive"
    assert str(tuned.get("provider_policy_tuning", {}).get("status", "")) == "success"

    status, configured = request_json(
        "POST",
        f"{base_url}/external/reliability/mission-policy/config",
        payload={
            "config": {
                "mission_outage_bias_gain": 0.61,
                "mission_outage_profile_hysteresis": 0.12,
                "outage_route_hard_block_threshold": 0.81,
            },
            "persist_now": True,
            "provider_limit": 12,
            "history_limit": 10,
            "history_window": 24,
        },
    )
    assert status == 200
    assert configured["status"] == "success"
    assert bool(configured.get("updated", False)) is True
    assert float(configured.get("config", {}).get("mission_outage_bias_gain", 0.0) or 0.0) == pytest.approx(0.61)
    validation = configured.get("validation", {})
    assert isinstance(validation, dict)
    summary = validation.get("summary", {})
    assert isinstance(summary, dict)
    assert int(summary.get("changed_count", 0) or 0) >= 1
    assert str(validation.get("recommended_preset_id", "")) == "balanced_adaptive"
    assert isinstance(validation.get("history_context", {}), dict)
    assert isinstance(validation.get("decision_trace", {}), dict)
    presets = validation.get("presets", [])
    assert isinstance(presets, list)
    assert len(presets) >= 1
    assert float(
        configured.get("status_snapshot", {}).get("config", {}).get("outage_route_hard_block_threshold", 0.0) or 0.0
    ) == pytest.approx(0.81)

    status, preset_configured = request_json(
        "POST",
        f"{base_url}/external/reliability/mission-policy/config",
        payload={
            "config": {"apply_recommended_preset": True},
            "persist_now": True,
            "provider_limit": 12,
            "history_limit": 10,
            "history_window": 24,
        },
    )
    assert status == 200
    assert preset_configured["status"] == "success"
    resolved_actions = preset_configured.get("validation", {}).get("resolved_actions", [])
    assert isinstance(resolved_actions, list)
    assert any(isinstance(row, dict) and str(row.get("kind", "")) == "preset" for row in resolved_actions)

    status, reset_payload = request_json(
        "POST",
        f"{base_url}/external/reliability/mission-policy/reset",
        payload={"reset_history": True, "reset_provider_biases": True},
    )
    assert status == 200
    assert reset_payload["status"] == "success"
    assert str(reset_payload.get("policy", {}).get("profile", "")) == "balanced"
    assert int(reset_payload.get("provider_biases_reset", 0) or 0) >= 1


def test_rollback_routes_list_get_run_and_goal_run(api_server: tuple[str, FakeDesktopService]) -> None:
    base_url, service = api_server
    seed = next(iter(service.rollbacks.values()))
    rollback_id = str(seed["rollback_id"])
    goal_id = str(seed["goal_id"])

    status, listed = request_json("GET", f"{base_url}/rollback/entries?status=ready")
    assert status == 200
    assert listed["status"] == "success"
    assert listed["count"] >= 1

    status, fetched = request_json("GET", f"{base_url}/rollback/entries/{rollback_id}")
    assert status == 200
    assert fetched["status"] == "success"
    assert fetched["rollback"]["rollback_id"] == rollback_id

    status, ran = request_json("POST", f"{base_url}/rollback/entries/{rollback_id}/run", payload={"dry_run": False})
    assert status == 200
    assert ran["status"] == "success"
    assert ran["rollback"]["status"] == "rolled_back"

    status, ran_goal = request_json("POST", f"{base_url}/rollback/goals/{goal_id}/run", payload={"dry_run": True})
    assert status == 200
    assert ran_goal["status"] == "success"
    assert ran_goal["goal_id"] == goal_id


