import asyncio
import importlib.util
import json
import os
import re
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from backend.python.api.groq_client import GroqClient
from backend.python.api.nvidia_client import NvidiaClient
from backend.python.core.model_connector_orchestrator import ModelConnectorOrchestrator
from backend.python.core.provider_credentials import ProviderCredentialManager
from backend.python.inference.model_registry import ModelRegistry
from backend.python.inference.model_router import ModelRouter, RouteDecision
from backend.python.utils.logger import Logger

from .contracts import ExecutionPlan, GoalRecord, PlanStep


class Planner:
    """
    Hybrid planner:
    - deterministic routing for well-known intents
    - optional LLM planning for harder/ambiguous requests
    - strict JSON validation + action allow-listing
    """

    DEFAULT_ALLOWED_ACTIONS = {
        "open_app",
        "open_url",
        "media_search",
        "defender_status",
        "system_snapshot",
        "list_processes",
        "terminate_process",
        "list_windows",
        "active_window",
        "focus_window",
        "media_info",
        "media_play_pause",
        "media_play",
        "media_pause",
        "media_stop",
        "media_next",
        "media_previous",
        "send_notification",
        "search_files",
        "search_text",
        "scan_directory",
        "hash_file",
        "backup_file",
        "copy_file",
        "list_folder",
        "create_folder",
        "folder_size",
        "explorer_open_path",
        "explorer_select_file",
        "list_files",
        "read_file",
        "write_file",
        "clipboard_read",
        "clipboard_write",
        "keyboard_type",
        "keyboard_hotkey",
        "mouse_move",
        "mouse_click",
        "mouse_scroll",
        "screenshot_capture",
        "browser_read_dom",
        "browser_extract_links",
        "browser_session_create",
        "browser_session_list",
        "browser_session_close",
        "browser_session_request",
        "browser_session_read_dom",
        "browser_session_extract_links",
        "computer_observe",
        "computer_assert_text_visible",
        "computer_find_text_targets",
        "computer_wait_for_text",
        "computer_click_text",
        "computer_click_target",
        "desktop_action_advice",
        "desktop_interact",
        "extract_text_from_image",
        "run_whitelisted_app",
        "run_trusted_script",
        "external_connector_status",
        "external_connector_preflight",
        "external_email_send",
        "external_email_list",
        "external_email_read",
        "external_calendar_create_event",
        "external_calendar_list_events",
        "external_calendar_update_event",
        "external_doc_create",
        "external_doc_list",
        "external_doc_read",
        "external_doc_update",
        "external_task_list",
        "external_task_create",
        "external_task_update",
        "oauth_token_maintain",
        "accessibility_status",
        "accessibility_list_elements",
        "accessibility_find_element",
        "accessibility_invoke_element",
        "time_now",
        "tts_speak",
    }

    EXTERNAL_PROVIDER_RULES: Dict[str, Dict[str, List[str]]] = {
        "external_connector_preflight": {"allow": ["google", "graph", "smtp"]},
        "external_email_send": {"allow": ["google", "graph", "smtp"]},
        "external_email_list": {"allow": ["google", "graph"]},
        "external_email_read": {"allow": ["google", "graph"]},
        "external_calendar_create_event": {"allow": ["google", "graph"]},
        "external_calendar_list_events": {"allow": ["google", "graph"]},
        "external_calendar_update_event": {"allow": ["google", "graph"]},
        "external_doc_create": {"allow": ["google", "graph"]},
        "external_doc_list": {"allow": ["google", "graph"]},
        "external_doc_read": {"allow": ["google", "graph"]},
        "external_doc_update": {"allow": ["google", "graph"]},
        "external_task_list": {"allow": ["google", "graph"]},
        "external_task_create": {"allow": ["google", "graph"]},
        "external_task_update": {"allow": ["google", "graph"]},
        "oauth_token_list": {"allow": ["google", "graph"]},
        "oauth_token_upsert": {"allow": ["google", "graph"]},
        "oauth_token_refresh": {"allow": ["google", "graph"]},
        "oauth_token_maintain": {"allow": ["google", "graph"]},
        "oauth_token_revoke": {"allow": ["google", "graph"]},
    }

    EXTERNAL_OPERATION_CLASS: Dict[str, str] = {
        "external_connector_status": "healthcheck",
        "external_connector_preflight": "healthcheck",
        "external_email_send": "write",
        "external_email_list": "read",
        "external_email_read": "read",
        "external_calendar_create_event": "write",
        "external_calendar_list_events": "read",
        "external_calendar_update_event": "mutate",
        "external_doc_create": "write",
        "external_doc_list": "read",
        "external_doc_read": "read",
        "external_doc_update": "mutate",
        "external_task_list": "read",
        "external_task_create": "write",
        "external_task_update": "mutate",
        "oauth_token_list": "healthcheck",
        "oauth_token_upsert": "auth",
        "oauth_token_refresh": "auth",
        "oauth_token_maintain": "maintenance",
        "oauth_token_revoke": "auth",
    }

    DEFAULT_VERIFY_BY_ACTION: Dict[str, Dict[str, Any]] = {
        "open_app": {"expect_status": "success"},
        "open_url": {"expect_status": "success", "expect_key": "url"},
        "media_search": {"expect_status": "success", "expect_key": "url"},
        "defender_status": {"expect_status": "success"},
        "system_snapshot": {"expect_status": "success", "expect_key": "metrics"},
        "list_processes": {"expect_status": "success", "expect_key": "processes"},
        "terminate_process": {"expect_status": "success", "expect_key": "count", "expect_numeric_min": {"count": 1}},
        "list_windows": {"expect_status": "success", "expect_key": "windows"},
        "active_window": {"expect_status": "success", "expect_key": "window"},
        "focus_window": {"expect_status": "success", "expect_key": "window"},
        "media_info": {"expect_status": "success"},
        "media_play_pause": {"expect_status": "success"},
        "media_play": {"expect_status": "success"},
        "media_pause": {"expect_status": "success"},
        "media_stop": {"expect_status": "success"},
        "media_next": {"expect_status": "success"},
        "media_previous": {"expect_status": "success"},
        "send_notification": {"expect_status": "success", "expect_key": "title"},
        "search_files": {"expect_status": "success", "expect_key": "results"},
        "search_text": {"expect_status": "success", "expect_key": "results"},
        "scan_directory": {"expect_status": "success", "expect_key": "results"},
        "hash_file": {"expect_status": "success", "expect_key": "hash"},
        "backup_file": {"expect_status": "success", "expect_key": "backup_path"},
        "copy_file": {"expect_status": "success"},
        "list_folder": {"expect_status": "success", "expect_key": "items"},
        "create_folder": {"expect_status": "success", "expect_key": "created"},
        "folder_size": {"expect_status": "success", "expect_key": "size_bytes"},
        "explorer_open_path": {"expect_status": "success", "expect_key": "path"},
        "explorer_select_file": {"expect_status": "success", "expect_key": "path"},
        "list_files": {"expect_status": "success", "expect_key": "items"},
        "read_file": {"expect_status": "success", "expect_key": "content"},
        "write_file": {"expect_status": "success", "expect_key": "bytes", "expect_numeric_min": {"bytes": 0}},
        "clipboard_read": {"expect_status": "success", "expect_key": "text"},
        "clipboard_write": {"expect_status": "success"},
        "keyboard_type": {"expect_status": "success"},
        "keyboard_hotkey": {"expect_status": "success"},
        "mouse_move": {"expect_status": "success"},
        "mouse_click": {"expect_status": "success"},
        "mouse_scroll": {"expect_status": "success"},
        "screenshot_capture": {"expect_status": "success", "expect_key": "path"},
        "browser_read_dom": {"expect_status": "success", "expect_keys": ["title", "text"]},
        "browser_extract_links": {"expect_status": "success", "expect_key": "links"},
        "browser_session_create": {"expect_status": "success", "expect_key": "session"},
        "browser_session_list": {"expect_status": "success", "expect_key": "items"},
        "browser_session_close": {"expect_status": "success", "expect_key": "session"},
        "browser_session_request": {"expect_status": "success", "expect_key": "response"},
        "browser_session_read_dom": {"expect_status": "success", "expect_keys": ["title", "text"]},
        "browser_session_extract_links": {"expect_status": "success", "expect_key": "links"},
        "computer_observe": {"expect_status": "success", "expect_key": "screenshot_path"},
        "computer_assert_text_visible": {"expect_status": "success", "expect_key": "found"},
        "computer_find_text_targets": {"expect_status": "success", "expect_key": "targets"},
        "computer_wait_for_text": {"expect_status": "success", "expect_key": "found"},
        "computer_click_text": {"expect_status": "success", "expect_keys": ["x", "y"]},
        "computer_click_target": {"expect_status": "success", "expect_keys": ["query", "method"]},
        "desktop_action_advice": {"expect_status": "success", "expect_key": "execution_plan"},
        "desktop_interact": {"expect_status": "success", "expect_key": "results"},
        "extract_text_from_image": {"expect_status": "success", "expect_key": "text"},
        "run_whitelisted_app": {"expect_status": "success", "expect_key": "pid"},
        "run_trusted_script": {"expect_status": "success", "expect_key": "pid"},
        "external_connector_status": {"expect_status": "success", "expect_key": "providers"},
        "external_connector_preflight": {"expect_key": "contract_diagnostic"},
        "external_email_send": {"expect_status": "success", "expect_key": "provider"},
        "external_email_list": {"expect_status": "success", "expect_key": "items"},
        "external_email_read": {"expect_status": "success", "expect_key": "message_id"},
        "external_calendar_create_event": {"expect_status": "success", "expect_key": "provider"},
        "external_calendar_list_events": {"expect_status": "success", "expect_key": "items"},
        "external_calendar_update_event": {"expect_status": "success", "expect_key": "event_id"},
        "external_doc_create": {"expect_status": "success", "expect_key": "provider"},
        "external_doc_list": {"expect_status": "success", "expect_key": "items"},
        "external_doc_read": {"expect_status": "success", "expect_key": "document_id"},
        "external_doc_update": {"expect_status": "success", "expect_key": "document_id"},
        "external_task_list": {"expect_status": "success", "expect_key": "items"},
        "external_task_create": {"expect_status": "success", "expect_key": "task_id"},
        "external_task_update": {"expect_status": "success", "expect_key": "task_id"},
        "oauth_token_maintain": {"expect_status": "success"},
        "accessibility_status": {"expect_status": "success", "expect_key": "provider"},
        "accessibility_list_elements": {"expect_status": "success", "expect_key": "items"},
        "accessibility_find_element": {"expect_status": "success", "expect_key": "items"},
        "accessibility_invoke_element": {"expect_status": "success", "expect_key": "action"},
        "time_now": {"expect_status": "success", "expect_keys": ["iso", "timezone"]},
        "tts_speak": {"optional": True},
    }
    DEFAULT_RETRY_BY_ACTION: Dict[str, Dict[str, Any]] = {
        "open_url": {"base_delay_s": 0.6, "max_delay_s": 4.0, "multiplier": 1.8, "jitter_s": 0.15},
        "browser_read_dom": {"base_delay_s": 0.8, "max_delay_s": 6.0, "multiplier": 1.9, "jitter_s": 0.2},
        "browser_extract_links": {"base_delay_s": 0.8, "max_delay_s": 6.0, "multiplier": 1.9, "jitter_s": 0.2},
        "browser_session_create": {"base_delay_s": 0.6, "max_delay_s": 4.0, "multiplier": 1.8, "jitter_s": 0.15},
        "browser_session_request": {"base_delay_s": 0.8, "max_delay_s": 6.0, "multiplier": 1.9, "jitter_s": 0.2},
        "browser_session_read_dom": {"base_delay_s": 0.8, "max_delay_s": 6.0, "multiplier": 1.9, "jitter_s": 0.2},
        "browser_session_extract_links": {"base_delay_s": 0.8, "max_delay_s": 6.0, "multiplier": 1.9, "jitter_s": 0.2},
        "search_files": {"base_delay_s": 0.4, "max_delay_s": 3.0, "multiplier": 1.7, "jitter_s": 0.1},
        "search_text": {"base_delay_s": 0.4, "max_delay_s": 3.0, "multiplier": 1.7, "jitter_s": 0.1},
        "scan_directory": {"base_delay_s": 0.4, "max_delay_s": 3.0, "multiplier": 1.7, "jitter_s": 0.1},
        "read_file": {"base_delay_s": 0.3, "max_delay_s": 2.5, "multiplier": 1.6, "jitter_s": 0.1},
        "write_file": {"base_delay_s": 0.4, "max_delay_s": 3.0, "multiplier": 1.7, "jitter_s": 0.1},
        "copy_file": {"base_delay_s": 0.5, "max_delay_s": 4.0, "multiplier": 1.8, "jitter_s": 0.1},
        "backup_file": {"base_delay_s": 0.5, "max_delay_s": 4.0, "multiplier": 1.8, "jitter_s": 0.1},
        "run_trusted_script": {"base_delay_s": 1.0, "max_delay_s": 5.0, "multiplier": 2.0, "jitter_s": 0.2},
        "run_whitelisted_app": {"base_delay_s": 0.8, "max_delay_s": 4.0, "multiplier": 1.9, "jitter_s": 0.15},
        "external_email_send": {"base_delay_s": 0.8, "max_delay_s": 6.0, "multiplier": 1.9, "jitter_s": 0.2},
        "external_email_list": {"base_delay_s": 0.8, "max_delay_s": 6.0, "multiplier": 1.9, "jitter_s": 0.2},
        "external_email_read": {"base_delay_s": 0.8, "max_delay_s": 6.0, "multiplier": 1.9, "jitter_s": 0.2},
        "external_calendar_create_event": {"base_delay_s": 0.8, "max_delay_s": 6.0, "multiplier": 1.9, "jitter_s": 0.2},
        "external_calendar_list_events": {"base_delay_s": 0.8, "max_delay_s": 6.0, "multiplier": 1.9, "jitter_s": 0.2},
        "external_calendar_update_event": {"base_delay_s": 0.8, "max_delay_s": 6.0, "multiplier": 1.9, "jitter_s": 0.2},
        "external_doc_create": {"base_delay_s": 0.8, "max_delay_s": 6.0, "multiplier": 1.9, "jitter_s": 0.2},
        "external_doc_list": {"base_delay_s": 0.8, "max_delay_s": 6.0, "multiplier": 1.9, "jitter_s": 0.2},
        "external_doc_read": {"base_delay_s": 0.8, "max_delay_s": 6.0, "multiplier": 1.9, "jitter_s": 0.2},
        "external_doc_update": {"base_delay_s": 0.8, "max_delay_s": 6.0, "multiplier": 1.9, "jitter_s": 0.2},
        "external_task_list": {"base_delay_s": 0.8, "max_delay_s": 6.0, "multiplier": 1.9, "jitter_s": 0.2},
        "external_task_create": {"base_delay_s": 0.8, "max_delay_s": 6.0, "multiplier": 1.9, "jitter_s": 0.2},
        "external_task_update": {"base_delay_s": 0.8, "max_delay_s": 6.0, "multiplier": 1.9, "jitter_s": 0.2},
        "external_connector_preflight": {"base_delay_s": 0.6, "max_delay_s": 4.0, "multiplier": 1.6, "jitter_s": 0.15},
        "oauth_token_maintain": {"base_delay_s": 0.8, "max_delay_s": 6.0, "multiplier": 1.9, "jitter_s": 0.2},
        "computer_click_text": {"base_delay_s": 0.25, "max_delay_s": 2.0, "multiplier": 1.6, "jitter_s": 0.05},
        "computer_click_target": {"base_delay_s": 0.25, "max_delay_s": 2.5, "multiplier": 1.7, "jitter_s": 0.06},
        "desktop_action_advice": {"base_delay_s": 0.2, "max_delay_s": 1.8, "multiplier": 1.5, "jitter_s": 0.05},
        "desktop_interact": {"base_delay_s": 0.35, "max_delay_s": 3.5, "multiplier": 1.7, "jitter_s": 0.08},
        "computer_wait_for_text": {"base_delay_s": 0.2, "max_delay_s": 1.5, "multiplier": 1.5, "jitter_s": 0.05},
        "accessibility_list_elements": {"base_delay_s": 0.35, "max_delay_s": 2.5, "multiplier": 1.6, "jitter_s": 0.08},
        "accessibility_find_element": {"base_delay_s": 0.35, "max_delay_s": 2.5, "multiplier": 1.6, "jitter_s": 0.08},
        "accessibility_invoke_element": {"base_delay_s": 0.3, "max_delay_s": 2.0, "multiplier": 1.5, "jitter_s": 0.05},
    }

    PROFILE_VERIFY_OVERRIDES: Dict[str, Dict[str, Dict[str, Any]]] = {
        "interactive": {
            "*": {"expect_result_status": "success"},
        },
        "automation_safe": {
            "*": {"expect_result_status": "success", "expect_status": "success"},
            "open_url": {
                "confirm": {
                    "action": "browser_read_dom",
                    "args": {"url": "{{args.url}}", "max_chars": 1500},
                    "required": False,
                    "attempts": 2,
                    "delay_s": 0.2,
                    "timeout_s": 20,
                },
                "checks": [{"source": "confirm", "type": "key_exists", "key": "title"}],
            },
            "browser_read_dom": {
                "confirm": {
                    "action": "browser_extract_links",
                    "args": {"url": "{{args.url}}", "max_links": 10},
                    "required": False,
                    "attempts": 2,
                    "delay_s": 0.2,
                    "timeout_s": 20,
                }
            },
            "search_files": {
                "confirm": {
                    "action": "list_folder",
                    "args": {"path": "{{args.base_dir}}"},
                    "required": False,
                    "attempts": 2,
                    "delay_s": 0.2,
                }
            },
            "search_text": {
                "confirm": {
                    "action": "list_folder",
                    "args": {"path": "{{args.base_dir}}"},
                    "required": False,
                    "attempts": 2,
                    "delay_s": 0.2,
                }
            },
            "scan_directory": {
                "confirm": {
                    "action": "list_folder",
                    "args": {"path": "{{args.path}}"},
                    "required": False,
                    "attempts": 2,
                    "delay_s": 0.2,
                }
            },
        },
        "automation_power": {
            "*": {"expect_result_status": "success", "expect_status": "success"},
            "open_app": {"confirm": {"required": True, "attempts": 3, "delay_s": 0.35}},
            "focus_window": {"confirm": {"required": True, "attempts": 3, "delay_s": 0.25}},
            "terminate_process": {"confirm": {"required": True, "attempts": 3, "delay_s": 0.35}},
            "write_file": {"confirm": {"required": True, "attempts": 3, "delay_s": 0.3}},
            "copy_file": {"confirm": {"required": True, "attempts": 3, "delay_s": 0.3}},
            "backup_file": {"confirm": {"required": True, "attempts": 3, "delay_s": 0.25}},
            "clipboard_write": {"confirm": {"required": True, "attempts": 3, "delay_s": 0.2}},
            "create_folder": {"confirm": {"required": True, "attempts": 3, "delay_s": 0.25}},
            "computer_click_text": {"confirm": {"required": True, "attempts": 3, "delay_s": 0.25}},
            "computer_click_target": {"confirm": {"required": True, "attempts": 3, "delay_s": 0.25}},
            "external_email_send": {"confirm": {"required": True, "attempts": 3, "delay_s": 0.3}},
            "external_calendar_create_event": {"confirm": {"required": True, "attempts": 3, "delay_s": 0.3}},
            "external_calendar_update_event": {"confirm": {"required": True, "attempts": 3, "delay_s": 0.3}},
            "external_doc_update": {"confirm": {"required": True, "attempts": 3, "delay_s": 0.3}},
            "external_task_update": {"confirm": {"required": True, "attempts": 3, "delay_s": 0.3}},
            "accessibility_invoke_element": {"confirm": {"required": True, "attempts": 3, "delay_s": 0.25}},
        },
    }

    def __init__(self, model_router: ModelRouter | None = None) -> None:
        self.log = Logger.get_logger("Planner")
        self.provider_credentials = ProviderCredentialManager()
        self.provider_credentials.refresh(overwrite_env=False)
        self.model_router = model_router or ModelRouter(
            ModelRegistry(
                provider_credentials=self.provider_credentials,
                enforce_provider_keys=os.getenv("JARVIS_MODEL_ENFORCE_PROVIDER_KEYS", "0") == "1",
                scan_local_models=os.getenv("JARVIS_SCAN_LOCAL_MODELS", "1") != "0",
            )
        )
        self.connector_orchestrator = ModelConnectorOrchestrator()
        self.groq_client = GroqClient(api_key=self.provider_credentials.get_api_key("groq") or None)

        nvidia_key = self.provider_credentials.get_api_key("nvidia") or os.getenv("NVIDIA_API_KEY", "").strip()
        self.nvidia_client = NvidiaClient(nvidia_key) if nvidia_key else None

        self.llm_enabled = os.getenv("JARVIS_ENABLE_LLM_PLANNER", "1") == "1"
        self.llm_timeout_s = max(2.0, min(float(os.getenv("JARVIS_LLM_PLANNER_TIMEOUT_S", "8")), 30.0))
        self.max_llm_steps = max(1, min(int(os.getenv("JARVIS_LLM_PLANNER_MAX_STEPS", "4")), 8))
        self.local_reasoning_enabled = os.getenv("JARVIS_ENABLE_LOCAL_REASONING_PROVIDER", "1") == "1"
        self.local_reasoning_timeout_s = max(6.0, min(float(os.getenv("JARVIS_LOCAL_REASONING_TIMEOUT_S", "90")), 300.0))
        self.local_reasoning_max_new_tokens = max(
            64,
            min(int(os.getenv("JARVIS_LOCAL_REASONING_MAX_NEW_TOKENS", "768")), 2048),
        )
        self.local_reasoning_prompt_max_chars = max(
            800,
            min(int(os.getenv("JARVIS_LOCAL_REASONING_PROMPT_MAX_CHARS", "12000")), 64000),
        )
        self.local_reasoning_probe_enabled = os.getenv("JARVIS_LOCAL_REASONING_PROBE_ENABLED", "1") == "1"
        self.local_reasoning_probe_prompt = str(
            os.getenv(
                "JARVIS_LOCAL_REASONING_PROBE_PROMPT",
                "Summarize your runtime readiness in one short sentence.",
            )
            or "Summarize your runtime readiness in one short sentence."
        ).strip()
        self.local_reasoning_probe_max_chars = max(
            32,
            min(int(os.getenv("JARVIS_LOCAL_REASONING_PROBE_MAX_CHARS", "240")), 1200),
        )
        self.local_reasoning_failure_streak_threshold = max(
            1,
            min(int(os.getenv("JARVIS_LOCAL_REASONING_FAILURE_STREAK_THRESHOLD", "2")), 12),
        )
        self.local_reasoning_failure_cooldown_s = max(
            3.0,
            min(float(os.getenv("JARVIS_LOCAL_REASONING_FAILURE_COOLDOWN_S", "20.0")), 900.0),
        )
        self._local_reasoning_lock = threading.RLock()
        self._local_transformers_cache: Dict[str, Dict[str, Any]] = {}
        self._local_llama_cpp_cache: Dict[str, Any] = {}
        self._local_reasoning_runtime_state: Dict[str, Dict[str, Any]] = {}
        self._local_reasoning_route_policy_snapshot: Dict[str, Any] = {}
        self._voice_route_policy_snapshot: Dict[str, Any] = {}
        self.local_reasoning_route_policy_snapshot_ttl_s = max(
            30.0,
            min(float(os.getenv("JARVIS_LOCAL_REASONING_ROUTE_POLICY_TTL_S", "900")), 86400.0),
        )
        self.voice_route_policy_snapshot_ttl_s = max(
            5.0,
            min(float(os.getenv("JARVIS_VOICE_ROUTE_POLICY_SNAPSHOT_TTL_S", "300")), 86400.0),
        )
        self.external_replan_confidence_budget_enabled = os.getenv("JARVIS_EXTERNAL_REPLAN_CONFIDENCE_BUDGET_ENABLED", "1") == "1"
        self.external_replan_min_confidence_floor = max(
            0.0,
            min(float(os.getenv("JARVIS_EXTERNAL_REPLAN_MIN_CONFIDENCE_FLOOR", "0.34")), 1.0),
        )
        self.external_replan_max_actions_base = max(
            1,
            min(int(os.getenv("JARVIS_EXTERNAL_REPLAN_MAX_ACTIONS_BASE", "4")), 10),
        )

        self.allowed_actions = set(self.DEFAULT_ALLOWED_ACTIONS)
        self.permissions_path = Path(os.getenv("JARVIS_PERMISSIONS_PATH", "configs/permissions.json"))
        self.profile_allow_actions: Dict[str, Set[str]] = {}
        self.profile_deny_actions: Dict[str, Set[str]] = {}
        self.source_default_profile: Dict[str, str] = {}
        self.default_profile_name: str = ""
        self._load_profile_policy()

    def set_tool_catalog(self, actions: set[str]) -> None:
        if not actions:
            return
        self.allowed_actions = {action for action in actions if isinstance(action, str) and action}
        self.allowed_actions.add("tts_speak")

    def _load_profile_policy(self) -> None:
        self.profile_allow_actions = {}
        self.profile_deny_actions = {}
        self.source_default_profile = {}
        self.default_profile_name = ""

        if not self.permissions_path.exists():
            return
        try:
            payload = json.loads(self.permissions_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return
        if not isinstance(payload, dict):
            return

        default_profile = payload.get("default_profile")
        if isinstance(default_profile, str) and default_profile.strip():
            self.default_profile_name = default_profile.strip().lower()

        default_profiles = payload.get("default_profiles", {})
        if isinstance(default_profiles, dict):
            for source_name, profile_name in default_profiles.items():
                if not isinstance(source_name, str) or not isinstance(profile_name, str):
                    continue
                source = source_name.strip().lower()
                profile = profile_name.strip().lower()
                if source and profile:
                    self.source_default_profile[source] = profile

        profiles = payload.get("profiles", {})
        if not isinstance(profiles, dict):
            return
        for profile_name, raw_rules in profiles.items():
            if not isinstance(profile_name, str) or not isinstance(raw_rules, dict):
                continue
            normalized = profile_name.strip().lower()
            if not normalized:
                continue
            allow_set = self._to_action_set(raw_rules.get("allow"))
            deny_set = self._to_action_set(raw_rules.get("deny"))
            self.profile_allow_actions[normalized] = allow_set
            self.profile_deny_actions[normalized] = deny_set

    @staticmethod
    def _to_action_set(raw: object) -> Set[str]:
        if not isinstance(raw, list):
            return set()
        output: Set[str] = set()
        for item in raw:
            if isinstance(item, str):
                clean = item.strip()
                if clean:
                    output.add(clean)
        return output

    def _resolve_policy_profile(self, goal: GoalRecord, context: Dict[str, object]) -> str:
        metadata = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
        explicit = str(metadata.get("policy_profile", "")).strip().lower()
        if explicit:
            return explicit

        from_context = str(context.get("policy_profile", "")).strip().lower()
        if from_context:
            return from_context

        source = str(context.get("source", "") or goal.request.source or "").strip().lower()
        source_default = self.source_default_profile.get(source, "")
        if source_default:
            return source_default

        return self.default_profile_name

    def _candidate_actions_for_profile(self, profile_name: str) -> Set[str]:
        candidates: Set[str] = set(self.allowed_actions)
        clean_profile = str(profile_name or "").strip().lower()
        if not clean_profile:
            return candidates

        allow_set = self.profile_allow_actions.get(clean_profile, set())
        deny_set = self.profile_deny_actions.get(clean_profile, set())
        if allow_set:
            candidates.intersection_update(allow_set)
            candidates.add("tts_speak")
        if deny_set:
            candidates.difference_update(deny_set)
        candidates.add("tts_speak")
        return candidates

    def _extract_runtime_constraints(self, text: str) -> Dict[str, Any]:
        clean = str(text or "").strip()
        if not clean:
            return {}
        lowered = clean.lower()
        constraints: Dict[str, Any] = {}

        max_steps_match = re.search(
            r"\b(?:at most|max(?:imum)?|no more than|within|in)\s+(\d{1,3})\s+steps?\b",
            lowered,
        )
        if max_steps_match:
            constraints["max_steps_hint"] = max(1, min(int(max_steps_match.group(1)), 250))
        elif any(token in lowered for token in ("single step", "one step", "1 step")):
            constraints["max_steps_hint"] = 1

        duration_match = re.search(
            r"\b(?:within|under|no more than)\s+(\d{1,4})\s*(seconds?|secs?|minutes?|mins?|hours?|hrs?)\b",
            lowered,
        )
        if duration_match:
            constraints["time_budget_s"] = self._duration_to_seconds(int(duration_match.group(1)), duration_match.group(2))
        else:
            in_match = re.search(r"\bin\s+(\d{1,4})\s*(seconds?|secs?|minutes?|mins?|hours?|hrs?)\b", lowered)
            if in_match and any(
                token in lowered
                for token in ("finish", "complete", "done", "execute", "perform", "handle this", "resolve")
            ):
                constraints["time_budget_s"] = self._duration_to_seconds(int(in_match.group(1)), in_match.group(2))

        if any(token in lowered for token in ("strict verification", "verify strictly", "no fallback", "strict mode")):
            constraints["verification_strictness"] = "strict"
        elif any(token in lowered for token in ("quick mode", "lenient", "best effort")):
            constraints["verification_strictness"] = "standard"

        deadline_match = re.search(r"\bby\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b", lowered)
        if deadline_match:
            hour = int(deadline_match.group(1))
            minute = int(deadline_match.group(2) or 0)
            meridiem = str(deadline_match.group(3) or "").strip().lower()
            if 0 <= minute <= 59:
                if meridiem:
                    if hour == 12:
                        hour = 0
                    if meridiem == "pm":
                        hour += 12
                if 0 <= hour <= 23:
                    now = datetime.now(timezone.utc)
                    deadline = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                    if deadline <= now:
                        deadline = deadline + timedelta(days=1)
                    constraints["deadline_at"] = deadline.isoformat()

        return constraints

    @staticmethod
    def _duration_to_seconds(value: int, unit_text: str) -> int:
        amount = max(1, min(int(value), 100000))
        unit = str(unit_text or "").strip().lower()
        if unit.startswith(("hour", "hr")):
            seconds = amount * 3600
        elif unit.startswith(("minute", "min")):
            seconds = amount * 60
        else:
            seconds = amount
        return max(1, min(seconds, 3600))

    async def build_plan(self, goal: GoalRecord, context: Dict[str, object] | None = None) -> ExecutionPlan:
        text = goal.request.text.strip()
        lowered = text.lower()
        planning_context = dict(context or {})
        runtime_constraints = self._extract_runtime_constraints(text)
        if runtime_constraints:
            planning_context["runtime_constraints"] = runtime_constraints
            if "time_budget_s" in runtime_constraints:
                planning_context["time_budget_s"] = runtime_constraints["time_budget_s"]
            if "max_steps_hint" in runtime_constraints:
                planning_context["max_steps_hint"] = runtime_constraints["max_steps_hint"]
            if "verification_strictness" in runtime_constraints:
                planning_context["verification_strictness"] = runtime_constraints["verification_strictness"]
            if "deadline_at" in runtime_constraints:
                planning_context["deadline_at"] = runtime_constraints["deadline_at"]
        policy_profile = self._resolve_policy_profile(goal, planning_context)
        if policy_profile:
            planning_context["policy_profile"] = policy_profile
        source = str(planning_context.get("source", "") or goal.request.source or "").strip().lower()
        if source.startswith("voice") or "voice" in source:
            voice_policy = self.voice_route_policy_snapshot(refresh=False)
            planning_context["voice_route_policy"] = voice_policy
            planning_constraints = (
                voice_policy.get("planning_constraints", {})
                if isinstance(voice_policy.get("planning_constraints", {}), dict)
                else {}
            )
            max_steps_hint = int(planning_constraints.get("max_steps_hint", 0) or 0)
            if max_steps_hint > 0 and "max_steps_hint" not in planning_context:
                planning_context["max_steps_hint"] = max_steps_hint
            if bool(planning_constraints.get("prefer_brief_response", False)):
                planning_context["voice_prefer_brief_response"] = True
        candidate_actions = self._candidate_actions_for_profile(policy_profile)
        llm_candidate_actions = set(candidate_actions)
        planning_context["planner_candidate_actions"] = len(candidate_actions)
        voice_interaction_policy = self._voice_interaction_policy(
            planning_context,
            allowed_actions=candidate_actions,
        )
        if voice_interaction_policy:
            planning_context["voice_interaction_policy"] = dict(voice_interaction_policy)
            interaction_max_steps = int(voice_interaction_policy.get("max_steps_hint", 0) or 0)
            if interaction_max_steps > 0:
                current_max_steps = int(planning_context.get("max_steps_hint", 0) or 0)
                planning_context["max_steps_hint"] = (
                    min(current_max_steps, interaction_max_steps)
                    if current_max_steps > 0
                    else interaction_max_steps
                )
            if bool(voice_interaction_policy.get("prefer_brief_response", False)):
                planning_context["voice_prefer_brief_response"] = True
            if str(voice_interaction_policy.get("followup_mode", "") or "").strip():
                planning_context["voice_followup_mode"] = str(voice_interaction_policy.get("followup_mode", "")).strip().lower()
            if str(voice_interaction_policy.get("confirmation_mode", "") or "").strip():
                planning_context["voice_confirmation_mode"] = str(
                    voice_interaction_policy.get("confirmation_mode", "")
                ).strip().lower()
            if bool(voice_interaction_policy.get("prefer_notification_followup", False)):
                planning_context["voice_prefer_notification_followup"] = True
            if bool(voice_interaction_policy.get("prefer_non_voice_completion", False)):
                planning_context["prefer_non_voice_completion"] = True
        voice_execution_policy = self._voice_execution_policy(
            planning_context,
            allowed_actions=candidate_actions,
        )
        if voice_execution_policy:
            execution_strictness = str(voice_execution_policy.get("verification_strictness", "") or "").strip().lower()
            if execution_strictness:
                current_strictness = str(planning_context.get("verification_strictness", "") or "").strip().lower()
                if current_strictness != "strict" or execution_strictness == "strict":
                    planning_context["verification_strictness"] = execution_strictness
            llm_candidate_actions = set(voice_execution_policy.get("llm_allowed_actions", llm_candidate_actions))
            planning_context["planner_llm_candidate_actions"] = len(llm_candidate_actions)
            filtered_llm_actions = sorted(action for action in candidate_actions if action not in llm_candidate_actions)
            if filtered_llm_actions:
                planning_context["planner_voice_filtered_llm_actions"] = filtered_llm_actions
            stored_voice_execution_policy = dict(voice_execution_policy)
            stored_voice_execution_policy["llm_allowed_actions"] = sorted(llm_candidate_actions)
            planning_context["voice_execution_policy"] = stored_voice_execution_policy
        voice_delivery_policy = self._voice_delivery_policy(
            planning_context,
            allowed_actions=candidate_actions,
        )
        if voice_delivery_policy:
            planning_context["voice_delivery_policy"] = dict(voice_delivery_policy)

        replan_attempt = int(planning_context.get("replan_attempt", 0) or 0)
        last_failure_action = str(planning_context.get("last_failure_action", "")).strip()

        if replan_attempt > 0 and last_failure_action:
            det_intent, det_steps = self._build_replan_steps(text, lowered, planning_context)
            intent, steps = det_intent, det_steps
            planning_context["planner_mode"] = "deterministic_replan"
            should_try_llm_replan, llm_replan_reason = self._should_try_llm_replan(
                text=text,
                deterministic_intent=det_intent,
                deterministic_steps=det_steps,
                context=planning_context,
            )
            if should_try_llm_replan:
                llm_allowed_actions = self._llm_replan_allowed_actions(
                    deterministic_steps=det_steps,
                    failed_action=last_failure_action,
                    allowed_actions=llm_candidate_actions,
                )
                llm_replan_text = self._build_llm_replan_text(
                    text=text,
                    context=planning_context,
                    deterministic_intent=det_intent,
                    deterministic_steps=det_steps,
                )
                llm_plan = await self._build_llm_plan(
                    text=llm_replan_text,
                    context=planning_context,
                    allowed_actions=llm_allowed_actions,
                )
                if llm_plan is not None:
                    llm_intent, llm_steps, llm_meta = llm_plan
                    if self._llm_replan_is_actionable(llm_steps=llm_steps, fallback_steps=det_steps):
                        intent, steps = llm_intent, llm_steps
                        planning_context.update(
                            {
                                "planner_mode": "llm_replan_hybrid",
                                "planner_provider": llm_meta.get("provider", ""),
                                "planner_model": llm_meta.get("model", ""),
                                "planner_reason": llm_replan_reason,
                                "planner_llm_replan_allowed_actions": sorted(llm_allowed_actions),
                            }
                        )
                    else:
                        planning_context["planner_llm_replan_reason"] = "llm_replan_not_actionable"
                else:
                    planning_context["planner_llm_replan_reason"] = "llm_replan_unavailable"
        else:
            det_intent, det_steps = self._build_primary_steps(text, lowered)
            intent, steps = det_intent, det_steps
            planning_context["planner_mode"] = "deterministic"

            should_try_llm, reason = self._should_try_llm(
                text=text,
                lowered=lowered,
                deterministic_intent=det_intent,
                deterministic_steps=det_steps,
                context=planning_context,
            )
            if len([action for action in candidate_actions if action != "tts_speak"]) == 0:
                should_try_llm = False
                reason = "policy_profile_no_actions"
            if should_try_llm:
                llm_plan = await self._build_llm_plan(
                    text=text,
                    context=planning_context,
                    allowed_actions=llm_candidate_actions,
                )
                if llm_plan is not None:
                    intent, steps, meta = llm_plan
                    planning_context.update(
                        {
                            "planner_mode": "llm_hybrid",
                            "planner_provider": meta.get("provider", ""),
                            "planner_model": meta.get("model", ""),
                            "planner_reason": reason,
                        }
                    )
                else:
                    planning_context["planner_mode"] = "deterministic_fallback"
                    planning_context["planner_reason"] = "llm_unavailable_or_invalid"

        steps = self._apply_profile_verification_overrides(steps, policy_profile)
        circuit_tuning = self._apply_circuit_breaker_overrides(steps, planning_context)
        if circuit_tuning:
            planning_context["circuit_step_tuning"] = circuit_tuning
        guardrail_tuning = self._apply_action_guardrail_overrides(steps, planning_context)
        if guardrail_tuning:
            planning_context["guardrail_step_tuning"] = guardrail_tuning
        strategy_applied = self._apply_episodic_strategy_overrides(
            steps,
            planning_context.get("retrieved_episodic_strategy", {}),
        )
        if strategy_applied:
            planning_context["strategy_applied"] = strategy_applied
        steps, voice_delivery_applied = self._apply_voice_delivery_policy(
            steps,
            planning_context,
            allowed_actions=candidate_actions,
        )
        if voice_delivery_applied:
            merged_voice_delivery = (
                dict(planning_context.get("voice_delivery_policy", {}))
                if isinstance(planning_context.get("voice_delivery_policy", {}), dict)
                else {}
            )
            merged_voice_delivery.update(voice_delivery_applied)
            planning_context["voice_delivery_policy"] = merged_voice_delivery
        steps, voice_execution_applied = self._apply_voice_execution_policy(
            steps,
            planning_context,
            allowed_actions=candidate_actions,
        )
        if voice_execution_applied:
            merged_voice_execution = (
                dict(planning_context.get("voice_execution_policy", {}))
                if isinstance(planning_context.get("voice_execution_policy", {}), dict)
                else {}
            )
            merged_voice_execution.update(voice_execution_applied)
            planning_context["voice_execution_policy"] = merged_voice_execution
        steps, filtered_actions = self._filter_steps_by_allowed_actions(steps, candidate_actions)
        if filtered_actions:
            planning_context["policy_filtered_actions"] = filtered_actions

        filtered_to_only_speak = bool(filtered_actions) and bool(steps) and all(step.action == "tts_speak" for step in steps)
        if not steps or filtered_to_only_speak:
            profile_label = policy_profile or "default"
            blocked = ", ".join(filtered_actions[:3]) if filtered_actions else "requested actions"
            intent = "policy_profile_blocked"
            steps = [
                self._voice_delivery_fallback_step(
                    message=(
                        f"Policy profile '{profile_label}' blocked: {blocked}. "
                        "Choose a different profile or request a safer action."
                    ),
                    context=planning_context,
                    allowed_actions=candidate_actions,
                )
            ]

        if isinstance(intent, str) and intent.startswith("compound_parallel_"):
            if len(steps) >= 2:
                planning_context["allow_parallel"] = True
                planning_context["max_parallel_steps"] = max(2, min(self.max_llm_steps, 4))
            else:
                planning_context["allow_parallel"] = False

        return ExecutionPlan(
            plan_id=str(uuid.uuid4()),
            goal_id=goal.goal_id,
            intent=intent,
            steps=steps,
            context=planning_context,
        )

    @staticmethod
    def _compact_voice_delivery_text(text: str, *, max_chars: int, prefer_single_sentence: bool = False) -> str:
        clean = " ".join(str(text or "").strip().split())
        if not clean:
            return ""
        if max_chars <= 0 or len(clean) <= max_chars:
            return clean
        sentence_parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+", clean) if part.strip()]
        if sentence_parts:
            first_sentence = sentence_parts[0]
            if len(first_sentence) <= max_chars:
                return first_sentence
            if prefer_single_sentence:
                truncated = first_sentence[: max(0, max_chars - 3)].rstrip(" ,;:.")
                return f"{truncated}..." if truncated else first_sentence[:max_chars]
        truncated = clean[: max(0, max_chars - 3)].rstrip(" ,;:.")
        return f"{truncated}..." if truncated else clean[:max_chars]

    def _clone_step_with_action(
        self,
        step: PlanStep,
        *,
        action: str,
        args: Dict[str, Any],
        verify: Dict[str, Any] | None = None,
        can_retry: Optional[bool] = None,
    ) -> PlanStep:
        replacement = self._step(
            action,
            args=dict(args or {}),
            depends_on=list(step.depends_on or []),
            verify=dict(verify or step.verify or {}),
            can_retry=step.can_retry if can_retry is None else bool(can_retry),
            max_retries=int(step.max_retries or 0),
            timeout_s=int(step.timeout_s or 0),
        )
        replacement.step_id = step.step_id
        replacement.priority = int(step.priority or 0)
        replacement.status = step.status
        replacement.preconditions = dict(step.preconditions or {})
        replacement.postconditions = dict(step.postconditions or {})
        replacement.rollback = dict(step.rollback or {})
        replacement.guardrails = dict(step.guardrails or {})
        replacement.budget = dict(step.budget or {})
        return replacement

    def _voice_delivery_policy(
        self,
        context: Dict[str, object],
        *,
        allowed_actions: Set[str] | None = None,
    ) -> Dict[str, Any]:
        source = str(context.get("source", "") or context.get("goal_source", "") or "").strip().lower()
        if not (source.startswith("voice") or "voice" in source):
            return {}
        voice_policy = context.get("voice_route_policy")
        policy = dict(voice_policy) if isinstance(voice_policy, dict) else {}
        tts_policy = policy.get("tts", {}) if isinstance(policy.get("tts", {}), dict) else {}
        summary = (
            policy.get("route_policy_summary", {})
            if isinstance(policy.get("route_policy_summary", {}), dict)
            else policy.get("summary", {}) if isinstance(policy.get("summary", {}), dict) else {}
        )
        planning_constraints = (
            policy.get("planning_constraints", {})
            if isinstance(policy.get("planning_constraints", {}), dict)
            else {}
        )
        interaction_policy = (
            context.get("voice_interaction_policy", {})
            if isinstance(context.get("voice_interaction_policy", {}), dict)
            else {}
        )
        allowed = set(allowed_actions or self.allowed_actions)
        can_notify = "send_notification" in allowed
        can_speak = "tts_speak" in allowed
        tts_status = str(tts_policy.get("status", "") or "").strip().lower()
        summary_status = str(summary.get("status", "") or "").strip().lower()
        route_blocked = bool(tts_policy.get("route_blocked", False)) or tts_status == "blocked"
        route_adjusted = bool(tts_policy.get("route_adjusted", False)) or tts_status == "rerouted"
        recovery_pending = bool(tts_policy.get("recovery_pending", False)) or summary_status in {"recovery", "blocked"}
        local_voice_pressure_score = max(0.0, min(1.0, float(policy.get("local_voice_pressure_score", 0.0) or 0.0)))
        prefer_brief_response = bool(context.get("voice_prefer_brief_response", False)) or bool(
            planning_constraints.get("prefer_brief_response", False)
        ) or bool(interaction_policy.get("prefer_brief_response", False))
        followup_mode = str(
            interaction_policy.get("followup_mode", "") or planning_constraints.get("voice_followup_mode", "")
        ).strip().lower()
        confirmation_mode = str(
            interaction_policy.get("confirmation_mode", "") or planning_constraints.get("voice_confirmation_mode", "")
        ).strip().lower()
        prefer_notification_followup = bool(
            interaction_policy.get("prefer_notification_followup", False)
            or planning_constraints.get("prefer_notification_followup", False)
            or followup_mode == "notification"
        )
        avoid_multi_turn_voice_loop = bool(
            interaction_policy.get("avoid_multi_turn_voice_loop", False)
            or planning_constraints.get("avoid_multi_turn_voice_loop", False)
        )
        suppress_tts = bool(route_blocked and can_notify)
        delivery_mode = "speech"
        max_tts_chars = 0
        if suppress_tts:
            delivery_mode = "notification_fallback"
        elif prefer_notification_followup and can_notify:
            suppress_tts = True
            delivery_mode = "notification_preferred"
        elif recovery_pending or route_adjusted or local_voice_pressure_score >= 0.55 or prefer_brief_response:
            delivery_mode = "brief_speech"
            if avoid_multi_turn_voice_loop or local_voice_pressure_score >= 0.75:
                max_tts_chars = 140
            else:
                max_tts_chars = 180 if (recovery_pending or route_adjusted or local_voice_pressure_score >= 0.7) else 240
        return {
            "mode": delivery_mode,
            "source": source or "voice",
            "tts_route_status": tts_status or summary_status or "unknown",
            "tts_route_blocked": bool(route_blocked),
            "tts_route_adjusted": bool(route_adjusted),
            "tts_recovery_pending": bool(recovery_pending),
            "local_voice_pressure_score": round(local_voice_pressure_score, 6),
            "prefer_brief_response": bool(prefer_brief_response),
            "prefer_notification_followup": bool(prefer_notification_followup),
            "avoid_multi_turn_voice_loop": bool(avoid_multi_turn_voice_loop),
            "suppress_tts": bool(suppress_tts),
            "fallback_action": "send_notification" if suppress_tts else ("tts_speak" if can_speak else ""),
            "notification_fallback_available": bool(can_notify),
            "tts_available": bool(can_speak),
            "max_tts_chars": int(max_tts_chars),
            "followup_mode": followup_mode,
            "confirmation_mode": confirmation_mode,
            "reason_code": str(tts_policy.get("reason_code", "") or summary.get("reason_code", "")).strip().lower(),
            "reason": str(tts_policy.get("reason", "") or summary.get("reason", "")).strip(),
        }

    def _voice_interaction_policy(
        self,
        context: Dict[str, object],
        *,
        allowed_actions: Set[str] | None = None,
    ) -> Dict[str, Any]:
        source = str(context.get("source", "") or context.get("goal_source", "") or "").strip().lower()
        if not (source.startswith("voice") or "voice" in source):
            return {}
        voice_policy = context.get("voice_route_policy")
        policy = dict(voice_policy) if isinstance(voice_policy, dict) else {}
        planning_constraints = (
            policy.get("planning_constraints", {})
            if isinstance(policy.get("planning_constraints", {}), dict)
            else {}
        )
        wakeword_supervision = (
            policy.get("wakeword_supervision", {})
            if isinstance(policy.get("wakeword_supervision", {}), dict)
            else {}
        )
        mission_reliability = (
            policy.get("mission_reliability", {})
            if isinstance(policy.get("mission_reliability", {}), dict)
            else {}
        )
        recovery = (
            policy.get("route_recovery_recommendation", {})
            if isinstance(policy.get("route_recovery_recommendation", {}), dict)
            else {}
        )
        allowed = set(allowed_actions or self.allowed_actions)
        can_notify = "send_notification" in allowed
        can_speak = "tts_speak" in allowed
        wakeword_status = str(wakeword_supervision.get("status", "") or "").strip().lower()
        wakeword_strategy = str(
            recovery.get("wakeword_strategy", "")
            or wakeword_supervision.get("strategy", "")
            or planning_constraints.get("wakeword_strategy", "")
        ).strip().lower()
        local_voice_pressure_score = max(0.0, min(1.0, float(policy.get("local_voice_pressure_score", 0.0) or 0.0)))
        sessions = int(mission_reliability.get("sessions", 0) or 0)
        pause_count = int(mission_reliability.get("route_policy_pause_count", 0) or 0)
        wakeword_gate_events = int(mission_reliability.get("wakeword_gate_events", 0) or 0)
        pause_pressure = (
            max(float(pause_count) / float(max(1, sessions)), float(wakeword_gate_events) / float(max(1, sessions)))
            if sessions > 0
            else 0.0
        )
        prefer_brief_response = bool(context.get("voice_prefer_brief_response", False)) or bool(
            planning_constraints.get("prefer_brief_response", False)
        )
        avoid_multi_turn_voice_loop = bool(
            planning_constraints.get("avoid_multi_turn_voice_loop", False)
            or wakeword_status in {"polling_only", "blocked", "recovery"}
            or wakeword_strategy in {"polling_only", "hybrid_polling"}
            or pause_pressure >= 0.45
            or local_voice_pressure_score >= 0.58
        )
        followup_mode = str(planning_constraints.get("voice_followup_mode", "") or "").strip().lower()
        if not followup_mode:
            if bool(planning_constraints.get("prefer_notification_followup", False)) and can_notify:
                followup_mode = "notification"
            elif wakeword_status in {"polling_only", "blocked"} and can_notify:
                followup_mode = "notification"
            elif avoid_multi_turn_voice_loop:
                followup_mode = "hybrid" if can_notify and can_speak else ("notification" if can_notify else "spoken")
            else:
                followup_mode = "spoken"
        confirmation_mode = str(planning_constraints.get("voice_confirmation_mode", "") or "").strip().lower()
        if not confirmation_mode:
            if followup_mode == "notification" or local_voice_pressure_score >= 0.72:
                confirmation_mode = "explicit"
            elif avoid_multi_turn_voice_loop or prefer_brief_response:
                confirmation_mode = "compact"
            else:
                confirmation_mode = "minimal"
        prefer_notification_followup = bool(followup_mode == "notification" and can_notify)
        prefer_non_voice_completion = bool(
            planning_constraints.get("prefer_non_voice_completion", False)
            or (followup_mode != "spoken" and can_notify)
        )
        max_steps_hint = int(planning_constraints.get("max_steps_hint", 0) or 0)
        if followup_mode == "notification":
            max_steps_hint = min(max_steps_hint, 2) if max_steps_hint > 0 else 2
        elif avoid_multi_turn_voice_loop:
            reduced_hint = 3 if local_voice_pressure_score >= 0.72 else 4
            max_steps_hint = min(max_steps_hint, reduced_hint) if max_steps_hint > 0 else reduced_hint
        return {
            "status": "success",
            "source": source or "voice",
            "wakeword_status": wakeword_status or "unknown",
            "wakeword_strategy": wakeword_strategy or "wakeword",
            "followup_mode": followup_mode or "spoken",
            "confirmation_mode": confirmation_mode,
            "avoid_multi_turn_voice_loop": bool(avoid_multi_turn_voice_loop),
            "prefer_notification_followup": bool(prefer_notification_followup),
            "prefer_non_voice_completion": bool(prefer_non_voice_completion),
            "prefer_brief_response": bool(prefer_brief_response or avoid_multi_turn_voice_loop),
            "max_steps_hint": int(max_steps_hint),
            "pause_pressure": round(max(0.0, min(pause_pressure, 1.0)), 6),
            "local_voice_pressure_score": round(local_voice_pressure_score, 6),
        }

    def _voice_execution_policy(
        self,
        context: Dict[str, object],
        *,
        allowed_actions: Set[str] | None = None,
    ) -> Dict[str, Any]:
        source = str(context.get("source", "") or context.get("goal_source", "") or "").strip().lower()
        interaction_policy = (
            context.get("voice_interaction_policy", {})
            if isinstance(context.get("voice_interaction_policy", {}), dict)
            else {}
        )
        delivery_policy = (
            context.get("voice_delivery_policy", {})
            if isinstance(context.get("voice_delivery_policy", {}), dict)
            else {}
        )
        voice_policy = (
            context.get("voice_route_policy", {})
            if isinstance(context.get("voice_route_policy", {}), dict)
            else {}
        )
        voice_policy_summary = (
            voice_policy.get("summary", {})
            if isinstance(voice_policy.get("summary", {}), dict)
            else {}
        )
        planner_voice_route_policy = (
            context.get("planner_voice_route_policy", {})
            if isinstance(context.get("planner_voice_route_policy", {}), dict)
            else {}
        )
        route_recovery_recommendation = (
            context.get("voice_route_recovery_recommendation", {})
            if isinstance(context.get("voice_route_recovery_recommendation", {}), dict)
            else {}
        )
        mission_reliability = (
            voice_policy.get("mission_reliability", {})
            if isinstance(voice_policy.get("mission_reliability", {}), dict)
            else {}
        )
        metadata = context.get("metadata", {}) if isinstance(context.get("metadata", {}), dict) else {}
        source_is_voice = bool(source.startswith("voice") or "voice" in source)
        recovery_handoff_active = bool(
            context.get("voice_recovery_handoff", False)
            or metadata.get("voice_recovery_handoff", False)
            or metadata.get("voice_session_id")
            or metadata.get("voice_originated")
            or metadata.get("voice_trigger_type")
            or voice_policy_summary.get("status")
            or planner_voice_route_policy.get("risk_level")
            or route_recovery_recommendation.get("status")
            or route_recovery_recommendation.get("strategy")
        )
        if not (source_is_voice or recovery_handoff_active):
            return {}
        allowed = set(allowed_actions or self.allowed_actions)
        llm_allowed_actions = set(allowed)
        filtered_llm_actions: List[str] = []
        confirmation_mode = str(interaction_policy.get("confirmation_mode", "") or "").strip().lower()
        followup_mode = str(interaction_policy.get("followup_mode", "") or "").strip().lower()
        prefer_notification_followup = bool(interaction_policy.get("prefer_notification_followup", False))
        avoid_multi_turn_voice_loop = bool(interaction_policy.get("avoid_multi_turn_voice_loop", False))
        prefer_non_voice_completion = bool(interaction_policy.get("prefer_non_voice_completion", False))
        can_notify = "send_notification" in allowed
        can_clipboard = "clipboard_write" in allowed
        can_open_url = "open_url" in allowed
        can_open_app = "open_app" in allowed
        mission_risk_level = str(
            context.get("mission_risk_level", "")
            or interaction_policy.get("mission_risk_level", "")
            or metadata.get("risk_level", "")
            or mission_reliability.get("last_risk_level", "")
            or planner_voice_route_policy.get("risk_level", "")
            or ""
        ).strip().lower()
        if not mission_risk_level:
            mission_risk_level = str(route_recovery_recommendation.get("risk_level", "") or "").strip().lower()
        local_voice_pressure_score = max(0.0, min(1.0, float(interaction_policy.get("local_voice_pressure_score", 0.0) or 0.0)))
        pause_pressure = max(0.0, min(1.0, float(interaction_policy.get("pause_pressure", 0.0) or 0.0)))
        if pause_pressure <= 0.0:
            pause_pressure = max(
                0.0,
                min(
                    1.0,
                    float(
                        route_recovery_recommendation.get("pause_pressure", 0.0)
                        or planner_voice_route_policy.get("pause_pressure", 0.0)
                        or 0.0
                    ),
                ),
            )
        if local_voice_pressure_score <= 0.0:
            local_voice_pressure_score = max(
                0.0,
                min(
                    1.0,
                    float(
                        planner_voice_route_policy.get("local_voice_pressure_score", 0.0)
                        or route_recovery_recommendation.get("local_voice_pressure_score", 0.0)
                        or 0.0
                    ),
                ),
            )
        if (
            "tts_speak" in llm_allowed_actions
            and can_notify
            and (
                prefer_notification_followup
                or prefer_non_voice_completion
                or followup_mode == "notification"
                or (
                    avoid_multi_turn_voice_loop
                    and bool(delivery_policy.get("tts_recovery_pending", False))
                )
            )
        ):
            llm_allowed_actions.discard("tts_speak")
            filtered_llm_actions.append("tts_speak")
        verification_strictness = ""
        if confirmation_mode == "explicit":
            verification_strictness = "strict"
        elif confirmation_mode == "compact":
            verification_strictness = "standard"
        notification_title = (
            "JARVIS Voice Confirmation"
            if confirmation_mode == "explicit"
            else "JARVIS Voice Follow-up"
        )
        notification_message_max_chars = 320
        if prefer_notification_followup or followup_mode == "notification":
            notification_message_max_chars = 220 if confirmation_mode == "explicit" else 240
        elif avoid_multi_turn_voice_loop:
            notification_message_max_chars = 260
        clipboard_text_max_chars = 0
        if prefer_non_voice_completion or followup_mode in {"notification", "hybrid"}:
            clipboard_text_max_chars = 220 if confirmation_mode == "explicit" else 260
            if not avoid_multi_turn_voice_loop:
                clipboard_text_max_chars = max(clipboard_text_max_chars, 320)
        preferred_followup_action = ""
        if followup_mode == "notification":
            if can_notify:
                preferred_followup_action = "send_notification"
            elif can_clipboard:
                preferred_followup_action = "clipboard_write"
            elif can_open_url:
                preferred_followup_action = "open_url"
            elif can_open_app:
                preferred_followup_action = "open_app"
        elif prefer_non_voice_completion or avoid_multi_turn_voice_loop:
            if can_notify and (
                prefer_notification_followup
                or confirmation_mode == "explicit"
                or mission_risk_level == "high"
                or local_voice_pressure_score >= 0.7
                or pause_pressure >= 0.45
            ):
                preferred_followup_action = "send_notification"
            elif can_clipboard and (
                mission_risk_level in {"medium", "high"}
                or confirmation_mode == "compact"
                or pause_pressure < 0.72
                or not can_notify
            ):
                preferred_followup_action = "clipboard_write"
            elif can_open_url and mission_risk_level != "high":
                preferred_followup_action = "open_url"
            elif can_open_app and mission_risk_level == "low" and pause_pressure < 0.3 and local_voice_pressure_score < 0.55:
                preferred_followup_action = "open_app"
            elif can_clipboard and (
                pause_pressure < 0.72
                or not can_notify
            ):
                preferred_followup_action = "clipboard_write"
            elif can_notify:
                preferred_followup_action = "send_notification"
            elif can_open_url:
                preferred_followup_action = "open_url"
            elif can_open_app:
                preferred_followup_action = "open_app"
        elif mission_risk_level == "low" and can_open_app and local_voice_pressure_score < 0.62 and pause_pressure < 0.35:
            preferred_followup_action = "open_app"
        elif can_open_url and local_voice_pressure_score >= 0.78 and pause_pressure < 0.45:
            preferred_followup_action = "open_url"
        elif can_open_app and local_voice_pressure_score < 0.62 and pause_pressure < 0.35:
            preferred_followup_action = "open_app"
        followup_channel_priority: List[str] = []
        for action in (
            preferred_followup_action,
            "send_notification" if can_notify else "",
            "clipboard_write" if can_clipboard else "",
            "open_url" if can_open_url else "",
            "open_app" if can_open_app else "",
            "tts_speak" if "tts_speak" in allowed else "",
        ):
            clean_action = str(action or "").strip().lower()
            if clean_action and clean_action not in followup_channel_priority:
                followup_channel_priority.append(clean_action)
        policy_scope = "voice" if source_is_voice else "voice_recovery_handoff"
        handoff_reason = str(
            route_recovery_recommendation.get("reason", "")
            or planner_voice_route_policy.get("reason", "")
            or voice_policy_summary.get("reason", "")
            or voice_policy_summary.get("reason_code", "")
            or ""
        ).strip()
        return {
            "status": "success",
            "source": source or policy_scope,
            "policy_scope": policy_scope,
            "recovery_handoff_active": bool(recovery_handoff_active and not source_is_voice),
            "handoff_reason": handoff_reason,
            "confirmation_mode": confirmation_mode or "minimal",
            "followup_mode": followup_mode or "spoken",
            "prefer_notification_followup": bool(prefer_notification_followup),
            "prefer_non_voice_completion": bool(prefer_non_voice_completion),
            "avoid_multi_turn_voice_loop": bool(avoid_multi_turn_voice_loop),
            "mission_risk_level": mission_risk_level,
            "local_voice_pressure_score": round(local_voice_pressure_score, 6),
            "pause_pressure": round(pause_pressure, 6),
            "verification_strictness": verification_strictness,
            "notification_title": notification_title,
            "notification_message_max_chars": int(notification_message_max_chars),
            "clipboard_text_max_chars": int(clipboard_text_max_chars),
            "preferred_followup_action": preferred_followup_action,
            "runtime_redirect_action": preferred_followup_action,
            "followup_channel_priority": followup_channel_priority,
            "llm_allowed_actions": sorted(llm_allowed_actions),
            "filtered_llm_actions": sorted(filtered_llm_actions),
        }

    def _apply_voice_execution_policy(
        self,
        steps: List[PlanStep],
        context: Dict[str, object],
        *,
        allowed_actions: Set[str] | None = None,
    ) -> Tuple[List[PlanStep], Dict[str, Any]]:
        policy = self._voice_execution_policy(context, allowed_actions=allowed_actions)
        if not policy or not steps:
            return (steps, {})
        verification_strictness = str(policy.get("verification_strictness", "") or "").strip().lower()
        strictness_updates = 0
        notification_normalizations = 0
        clipboard_compactions = 0
        open_url_normalizations = 0
        notification_title = str(policy.get("notification_title", "") or "").strip()
        notification_message_max_chars = int(policy.get("notification_message_max_chars", 0) or 0)
        clipboard_text_max_chars = int(policy.get("clipboard_text_max_chars", 0) or 0)
        prefer_single_sentence = bool(
            policy.get("avoid_multi_turn_voice_loop", False) or policy.get("prefer_notification_followup", False)
        )
        channel_priority = [
            str(action or "").strip().lower()
            for action in policy.get("followup_channel_priority", [])
            if str(action or "").strip()
        ]
        present_followup_actions: List[str] = []
        followup_candidates: List[Tuple[int, int, str, Dict[str, Any], PlanStep]] = []
        mission_risk_level = str(policy.get("mission_risk_level", "") or "").strip().lower()
        recovery_handoff_active = bool(policy.get("recovery_handoff_active", False))
        def _followup_channel_reason(action: str, args: Dict[str, Any], suitability_bonus: int) -> str:
            if action == "send_notification":
                if mission_risk_level == "high":
                    return "high_risk_confirmation_path"
                if bool(policy.get("prefer_notification_followup", False)):
                    return "notification_preferred_by_voice_policy"
                return "notification_available_for_short_confirmation"
            if action == "clipboard_write":
                if mission_risk_level in {"medium", "high"}:
                    return "clipboard_safe_for_medium_or_high_risk_followup"
                if bool(policy.get("prefer_non_voice_completion", False)):
                    return "clipboard_preferred_for_non_voice_completion"
                return "clipboard_available_as_low_friction_handoff"
            if action == "open_url":
                return "open_url_kept_only_when_action_context_contains_url" if str(args.get("url", "")).strip() else "open_url_missing_target"
            if action == "open_app":
                return "open_app_allowed_for_low_risk_reentry" if suitability_bonus > 0 else "open_app_deprioritized_by_voice_risk"
            return "voice_followup_channel_available"

        for step in steps:
            if not isinstance(step, PlanStep):
                continue
            verify = dict(step.verify or {}) if isinstance(step.verify, dict) else {}
            current = str(verify.get("verification_strictness", "") or "").strip().lower()
            if current != "strict" and (
                verification_strictness == "strict" or (verification_strictness == "standard" and not current)
            ):
                verify["verification_strictness"] = verification_strictness
                step.verify = verify
                strictness_updates += 1
            clean_action = str(step.action or "").strip().lower()
            if clean_action in {"send_notification", "clipboard_write", "open_url", "open_app"}:
                if clean_action not in present_followup_actions:
                    present_followup_actions.append(clean_action)
                rank = channel_priority.index(clean_action) if clean_action in channel_priority else len(channel_priority)
                args = dict(step.args or {}) if isinstance(step.args, dict) else {}
                suitability_bonus = 0
                if clean_action == "send_notification":
                    suitability_bonus = 4 if mission_risk_level == "high" else (3 if policy.get("prefer_notification_followup", False) else 1)
                elif clean_action == "clipboard_write":
                    suitability_bonus = 3 if mission_risk_level in {"medium", "high"} else (2 if policy.get("prefer_non_voice_completion", False) else 0)
                elif clean_action == "open_url":
                    has_url = bool(str(args.get("url", "") or "").strip())
                    suitability_bonus = 3 if (has_url and mission_risk_level != "high") else (1 if has_url else -1)
                elif clean_action == "open_app":
                    has_app = bool(
                        str(
                            args.get("app_name", "")
                            or args.get("app", "")
                            or args.get("name", "")
                            or ""
                        ).strip()
                    )
                    if mission_risk_level == "high":
                        suitability_bonus = -2
                    else:
                        suitability_bonus = 4 if (has_app and mission_risk_level == "low") else (2 if has_app else -1)
                followup_candidates.append((rank, -suitability_bonus, clean_action, args, step))
                verify["voice_followup_policy_scope"] = str(policy.get("policy_scope", "voice") or "voice")
                if recovery_handoff_active:
                    verify["voice_recovery_handoff"] = True
                verify["voice_followup_channel_reason"] = _followup_channel_reason(clean_action, args, suitability_bonus)
                verify["voice_followup_priority_index"] = int(rank)
                verify["voice_followup_suitability_bonus"] = int(suitability_bonus)
                verify["voice_followup_selection_score"] = int(
                    100
                    - (rank * 11)
                    + (suitability_bonus * 9)
                    + (4 if recovery_handoff_active and clean_action != "open_app" else 0)
                )
                step.verify = verify
            if clean_action == "send_notification":
                args = dict(step.args or {}) if isinstance(step.args, dict) else {}
                raw_message = str(
                    args.get("message", "") or args.get("text", "") or args.get("title", "") or ""
                ).strip()
                changed = False
                if notification_title and str(args.get("title", "")).strip() != notification_title:
                    args["title"] = notification_title
                    changed = True
                if raw_message and notification_message_max_chars > 0:
                    compacted = self._compact_voice_delivery_text(
                        raw_message,
                        max_chars=notification_message_max_chars,
                        prefer_single_sentence=prefer_single_sentence,
                    )
                    if compacted and compacted != str(args.get("message", "")).strip():
                        args["message"] = compacted
                        changed = True
                if changed:
                    step.args = args
                    notification_normalizations += 1
            elif clean_action == "clipboard_write" and clipboard_text_max_chars > 0:
                args = dict(step.args or {}) if isinstance(step.args, dict) else {}
                raw_text = str(args.get("text", "") or "").strip()
                if raw_text:
                    compacted = self._compact_voice_delivery_text(
                        raw_text,
                        max_chars=clipboard_text_max_chars,
                        prefer_single_sentence=True,
                    )
                    if compacted and compacted != raw_text:
                        args["text"] = compacted
                        step.args = args
                        clipboard_compactions += 1
            elif clean_action == "open_url" and notification_message_max_chars > 0:
                args = dict(step.args or {}) if isinstance(step.args, dict) else {}
                changed = False
                open_url_text_max_chars = (
                    160
                    if prefer_single_sentence or bool(policy.get("prefer_non_voice_completion", False))
                    else min(notification_message_max_chars, 220)
                )
                for key in ("title", "label", "description", "message"):
                    raw_value = str(args.get(key, "") or "").strip()
                    if not raw_value:
                        continue
                    compacted = self._compact_voice_delivery_text(
                        raw_value,
                        max_chars=max(72, open_url_text_max_chars),
                        prefer_single_sentence=prefer_single_sentence,
                    )
                    if compacted and compacted != raw_value:
                        args[key] = compacted
                        changed = True
                if changed:
                    step.args = args
                    open_url_normalizations += 1
            elif clean_action == "open_app" and notification_message_max_chars > 0:
                args = dict(step.args or {}) if isinstance(step.args, dict) else {}
                changed = False
                open_app_text_max_chars = 120 if prefer_single_sentence else 180
                for key in ("app_name", "app", "name", "label"):
                    raw_value = str(args.get(key, "") or "").strip()
                    if not raw_value:
                        continue
                    compacted = self._compact_voice_delivery_text(
                        raw_value,
                        max_chars=max(48, open_app_text_max_chars),
                        prefer_single_sentence=True,
                    )
                    if compacted and compacted != raw_value:
                        args[key] = compacted
                        changed = True
                if changed:
                    step.args = args
        planner_followup_candidates = [
            {
                "action": action,
                "rank": int(rank),
                "priority_index": int(rank),
                "selection_score": int(
                    100
                    - (rank * 11)
                    + ((-_bonus_rank) * 9)
                    + (4 if recovery_handoff_active and action != "open_app" else 0)
                ),
                "suitability_bonus": int(-_bonus_rank),
                "channel_reason": _followup_channel_reason(action, args, int(-_bonus_rank)),
                "args": dict(step.args or {}) if isinstance(step.args, dict) else dict(args),
            }
            for rank, _bonus_rank, action, args, step in sorted(followup_candidates, key=lambda item: (item[0], item[1], item[2]))
        ]
        selected_present_followup_action = ""
        sorted_candidates = sorted(followup_candidates, key=lambda item: (item[0], item[1], item[2]))
        for index, (_rank, _bonus_rank, action, _args, step) in enumerate(sorted_candidates, start=1):
            verify = dict(step.verify or {}) if isinstance(step.verify, dict) else {}
            verify["voice_followup_candidate"] = True
            verify["voice_followup_rank"] = int(index)
            step.verify = verify
        runtime_redirect_action = str(policy.get("runtime_redirect_action", "") or "").strip().lower()
        runtime_redirect_args: Dict[str, Any] = {}
        if planner_followup_candidates:
            selected = planner_followup_candidates[0]
            selected_present_followup_action = str(selected.get("action", "") or "").strip().lower()
            runtime_redirect_action = str(selected.get("action", "") or "").strip().lower() or runtime_redirect_action
            runtime_redirect_args = (
                dict(selected.get("args", {}))
                if isinstance(selected.get("args", {}), dict)
                else {}
            )
        return (
            steps,
            {
                "verification_strictness": verification_strictness,
                "strictness_updates": int(strictness_updates),
                "notification_normalizations": int(notification_normalizations),
                "clipboard_compactions": int(clipboard_compactions),
                "open_url_normalizations": int(open_url_normalizations),
                "preferred_followup_action": str(policy.get("preferred_followup_action", "") or "").strip().lower(),
                "runtime_redirect_action": runtime_redirect_action,
                "runtime_redirect_args": runtime_redirect_args,
                "present_followup_actions": present_followup_actions,
                "planner_followup_candidates": planner_followup_candidates,
                "planner_followup_contract": {
                    "status": "success",
                    "policy_scope": str(policy.get("policy_scope", "voice") or "voice"),
                    "recovery_handoff_active": bool(recovery_handoff_active),
                    "ranking_strategy": "mission_risk_weighted_followup",
                    "handoff_reason": str(policy.get("handoff_reason", "") or "").strip(),
                    "preferred_followup_action": str(policy.get("preferred_followup_action", "") or "").strip().lower(),
                    "selected_followup_action": selected_present_followup_action or runtime_redirect_action,
                    "candidates": planner_followup_candidates,
                },
                "selected_present_followup_action": selected_present_followup_action,
                "mission_risk_level": mission_risk_level,
                "followup_channel_priority": list(policy.get("followup_channel_priority", []))
                if isinstance(policy.get("followup_channel_priority", []), list)
                else [],
                "applied": bool(
                    strictness_updates
                    or notification_normalizations
                    or clipboard_compactions
                    or open_url_normalizations
                ),
            },
        )

    def _voice_delivery_fallback_step(
        self,
        *,
        message: str,
        context: Dict[str, object],
        allowed_actions: Set[str] | None = None,
    ) -> PlanStep:
        policy = self._voice_delivery_policy(context, allowed_actions=allowed_actions)
        clean_message = str(message or "").strip() or "Voice delivery fallback triggered."
        if bool(policy.get("suppress_tts", False)) and bool(policy.get("notification_fallback_available", False)):
            return self._step(
                "send_notification",
                args={
                    "title": "JARVIS Voice Fallback",
                    "message": self._compact_voice_delivery_text(clean_message, max_chars=220),
                },
                verify={"expect_status": "success", "expect_key": "title"},
                can_retry=False,
            )
        max_chars = int(policy.get("max_tts_chars", 0) or 0)
        tts_text = (
            self._compact_voice_delivery_text(clean_message, max_chars=max_chars)
            if max_chars > 0
            else clean_message
        )
        return self._step(
            "tts_speak",
            args={"text": tts_text},
            verify={"optional": True},
            can_retry=False,
        )

    def _apply_voice_delivery_policy(
        self,
        steps: List[PlanStep],
        context: Dict[str, object],
        *,
        allowed_actions: Set[str] | None = None,
    ) -> Tuple[List[PlanStep], Dict[str, Any]]:
        policy = self._voice_delivery_policy(context, allowed_actions=allowed_actions)
        if not policy or not steps:
            return (steps, {})
        max_tts_chars = int(policy.get("max_tts_chars", 0) or 0)
        adjusted_steps: List[PlanStep] = []
        notification_fallback_count = 0
        compacted_tts_count = 0
        for step in steps:
            if not isinstance(step, PlanStep) or str(step.action or "").strip().lower() != "tts_speak":
                adjusted_steps.append(step)
                continue
            text = str(step.args.get("text", "") or "").strip()
            if bool(policy.get("suppress_tts", False)) and bool(policy.get("notification_fallback_available", False)):
                replacement = self._clone_step_with_action(
                    step,
                    action="send_notification",
                    args={
                        "title": "JARVIS Voice Fallback",
                        "message": self._compact_voice_delivery_text(text, max_chars=220),
                    },
                    verify={"expect_status": "success", "expect_key": "title"},
                    can_retry=False,
                )
                adjusted_steps.append(replacement)
                notification_fallback_count += 1
                continue
            if max_tts_chars > 0 and text:
                compacted = self._compact_voice_delivery_text(
                    text,
                    max_chars=max_tts_chars,
                    prefer_single_sentence=bool(policy.get("prefer_brief_response", False)),
                )
                if compacted and compacted != text:
                    step.args = dict(step.args or {})
                    step.args["text"] = compacted
                    compacted_tts_count += 1
            adjusted_steps.append(step)
        return (
            adjusted_steps,
            {
                "notification_fallback_count": int(notification_fallback_count),
                "compacted_tts_count": int(compacted_tts_count),
                "applied": bool(notification_fallback_count or compacted_tts_count),
            },
        )

    def _should_try_llm_replan(
        self,
        *,
        text: str,
        deterministic_intent: str,
        deterministic_steps: List[PlanStep],
        context: Dict[str, object],
    ) -> Tuple[bool, str]:
        if not self.llm_enabled:
            return (False, "llm_planner_disabled")

        source = str(context.get("source", "")).strip().lower()
        if source == "evaluation":
            return (False, "evaluation_mode")

        replan_attempt = self._clamp_int(context.get("replan_attempt", 0), minimum=0, maximum=100, default=0)
        if replan_attempt <= 0:
            return (False, "not_replan")
        if replan_attempt >= 4:
            return (False, "replan_budget_guard")

        failed_action = str(context.get("last_failure_action", "")).strip().lower()
        if not failed_action:
            return (False, "missing_failed_action")

        contract_raw = context.get("last_failure_external_contract", {})
        contract = contract_raw if isinstance(contract_raw, dict) else {}
        contract_code = str(contract.get("code", "")).strip().lower()
        failure_category = str(context.get("last_failure_category", "")).strip().lower()
        non_speak_steps = [step for step in deterministic_steps if isinstance(step, PlanStep) and step.action != "tts_speak"]

        if contract_code:
            if deterministic_intent in {"external_contract_repair_replan", "external_contract_discovery_replan"} and non_speak_steps:
                return (False, "deterministic_contract_replan_sufficient")
            return (True, "contract_repair_assist")

        if failed_action.startswith("external_"):
            if deterministic_intent in {"external_connector_replan", "non_retryable_replan", "replan_fallback"}:
                return (True, "external_replan_assist")
            if not non_speak_steps:
                return (True, "external_empty_replan")

        words = [token for token in re.split(r"\s+", text) if token]
        if failure_category in {"unknown", "non_retryable"} and len(words) >= 8 and not non_speak_steps:
            return (True, "complex_failure_no_actionable_steps")

        return (False, "deterministic_replan_sufficient")

    @staticmethod
    def _llm_replan_is_actionable(*, llm_steps: List[PlanStep], fallback_steps: List[PlanStep]) -> bool:
        if not llm_steps:
            return False
        llm_executable = [step for step in llm_steps if isinstance(step, PlanStep) and step.action != "tts_speak"]
        if llm_executable:
            return True
        fallback_executable = [step for step in fallback_steps if isinstance(step, PlanStep) and step.action != "tts_speak"]
        return len(fallback_executable) == 0

    def _llm_replan_allowed_actions(
        self,
        *,
        deterministic_steps: List[PlanStep],
        failed_action: str,
        allowed_actions: Set[str],
    ) -> Set[str]:
        base_allowed = set(allowed_actions or self.allowed_actions)
        selected: Set[str] = {"tts_speak"} if "tts_speak" in base_allowed else set()
        clean_failed_action = str(failed_action or "").strip().lower()
        if clean_failed_action in base_allowed:
            selected.add(clean_failed_action)
        for step in deterministic_steps:
            if isinstance(step, PlanStep) and step.action in base_allowed:
                selected.add(step.action)
        if clean_failed_action.startswith("external_"):
            for candidate in {
                "oauth_token_maintain",
                "external_connector_status",
                "external_connector_preflight",
                "external_email_list",
                "external_calendar_list_events",
                "external_doc_list",
                "external_task_list",
                "external_email_read",
                "external_doc_read",
                "external_task_update",
                "external_calendar_update_event",
            }:
                if candidate in base_allowed:
                    selected.add(candidate)
        if not selected:
            return base_allowed
        # Keep output constrained while still giving LLM enough flexibility.
        prioritized = sorted(selected)
        if len(prioritized) > max(8, self.max_llm_steps * 3):
            prioritized = prioritized[: max(8, self.max_llm_steps * 3)]
        return set(prioritized)

    def _build_llm_replan_text(
        self,
        *,
        text: str,
        context: Dict[str, object],
        deterministic_intent: str,
        deterministic_steps: List[PlanStep],
    ) -> str:
        failed_action = str(context.get("last_failure_action", "")).strip().lower()
        failure_category = str(context.get("last_failure_category", "")).strip().lower()
        failure_error = str(context.get("last_failure_error", "")).strip()
        contract_raw = context.get("last_failure_external_contract", {})
        contract = contract_raw if isinstance(contract_raw, dict) else {}
        contract_code = str(contract.get("code", "")).strip().lower()
        missing_fields = contract.get("missing_fields", []) if isinstance(contract.get("missing_fields", []), list) else []
        repair_memory_hints = context.get("repair_memory_hints", [])
        hint_summary: List[Dict[str, Any]] = []
        if isinstance(repair_memory_hints, list):
            for row in repair_memory_hints[:3]:
                if not isinstance(row, dict):
                    continue
                memory_signals = row.get("signals", [])
                if not isinstance(memory_signals, list) or not memory_signals:
                    continue
                top = memory_signals[0] if isinstance(memory_signals[0], dict) else {}
                hint_summary.append(
                    {
                        "provider": str(top.get("provider", "")).strip().lower(),
                        "contract_code": str(top.get("contract_code", "")).strip().lower(),
                        "status": str(top.get("status", "")).strip().lower(),
                    }
                )
        failure_clusters_raw = context.get("external_failure_clusters", [])
        cluster_summary: List[Dict[str, Any]] = []
        if isinstance(failure_clusters_raw, list):
            for row in failure_clusters_raw[:4]:
                if not isinstance(row, dict):
                    continue
                cluster_summary.append(
                    {
                        "action": str(row.get("action", "")).strip().lower(),
                        "contract_code": str(row.get("contract_code", "")).strip().lower(),
                        "preferred_provider": str(row.get("preferred_provider", "")).strip().lower(),
                        "risk_score": self._clamp_float(row.get("risk_score", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        "samples": self._clamp_int(row.get("samples", 0), minimum=0, maximum=100_000, default=0),
                    }
                )
        deterministic_actions = [step.action for step in deterministic_steps if isinstance(step, PlanStep)]
        summary = {
            "failed_action": failed_action,
            "failure_category": failure_category,
            "failure_error": failure_error,
            "contract_code": contract_code,
            "missing_fields": [str(item).strip() for item in missing_fields[:12] if str(item).strip()],
            "deterministic_replan_intent": deterministic_intent,
            "deterministic_replan_actions": deterministic_actions[:8],
            "repair_memory_hints": hint_summary,
            "external_failure_clusters": cluster_summary,
        }
        return (
            f"{text}\n\n"
            "Repair objective: recover the failed step with executable tool actions, not a generic explanation.\n"
            "If the failure is external connector related, prefer: oauth_token_maintain, external_connector_preflight, external_connector_status, "
            "provider-compatible retries, and ID-discovery list calls before update/read calls.\n"
            f"Failure summary: {json.dumps(summary, ensure_ascii=True)}"
        )

    def _should_try_llm(
        self,
        *,
        text: str,
        lowered: str,
        deterministic_intent: str,
        deterministic_steps: List[PlanStep],
        context: Dict[str, object],
    ) -> Tuple[bool, str]:
        if not self.llm_enabled:
            return (False, "llm_planner_disabled")

        source = str(context.get("source", "")).strip().lower()
        if source == "evaluation":
            return (False, "evaluation_mode")

        if context.get("force_llm_planner") is True:
            return (True, "forced_by_context")

        words = [token for token in re.split(r"\s+", text) if token]
        has_complex_markers = any(
            marker in lowered
            for marker in (
                " and then ",
                " then ",
                " after that ",
                " before ",
                " if ",
                " while ",
                ";",
            )
        )
        has_and_connector = " and " in lowered
        has_many_words = len(words) >= 11
        fallback_speak = deterministic_intent == "speak" and any(ch.isalpha() for ch in text)
        single_step = len(deterministic_steps) <= 1

        if fallback_speak and len(words) >= 4:
            return (True, "fallback_intent")
        if has_complex_markers and single_step:
            return (True, "complex_markers")
        if has_and_connector and len(words) >= 7 and single_step:
            return (True, "conjunction_request")
        if has_many_words and lowered.count(",") >= 2 and single_step:
            return (True, "multi_clause_request")

        return (False, "deterministic_sufficient")

    async def _build_llm_plan(
        self,
        *,
        text: str,
        context: Dict[str, object],
        allowed_actions: Set[str] | None = None,
    ) -> Optional[Tuple[str, List[PlanStep], Dict[str, str]]]:
        decision = self._choose_reasoning_provider(context)
        if decision is None:
            return None

        effective_allowed = set(allowed_actions or self.allowed_actions)
        prompt = self._build_llm_prompt(text=text, context=context, allowed_actions=effective_allowed)
        response = await self._query_reasoning_provider(prompt=prompt, decision=decision)
        if not response.strip():
            return None

        payload = self._extract_json_object(response)
        if not payload:
            return None

        parsed = self._normalize_llm_plan_payload(
            payload,
            original_text=text,
            allowed_actions=effective_allowed,
        )
        if parsed is None:
            return None

        intent, steps = parsed
        if not steps:
            return None
        return (intent, steps, {"provider": decision.provider, "model": decision.model})

    def _compute_local_reasoning_route_policy(
        self,
        *,
        runtime_payload: Dict[str, Any],
        bridge_payload: Dict[str, Any],
        external_snapshot: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        now_epoch = time.time()
        candidate_count = int(runtime_payload.get("candidate_count", 0) or 0)
        runtime_ready = bool(runtime_payload.get("runtime_ready", False))
        probe_healthy_count = int(runtime_payload.get("probe_healthy_count", 0) or 0)
        cooldown_count = int(runtime_payload.get("cooldown_count", 0) or 0)
        items = runtime_payload.get("items", []) if isinstance(runtime_payload.get("items"), list) else []
        cooldown_hint_s = max(
            [
                max(0, int(float(item.get("runtime_cooldown_remaining_s", 0.0) or 0.0)))
                for item in items
                if isinstance(item, dict)
            ]
            or [0]
        )
        bridge_configured = bool(
            bridge_payload.get("configured", False)
            or bridge_payload.get("endpoint_configured", False)
            or bridge_payload.get("server_command_configured", False)
        )
        bridge_ready = bool(bridge_payload.get("ready", False))
        bridge_running = bool(bridge_payload.get("running", False))
        local_route_viable = bool(candidate_count > 0 and runtime_ready and (not bridge_configured or bridge_ready))
        review_required = bool(candidate_count > 0 and probe_healthy_count <= 0)
        blacklisted = False
        reason_code = ""
        reason = ""
        preferred_provider = "local"
        cloud_fallback_candidates: List[str] = []
        profile_id = ""
        template_id = ""
        autonomous_score = 1.0 if local_route_viable else 0.0

        external = dict(external_snapshot or {}) if isinstance(external_snapshot, dict) else {}
        external_updated_epoch = float(external.get("updated_at_epoch", 0.0) or 0.0)
        external_fresh = external_updated_epoch > 0.0 and (now_epoch - external_updated_epoch) <= self.local_reasoning_route_policy_snapshot_ttl_s
        if external_fresh:
            blacklisted = bool(external.get("blacklisted", False))
            review_required = bool(review_required or external.get("review_required", False))
            local_route_viable = bool(local_route_viable and external.get("local_route_viable", True))
            cooldown_hint_s = max(cooldown_hint_s, int(float(external.get("cooldown_hint_s", 0.0) or 0.0)))
            reason_code = str(external.get("reason_code", "") or "").strip().lower()
            reason = str(external.get("reason", "") or "").strip()
            preferred_provider = str(external.get("recommended_provider", "local") or "local").strip().lower() or "local"
            cloud_fallback_candidates = [
                str(item or "").strip().lower()
                for item in external.get("cloud_fallback_candidates", [])
                if str(item or "").strip()
            ] if isinstance(external.get("cloud_fallback_candidates"), list) else []
            profile_id = str(external.get("profile_id", "") or "").strip().lower()
            template_id = str(external.get("template_id", "") or "").strip().lower()
            try:
                autonomous_score = max(
                    0.0,
                    min(1.0, float(external.get("autonomy_score", autonomous_score) or autonomous_score)),
                )
            except Exception:
                autonomous_score = max(0.0, min(1.0, autonomous_score))

        if not reason_code:
            if blacklisted:
                reason_code = "external_launch_template_blacklisted"
                reason = reason or "Desktop launch policy blacklisted the active local reasoning launcher."
            elif candidate_count <= 0:
                reason_code = "no_local_reasoning_candidates"
                reason = "No local reasoning candidates are available."
            elif bridge_configured and not bridge_ready:
                reason_code = "local_reasoning_bridge_not_ready"
                reason = "The local reasoning bridge is configured but not ready."
            elif cooldown_count > 0 and cooldown_hint_s > 0:
                reason_code = "local_reasoning_runtime_cooldown"
                reason = f"Local reasoning runtime is cooling down for roughly {cooldown_hint_s}s."
            elif not runtime_ready:
                reason_code = "local_reasoning_runtime_unready"
                reason = "Local reasoning runtime is not currently ready."
            elif review_required:
                reason_code = "local_reasoning_probe_unverified"
                reason = "Local reasoning runtime lacks a recent healthy probe."
            else:
                reason_code = "local_reasoning_route_ready"
                reason = "Local reasoning runtime is viable for planner routing."

        if not cloud_fallback_candidates:
            cloud_fallback_candidates = ["groq", "nvidia"]
        if preferred_provider == "local" and reason_code not in {"local_reasoning_route_ready"} and cloud_fallback_candidates:
            preferred_provider = cloud_fallback_candidates[0]

        autonomous_allowed = bool(local_route_viable and not review_required and not blacklisted)
        if external_fresh and "autonomous_allowed" in external:
            autonomous_allowed = bool(autonomous_allowed and external.get("autonomous_allowed", False))
        if not local_route_viable:
            autonomous_score = min(autonomous_score, 0.3 if candidate_count > 0 else 0.0)
        elif review_required:
            autonomous_score = min(autonomous_score, 0.55)
        elif blacklisted:
            autonomous_score = min(autonomous_score, 0.2)
        else:
            autonomous_score = max(autonomous_score, 0.82 if bridge_running or not bridge_configured else 0.68)

        return {
            "status": "success",
            "source": "planner_runtime",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "updated_at_epoch": now_epoch,
            "candidate_count": candidate_count,
            "runtime_ready": runtime_ready,
            "probe_healthy_count": probe_healthy_count,
            "cooldown_count": cooldown_count,
            "bridge_configured": bridge_configured,
            "bridge_ready": bridge_ready,
            "bridge_running": bridge_running,
            "local_route_viable": bool(local_route_viable),
            "autonomous_allowed": bool(autonomous_allowed),
            "review_required": bool(review_required),
            "blacklisted": bool(blacklisted),
            "cooldown_hint_s": int(cooldown_hint_s),
            "autonomy_score": round(float(max(0.0, min(1.0, autonomous_score))), 4),
            "reason_code": reason_code,
            "reason": reason,
            "recommended_provider": preferred_provider,
            "cloud_fallback_candidates": cloud_fallback_candidates[:4],
            "profile_id": profile_id,
            "template_id": template_id,
            "external_snapshot_active": bool(external_fresh),
        }

    def update_local_reasoning_route_policy_snapshot(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        snapshot = dict(payload or {}) if isinstance(payload, dict) else {}
        snapshot["updated_at"] = datetime.now(timezone.utc).isoformat()
        snapshot["updated_at_epoch"] = time.time()
        self._local_reasoning_route_policy_snapshot = snapshot
        return dict(snapshot)

    def local_reasoning_route_policy_snapshot(
        self,
        *,
        refresh: bool = False,
        runtime_payload: Optional[Dict[str, Any]] = None,
        bridge_payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not refresh and isinstance(self._local_reasoning_route_policy_snapshot, dict) and self._local_reasoning_route_policy_snapshot:
            cached = dict(self._local_reasoning_route_policy_snapshot)
            cached_epoch = float(cached.get("computed_at_epoch", cached.get("updated_at_epoch", 0.0)) or 0.0)
            if cached_epoch > 0.0 and (time.time() - cached_epoch) <= 5.0:
                return cached
        runtime = dict(runtime_payload or {}) if isinstance(runtime_payload, dict) and runtime_payload else self.local_reasoning_runtime_status(limit=8)
        bridge = dict(bridge_payload or {}) if isinstance(bridge_payload, dict) and bridge_payload else (
            runtime.get("bridge", {}) if isinstance(runtime.get("bridge"), dict) else self.local_reasoning_bridge_status(probe=False)
        )
        policy = self._compute_local_reasoning_route_policy(
            runtime_payload=runtime,
            bridge_payload=bridge,
            external_snapshot=self._local_reasoning_route_policy_snapshot,
        )
        policy["computed_at_epoch"] = time.time()
        self._local_reasoning_route_policy_snapshot = dict(policy)
        return policy

    def _normalize_voice_route_policy_snapshot(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        raw = dict(payload or {}) if isinstance(payload, dict) else {}
        summary = raw.get("route_policy_summary", {}) if isinstance(raw.get("route_policy_summary", {}), dict) else {}
        stt = raw.get("stt", {}) if isinstance(raw.get("stt", {}), dict) else {}
        wakeword = raw.get("wakeword", {}) if isinstance(raw.get("wakeword", {}), dict) else {}
        tts = raw.get("tts", {}) if isinstance(raw.get("tts", {}), dict) else {}
        wakeword_supervision = raw.get("wakeword_supervision", {}) if isinstance(raw.get("wakeword_supervision", {}), dict) else {}
        mission_reliability = raw.get("mission_reliability", {}) if isinstance(raw.get("mission_reliability", {}), dict) else {}
        recovery = raw.get("route_recovery_recommendation", {}) if isinstance(raw.get("route_recovery_recommendation", {}), dict) else {}
        adaptive_wakeword_tuning = (
            raw.get("adaptive_wakeword_tuning", {})
            if isinstance(raw.get("adaptive_wakeword_tuning", {}), dict)
            else {}
        )
        pressure_score = max(0.0, min(1.0, float(raw.get("local_voice_pressure_score", 0.0) or 0.0)))
        planning_constraints = raw.get("planning_constraints", {}) if isinstance(raw.get("planning_constraints", {}), dict) else {}
        preferred_reasoning_provider = str(raw.get("preferred_reasoning_provider", "") or "").strip().lower()
        ban_local_reasoning = bool(raw.get("ban_local_reasoning", False))
        if not preferred_reasoning_provider and ban_local_reasoning:
            preferred_reasoning_provider = "groq"
        return {
            "status": str(raw.get("status", "success") or "success").strip().lower() or "success",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "updated_at_epoch": time.time(),
            "mission_id": str(raw.get("mission_id", "") or "").strip(),
            "risk_level": str(raw.get("risk_level", "") or "").strip().lower(),
            "policy_profile": str(raw.get("policy_profile", "") or "").strip().lower(),
            "reason_code": str(raw.get("reason_code", "") or summary.get("reason_code", "")).strip().lower(),
            "reason": str(raw.get("reason", "") or summary.get("reason", "")).strip(),
            "route_policy_summary": dict(summary),
            "stt": dict(stt),
            "wakeword": dict(wakeword),
            "tts": dict(tts),
            "wakeword_supervision": dict(wakeword_supervision),
            "mission_reliability": dict(mission_reliability),
            "route_recovery_recommendation": dict(recovery),
            "adaptive_wakeword_tuning": dict(adaptive_wakeword_tuning),
            "local_voice_pressure_score": round(pressure_score, 6),
            "ban_local_reasoning": bool(ban_local_reasoning),
            "preferred_reasoning_provider": preferred_reasoning_provider,
            "planning_constraints": dict(planning_constraints),
        }

    def update_voice_route_policy_snapshot(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        snapshot = self._normalize_voice_route_policy_snapshot(payload)
        self._voice_route_policy_snapshot = dict(snapshot)
        return dict(snapshot)

    def voice_route_policy_snapshot(self, *, refresh: bool = False) -> Dict[str, Any]:
        if isinstance(self._voice_route_policy_snapshot, dict) and self._voice_route_policy_snapshot:
            cached = dict(self._voice_route_policy_snapshot)
            cached_epoch = float(cached.get("updated_at_epoch", 0.0) or 0.0)
            if not refresh and cached_epoch > 0.0 and (time.time() - cached_epoch) <= self.voice_route_policy_snapshot_ttl_s:
                return cached
            return cached
        return {
            "status": "unknown",
            "updated_at": "",
            "updated_at_epoch": 0.0,
            "mission_id": "",
            "risk_level": "",
            "policy_profile": "",
            "reason_code": "",
            "reason": "",
            "route_policy_summary": {},
            "stt": {},
            "wakeword": {},
            "tts": {},
            "wakeword_supervision": {},
            "mission_reliability": {},
            "route_recovery_recommendation": {},
            "adaptive_wakeword_tuning": {},
            "local_voice_pressure_score": 0.0,
            "ban_local_reasoning": False,
            "preferred_reasoning_provider": "",
            "planning_constraints": {},
        }

    def _choose_reasoning_provider(self, context: Dict[str, object]) -> Optional[RouteDecision]:
        groq_ready = bool(getattr(self.groq_client, "is_ready", lambda: bool(self.groq_client.api_key))())
        nvidia_ready = bool(self.nvidia_client is not None and getattr(self.nvidia_client, "is_ready", lambda: True)())
        requires_offline = not bool(groq_ready or nvidia_ready)
        privacy_mode = bool(context.get("privacy_mode", False))
        mission_profile = str(context.get("policy_profile", "balanced") or "balanced").strip().lower() or "balanced"
        provider_snapshot = self._connector_provider_snapshot()
        route_plan = self.connector_orchestrator.plan_reasoning_route(
            registry=self.model_router.registry,
            provider_snapshot=provider_snapshot,
            requires_offline=requires_offline,
            privacy_mode=privacy_mode,
            mission_profile=mission_profile,
            max_fallbacks=3,
        )
        local_policy = provider_snapshot.get("local", {}).get("route_policy", {}) if isinstance(provider_snapshot.get("local", {}), dict) else {}
        source = str(context.get("source", "") or context.get("goal_source", "") or "").strip().lower()
        voice_request = source.startswith("voice") or "voice" in source
        context_voice_policy = context.get("voice_route_policy")
        if isinstance(context_voice_policy, dict) and context_voice_policy:
            voice_policy = dict(context_voice_policy)
        elif voice_request:
            voice_policy = self.voice_route_policy_snapshot(refresh=False)
        else:
            voice_policy = {}
        banned_providers = [
            str(item or "").strip().lower()
            for item in route_plan.get("banned_providers", [])
            if str(item or "").strip()
        ] if isinstance(route_plan.get("banned_providers"), list) else []
        if isinstance(local_policy, dict):
            local_route_viable = bool(local_policy.get("local_route_viable", True))
            autonomous_allowed = bool(local_policy.get("autonomous_allowed", True))
            if (not local_route_viable or (not autonomous_allowed and not requires_offline and not privacy_mode)) and "local" not in banned_providers:
                banned_providers.append("local")
            route_plan["local_route_policy"] = local_policy
        if isinstance(voice_policy, dict) and voice_policy:
            route_plan["voice_route_policy"] = voice_policy
            if bool(voice_policy.get("ban_local_reasoning", False)) and not requires_offline and not privacy_mode and "local" not in banned_providers:
                banned_providers.append("local")
        preferred_provider = str(route_plan.get("preferred_provider", "")).strip().lower()
        voice_preferred_provider = str(voice_policy.get("preferred_reasoning_provider", "")).strip().lower() if isinstance(voice_policy, dict) else ""
        if voice_preferred_provider and preferred_provider in {"", "local"} and voice_preferred_provider not in banned_providers:
            preferred_provider = voice_preferred_provider
            route_plan["preferred_provider_adjusted"] = preferred_provider
            route_plan["preferred_provider_adjustment_reason"] = (
                str(voice_policy.get("reason_code", "") or "voice_route_policy_pressure").strip().lower()
                or "voice_route_policy_pressure"
            )
        if preferred_provider == "local" and "local" in banned_providers:
            fallback_providers = [
                str(item or "").strip().lower()
                for item in route_plan.get("fallback_providers", [])
                if str(item or "").strip()
            ] if isinstance(route_plan.get("fallback_providers"), list) else []
            preferred_provider = next((provider for provider in fallback_providers if provider not in banned_providers), "")
            if preferred_provider:
                route_plan["preferred_provider_adjusted"] = preferred_provider
                route_plan["preferred_provider_adjustment_reason"] = str(local_policy.get("reason_code", "local_route_policy_gated") or "local_route_policy_gated")

        try:
            decision = self.model_router.choose(
                "reasoning",
                requires_offline=requires_offline,
                high_quality=True,
                privacy_mode=privacy_mode,
                mission_profile=mission_profile,
                preferred_provider=preferred_provider,
                banned_providers=banned_providers,
                provider_affinity=route_plan.get("provider_affinity", {}),
            )
            diagnostics = dict(decision.diagnostics or {})
            diagnostics["connector_route_plan"] = route_plan
            diagnostics["connector_snapshot"] = provider_snapshot
            diagnostics["local_route_policy"] = local_policy if isinstance(local_policy, dict) else {}
            diagnostics["voice_route_policy"] = voice_policy if isinstance(voice_policy, dict) else {}
            decision.diagnostics = diagnostics
            return decision
        except Exception as exc:  # noqa: BLE001
            self.log.warning(f"LLM planner routing failed: {exc}")
            return None

    async def _query_reasoning_provider(self, *, prompt: str, decision: RouteDecision) -> str:
        providers: List[str] = [decision.provider]
        route_plan = {}
        local_policy: Dict[str, Any] = {}
        if isinstance(decision.diagnostics, dict):
            route_plan = decision.diagnostics.get("connector_route_plan", {})
            local_policy = decision.diagnostics.get("local_route_policy", {}) if isinstance(decision.diagnostics.get("local_route_policy", {}), dict) else {}
        plan_fallbacks = route_plan.get("fallback_providers", []) if isinstance(route_plan, dict) else []
        if isinstance(plan_fallbacks, list):
            for row in plan_fallbacks:
                provider_name = str(row or "").strip().lower()
                if provider_name == "local" and decision.provider != "local" and not bool(local_policy.get("autonomous_allowed", True)):
                    continue
                if provider_name and provider_name not in providers:
                    providers.append(provider_name)
        for provider_name in ("groq", "nvidia", "local"):
            if provider_name == "local" and provider_name != decision.provider and not bool(local_policy.get("autonomous_allowed", True)):
                continue
            if provider_name not in providers:
                providers.append(provider_name)

        for provider in providers:
            started = time.monotonic()
            routed_model_name = decision.model if provider == decision.provider else ("groq-llm" if provider == "groq" else ("nvidia-nim" if provider == "nvidia" else "local-llm"))
            try:
                if provider == "groq" and bool(getattr(self.groq_client, "is_ready", lambda: bool(self.groq_client.api_key))()):
                    model = os.getenv("JARVIS_GROQ_PLANNER_MODEL", "llama-3.3-70b-versatile").strip() or "llama-3.3-70b-versatile"
                    text = await asyncio.wait_for(
                        self.groq_client.ask(prompt, model=model, temperature=0.1),
                        timeout=self.llm_timeout_s,
                    )
                    latency_ms = (time.monotonic() - started) * 1000.0
                    try:
                        self.model_router.registry.note_result(
                            routed_model_name,
                            success=bool(str(text).strip()),
                            latency_ms=latency_ms,
                        )
                        self.connector_orchestrator.report_outcome(
                            provider=provider,
                            success=bool(str(text).strip()),
                            latency_ms=latency_ms,
                        )
                    except Exception:
                        pass
                    return text

                if provider == "nvidia" and self.nvidia_client is not None and bool(getattr(self.nvidia_client, "is_ready", lambda: True)()):
                    model = os.getenv("JARVIS_NVIDIA_PLANNER_MODEL", "meta/llama-3.1-70b-instruct").strip() or "meta/llama-3.1-70b-instruct"
                    text = await asyncio.wait_for(
                        self.nvidia_client.generate_text(prompt, model=model, max_tokens=1200),
                        timeout=self.llm_timeout_s,
                    )
                    latency_ms = (time.monotonic() - started) * 1000.0
                    try:
                        self.model_router.registry.note_result(
                            routed_model_name,
                            success=bool(str(text).strip()),
                            latency_ms=latency_ms,
                        )
                        self.connector_orchestrator.report_outcome(
                            provider=provider,
                            success=bool(str(text).strip()),
                            latency_ms=latency_ms,
                        )
                    except Exception:
                        pass
                    return text

                if provider == "local" and self.local_reasoning_enabled:
                    text, local_model_name = await self._query_local_reasoning(
                        prompt=prompt,
                        preferred_model_name=decision.model if decision.provider == "local" else "",
                    )
                    latency_ms = (time.monotonic() - started) * 1000.0
                    used_model = str(local_model_name or routed_model_name).strip().lower() or "local-llm"
                    try:
                        self.model_router.registry.note_result(
                            used_model,
                            success=bool(str(text).strip()),
                            latency_ms=latency_ms,
                        )
                        self.connector_orchestrator.report_outcome(
                            provider=provider,
                            success=bool(str(text).strip()),
                            latency_ms=latency_ms,
                        )
                    except Exception:
                        pass
                    if str(text).strip():
                        return text
            except Exception as exc:  # noqa: BLE001
                latency_ms = (time.monotonic() - started) * 1000.0
                try:
                    self.model_router.registry.note_result(
                        routed_model_name,
                        success=False,
                        latency_ms=latency_ms,
                    )
                    self.connector_orchestrator.report_outcome(
                        provider=provider,
                        success=False,
                        latency_ms=latency_ms,
                        error=str(exc),
                    )
                    message = str(exc).lower()
                    if any(token in message for token in ("timeout", "tempor", "unavailable", "rate limit", "429", "5xx")):
                        self.model_router.registry.mark_outage(provider=provider, penalty=0.72, cooldown_s=90.0)
                except Exception:
                    pass
                self.log.warning(f"LLM planner provider {provider} failed: {exc}")
                continue

        return ""

    def _connector_provider_snapshot(self) -> Dict[str, Any]:
        snapshot: Dict[str, Any] = {}
        registry = getattr(self.model_router, "registry", None)
        provider_status_fn = getattr(registry, "provider_status_snapshot", None)
        if callable(provider_status_fn):
            try:
                raw = provider_status_fn()
                if isinstance(raw, dict):
                    snapshot.update({str(key).strip().lower(): dict(value) for key, value in raw.items() if isinstance(value, dict)})
            except Exception:
                pass

        if "groq" not in snapshot:
            snapshot["groq"] = {"provider": "groq", "ready": bool(getattr(self.groq_client, "is_ready", lambda: bool(self.groq_client.api_key))())}
        else:
            snapshot["groq"]["ready"] = bool(
                snapshot["groq"].get("ready", False) and bool(getattr(self.groq_client, "is_ready", lambda: bool(self.groq_client.api_key))())
            )
        if hasattr(self.groq_client, "diagnostics"):
            try:
                snapshot["groq"]["connector"] = self.groq_client.diagnostics()
            except Exception:
                pass

        nvidia_ready = bool(self.nvidia_client is not None and getattr(self.nvidia_client, "is_ready", lambda: True)())
        if "nvidia" not in snapshot:
            snapshot["nvidia"] = {"provider": "nvidia", "ready": nvidia_ready}
        else:
            snapshot["nvidia"]["ready"] = bool(snapshot["nvidia"].get("ready", False) and nvidia_ready)
        if self.nvidia_client is not None and hasattr(self.nvidia_client, "diagnostics"):
            try:
                snapshot["nvidia"]["connector"] = self.nvidia_client.diagnostics()
            except Exception:
                pass

        local_candidates = self._local_reasoning_candidates(preferred_model_name="")
        local_runtime = self.local_reasoning_runtime_status(limit=8)
        local_row = snapshot.get("local", {})
        if not isinstance(local_row, dict):
            local_row = {}
        runtime_ready = bool(local_runtime.get("runtime_ready", False))
        route_policy = self.local_reasoning_route_policy_snapshot(
            refresh=True,
            runtime_payload=local_runtime,
            bridge_payload=local_runtime.get("bridge", {}) if isinstance(local_runtime.get("bridge"), dict) else {},
        )
        local_row.update(
            {
                "provider": "local",
                "ready": bool(local_candidates) and runtime_ready and bool(self.local_reasoning_enabled) and bool(route_policy.get("local_route_viable", True)),
                "present": bool(local_candidates),
                "local_candidate_count": len(local_candidates),
                "local_candidates": local_candidates[:8],
                "runtime_ready": runtime_ready,
                "runtime_loaded_count": int(local_runtime.get("loaded_count", 0) or 0),
                "runtime_error_count": int(local_runtime.get("error_count", 0) or 0),
                "runtime": local_runtime,
                "route_policy": route_policy,
                "autonomous_allowed": bool(route_policy.get("autonomous_allowed", False)),
            }
        )
        snapshot["local"] = local_row
        return snapshot

    def connector_route_plan(
        self,
        *,
        requires_offline: bool = False,
        privacy_mode: bool = False,
        mission_profile: str = "balanced",
    ) -> Dict[str, Any]:
        provider_snapshot = self._connector_provider_snapshot()
        payload = self.connector_orchestrator.plan_reasoning_route(
            registry=self.model_router.registry,
            provider_snapshot=provider_snapshot,
            requires_offline=bool(requires_offline),
            privacy_mode=bool(privacy_mode),
            mission_profile=str(mission_profile or "balanced").strip().lower() or "balanced",
            max_fallbacks=4,
        )
        payload["local_route_policy"] = provider_snapshot.get("local", {}).get("route_policy", {}) if isinstance(provider_snapshot.get("local", {}), dict) else {}
        payload["voice_route_policy"] = self.voice_route_policy_snapshot(refresh=False)
        return payload

    def connector_diagnostics(
        self,
        *,
        include_route_plan: bool = True,
        requires_offline: bool = False,
        privacy_mode: bool = False,
        mission_profile: str = "balanced",
    ) -> Dict[str, Any]:
        payload = self.connector_orchestrator.diagnostics(limit_history=80)
        provider_snapshot = self._connector_provider_snapshot()
        payload["provider_snapshot"] = provider_snapshot
        payload["local_reasoning_runtime"] = self.local_reasoning_runtime_status(limit=8)
        payload["local_route_policy"] = provider_snapshot.get("local", {}).get("route_policy", {}) if isinstance(provider_snapshot.get("local", {}), dict) else {}
        payload["voice_route_policy"] = self.voice_route_policy_snapshot(refresh=False)
        if include_route_plan:
            payload["route_plan"] = self.connector_orchestrator.plan_reasoning_route(
                registry=self.model_router.registry,
                provider_snapshot=provider_snapshot,
                requires_offline=bool(requires_offline),
                privacy_mode=bool(privacy_mode),
                mission_profile=str(mission_profile or "balanced").strip().lower() or "balanced",
                max_fallbacks=4,
            )
            if isinstance(payload.get("route_plan", {}), dict):
                payload["route_plan"]["local_route_policy"] = payload["local_route_policy"]
                payload["route_plan"]["voice_route_policy"] = payload["voice_route_policy"]
        return payload

    def probe_model_connectors(self, *, active_probe: bool = False, timeout_s: float = 4.0) -> Dict[str, Any]:
        provider_snapshot = self._connector_provider_snapshot()
        probes: List[Dict[str, Any]] = []
        for provider_name in ("groq", "nvidia", "local"):
            row = provider_snapshot.get(provider_name, {})
            connector = row.get("connector", {}) if isinstance(row.get("connector", {}), dict) else {}
            probes.append(
                {
                    "provider": provider_name,
                    "ready": bool(row.get("ready", False)),
                    "present": bool(row.get("present", row.get("ready", False))),
                    "failure_ema": float(connector.get("failure_ema", 0.0) or 0.0),
                    "cooldown_until_epoch": float(connector.get("cooldown_until_epoch", 0.0) or 0.0),
                    "local_candidate_count": int(row.get("local_candidate_count", 0) or 0),
                    "active_probe": bool(active_probe),
                    "active_probe_executed": False,
                }
            )
        return {
            "status": "success",
            "active_probe": bool(active_probe),
            "timeout_s": float(max(0.5, min(timeout_s, 20.0))),
            "count": len(probes),
            "probes": probes,
            "provider_snapshot": provider_snapshot,
            "route_plan": self.connector_orchestrator.plan_reasoning_route(
                registry=self.model_router.registry,
                provider_snapshot=provider_snapshot,
                requires_offline=False,
                privacy_mode=False,
                mission_profile="balanced",
                max_fallbacks=4,
            ),
        }

    def update_connector_policy(self, updates: Dict[str, Any]) -> Dict[str, Any]:
        payload = self.connector_orchestrator.update_policy(updates if isinstance(updates, dict) else {})
        payload["diagnostics"] = self.connector_diagnostics(include_route_plan=False)
        return payload

    async def _query_local_reasoning(self, *, prompt: str, preferred_model_name: str = "") -> tuple[str, str]:
        clean_prompt = str(prompt or "").strip()
        if not clean_prompt:
            return ("", "")
        if len(clean_prompt) > self.local_reasoning_prompt_max_chars:
            clean_prompt = clean_prompt[-self.local_reasoning_prompt_max_chars :]

        try:
            return await asyncio.wait_for(
                asyncio.to_thread(
                    self._query_local_reasoning_sync,
                    clean_prompt,
                    preferred_model_name,
                ),
                timeout=self.local_reasoning_timeout_s,
            )
        except Exception as exc:  # noqa: BLE001
            self.log.warning(f"Local reasoning provider failed: {exc}")
            return ("", "")

    def local_reasoning_runtime_status(self, *, preferred_model_name: str = "", limit: int = 8) -> Dict[str, Any]:
        bounded_limit = max(1, min(int(limit), 32))
        candidates = self._local_reasoning_candidates(preferred_model_name=preferred_model_name)
        bridge_payload = self.local_reasoning_bridge_status(probe=False)
        loaded_count = sum(1 for row in candidates if bool(row.get("runtime_loaded", False)))
        ready_count = sum(
            1
            for row in candidates
            if bool(row.get("runtime_supported", False))
            and float(row.get("runtime_cooldown_remaining_s", 0.0) or 0.0) <= 0.0
            and (not bool(row.get("runtime_bridge_required", False)) or bool(row.get("runtime_bridge_ready", False)))
        )
        error_count = sum(1 for row in candidates if str(row.get("runtime_last_error", "")).strip())
        cooldown_count = sum(1 for row in candidates if float(row.get("runtime_cooldown_remaining_s", 0.0) or 0.0) > 0.0)
        probe_healthy_count = sum(1 for row in candidates if bool(row.get("runtime_last_probe_ok", False)))
        active_candidate = next((row for row in candidates if bool(row.get("runtime_loaded", False))), candidates[0] if candidates else None)
        bridge_transport_count = sum(1 for row in candidates if str(row.get("runtime_transport", "")).strip().lower() == "bridge")
        return {
            "status": "success",
            "enabled": bool(self.local_reasoning_enabled),
            "timeout_s": float(self.local_reasoning_timeout_s),
            "max_new_tokens": int(self.local_reasoning_max_new_tokens),
            "prompt_max_chars": int(self.local_reasoning_prompt_max_chars),
            "probe_enabled": bool(self.local_reasoning_probe_enabled),
            "probe_prompt": str(self.local_reasoning_probe_prompt),
            "probe_max_chars": int(self.local_reasoning_probe_max_chars),
            "failure_streak_threshold": int(self.local_reasoning_failure_streak_threshold),
            "failure_cooldown_s": float(self.local_reasoning_failure_cooldown_s),
            "candidate_count": len(candidates),
            "runtime_ready": bool(ready_count > 0),
            "loaded_count": int(loaded_count),
            "error_count": int(error_count),
            "cooldown_count": int(cooldown_count),
            "probe_healthy_count": int(probe_healthy_count),
            "bridge_transport_count": int(bridge_transport_count),
            "active_model": str(active_candidate.get("name", "")).strip().lower() if isinstance(active_candidate, dict) else "",
            "active_backend": str(active_candidate.get("backend", "")).strip().lower() if isinstance(active_candidate, dict) else "",
            "active_path": str(active_candidate.get("path", "")).strip() if isinstance(active_candidate, dict) else "",
            "bridge": bridge_payload,
            "items": candidates[:bounded_limit],
        }

    def warm_local_reasoning_runtime(
        self,
        *,
        preferred_model_name: str = "",
        load_all: bool = False,
        force_reload: bool = False,
    ) -> Dict[str, Any]:
        candidates = self._local_reasoning_candidates(preferred_model_name=preferred_model_name)
        if not candidates:
            return {"status": "error", "message": "No local reasoning candidates available.", "items": []}
        selected = candidates if load_all else candidates[:1]
        results: List[Dict[str, Any]] = []
        for row in selected:
            results.append(self._warm_local_reasoning_candidate(row, force_reload=force_reload))
        return {
            "status": "success" if any(str(item.get("status", "")).strip().lower() == "success" for item in results) else "error",
            "load_all": bool(load_all),
            "force_reload": bool(force_reload),
            "count": len(results),
            "items": results,
            "runtime": self.local_reasoning_runtime_status(preferred_model_name=preferred_model_name, limit=8),
        }

    def probe_local_reasoning_runtime(
        self,
        *,
        preferred_model_name: str = "",
        prompt: str = "",
        force_reload: bool = False,
    ) -> Dict[str, Any]:
        if not bool(self.local_reasoning_probe_enabled):
            return {
                "status": "error",
                "message": "Local reasoning runtime probing is disabled.",
                "runtime": self.local_reasoning_runtime_status(preferred_model_name=preferred_model_name, limit=8),
            }

        candidates = self._local_reasoning_candidates(preferred_model_name=preferred_model_name)
        if not candidates:
            return {
                "status": "error",
                "message": "No local reasoning candidates available.",
                "runtime": self.local_reasoning_runtime_status(preferred_model_name=preferred_model_name, limit=8),
            }

        row = candidates[0]
        model_name = str(row.get("name", "")).strip().lower()
        model_path = str(row.get("path", "")).strip()
        backend = str(row.get("backend", "")).strip().lower()
        cooldown_remaining_s = float(row.get("runtime_cooldown_remaining_s", 0.0) or 0.0)
        if cooldown_remaining_s > 0.0 and not force_reload:
            message = f"Runtime probe is cooling down for {round(cooldown_remaining_s, 3)}s."
            self._mark_local_reasoning_runtime_probe(
                model_name=model_name,
                model_path=model_path,
                backend=backend,
                ok=False,
                latency_s=0.0,
                prompt="",
                response_preview="",
                error=message,
            )
            return {
                "status": "error",
                "model": model_name,
                "backend": backend,
                "message": message,
                "cooldown_remaining_s": round(cooldown_remaining_s, 3),
                "runtime": self.local_reasoning_runtime_status(preferred_model_name=preferred_model_name, limit=8),
            }

        clean_prompt = str(prompt or self.local_reasoning_probe_prompt or "").strip()
        if not clean_prompt:
            clean_prompt = "Summarize your runtime readiness in one short sentence."
        clean_prompt = clean_prompt[: self.local_reasoning_probe_max_chars]

        if force_reload:
            self.reset_local_reasoning_runtime(model_name=model_name, clear_all=False)

        started = time.monotonic()
        try:
            warm_result = self._warm_local_reasoning_candidate(row, force_reload=False)
            if str(warm_result.get("status", "")).strip().lower() != "success":
                raise RuntimeError(str(warm_result.get("message", "runtime warmup failed")).strip() or "runtime warmup failed")
            if str(row.get("runtime_transport", "")).strip().lower() == "bridge":
                response_text = self._run_local_reasoning_bridge_completion(
                    prompt=clean_prompt,
                    model_name=model_name,
                    model_path=model_path,
                )
            elif backend == "transformers":
                response_text = self._run_local_transformers_reasoning(prompt=clean_prompt, model_path=model_path)
            elif backend == "llama_cpp":
                response_text = self._run_local_llama_cpp_reasoning(prompt=clean_prompt, model_path=model_path)
            else:
                raise RuntimeError(f"Unsupported reasoning backend '{backend}'.")
            probe_latency_s = max(0.0, time.monotonic() - started)
            preview = str(response_text or "").strip()
            if not preview:
                raise RuntimeError("runtime returned an empty probe response")
            self._mark_local_reasoning_runtime_success(
                model_name=model_name,
                model_path=model_path,
                backend=backend,
                load_latency_s=probe_latency_s,
            )
            self._mark_local_reasoning_runtime_probe(
                model_name=model_name,
                model_path=model_path,
                backend=backend,
                ok=True,
                latency_s=probe_latency_s,
                prompt=clean_prompt,
                response_preview=preview[:240],
                error="",
            )
            return {
                "status": "success",
                "model": model_name,
                "backend": backend,
                "transport": str(row.get("runtime_transport", backend)).strip().lower() or backend,
                "path": model_path,
                "probe_prompt": clean_prompt,
                "probe_latency_s": round(probe_latency_s, 4),
                "response_preview": preview[:240],
                "runtime": self.local_reasoning_runtime_status(preferred_model_name=preferred_model_name or model_name, limit=8),
            }
        except Exception as exc:  # noqa: BLE001
            probe_latency_s = max(0.0, time.monotonic() - started)
            self._mark_local_reasoning_runtime_failure(
                model_name=model_name,
                model_path=model_path,
                backend=backend,
                error=str(exc),
            )
            self._mark_local_reasoning_runtime_probe(
                model_name=model_name,
                model_path=model_path,
                backend=backend,
                ok=False,
                latency_s=probe_latency_s,
                prompt=clean_prompt,
                response_preview="",
                error=str(exc),
            )
            return {
                "status": "error",
                "model": model_name,
                "backend": backend,
                "transport": str(row.get("runtime_transport", backend)).strip().lower() or backend,
                "path": model_path,
                "probe_prompt": clean_prompt,
                "probe_latency_s": round(probe_latency_s, 4),
                "message": str(exc),
                "runtime": self.local_reasoning_runtime_status(preferred_model_name=preferred_model_name or model_name, limit=8),
            }

    def reset_local_reasoning_runtime(
        self,
        *,
        model_name: str = "",
        clear_all: bool = False,
    ) -> Dict[str, Any]:
        normalized_name = str(model_name or "").strip().lower()
        removed: List[str] = []
        removed_bridge_paths: List[str] = []
        with self._local_reasoning_lock:
            if clear_all:
                bridge_candidates = self._local_reasoning_candidates(preferred_model_name=normalized_name)
                removed_bridge_paths = [
                    str(row.get("path", "")).strip()
                    for row in bridge_candidates
                    if str(row.get("runtime_transport", "")).strip().lower() == "bridge"
                ]
                for model_path, client in list(self._local_llama_cpp_cache.items()):
                    self._close_local_runtime_object(client)
                    removed.append(str(model_path))
                self._local_llama_cpp_cache.clear()
                for model_path, bundle in list(self._local_transformers_cache.items()):
                    self._close_local_runtime_object(bundle.get("model") if isinstance(bundle, dict) else None)
                    removed.append(str(model_path))
                self._local_transformers_cache.clear()
            else:
                candidates = self._local_reasoning_candidates(preferred_model_name=normalized_name)
                target_paths = {
                    str(row.get("path", "")).strip()
                    for row in candidates
                    if not normalized_name or normalized_name in str(row.get("name", "")).strip().lower()
                }
                removed_bridge_paths = [
                    str(row.get("path", "")).strip()
                    for row in candidates
                    if str(row.get("path", "")).strip() in target_paths and str(row.get("runtime_transport", "")).strip().lower() == "bridge"
                ]
                for model_path in list(target_paths):
                    client = self._local_llama_cpp_cache.pop(model_path, None)
                    if client is not None:
                        self._close_local_runtime_object(client)
                        removed.append(model_path)
                    bundle = self._local_transformers_cache.pop(model_path, None)
                    if isinstance(bundle, dict):
                        self._close_local_runtime_object(bundle.get("model"))
                        removed.append(model_path)
            for runtime in self._local_reasoning_runtime_state.values():
                if clear_all or str(runtime.get("path", "")).strip() in removed:
                    runtime["loaded"] = False
                    runtime["active_backend"] = ""
                    runtime["failure_streak"] = 0
                    runtime["cooldown_until"] = 0.0
                    runtime["last_probe_ok"] = False
                    runtime["last_probe_error"] = ""
                    runtime["last_probe_latency_s"] = 0.0
        bridge_payload: Dict[str, Any] | None = None
        if clear_all and removed_bridge_paths:
            bridge_payload = self.stop_local_reasoning_bridge(reason="planner_reset_clear_all")
        return {
            "status": "success",
            "cleared_all": bool(clear_all),
            "removed_count": len(removed),
            "removed_paths": removed[:16],
            "bridge": bridge_payload,
            "runtime": self.local_reasoning_runtime_status(preferred_model_name=normalized_name, limit=8),
        }

    def _query_local_reasoning_sync(self, prompt: str, preferred_model_name: str = "") -> tuple[str, str]:
        candidates = self._local_reasoning_candidates(preferred_model_name=preferred_model_name)
        if not candidates:
            return ("", "")

        for row in candidates:
            model_name = str(row.get("name", "")).strip().lower()
            model_path = str(row.get("path", "")).strip()
            backend = str(row.get("backend", "")).strip().lower()
            if not model_name or not model_path:
                continue
            if not bool(row.get("runtime_supported", False)):
                self._mark_local_reasoning_runtime_failure(
                    model_name=model_name,
                    model_path=model_path,
                    backend=backend,
                    error=str(row.get("runtime_reason", "runtime unavailable")),
                )
                continue
            started = time.monotonic()
            try:
                if str(row.get("runtime_transport", "")).strip().lower() == "bridge":
                    text = self._run_local_reasoning_bridge_completion(
                        prompt=prompt,
                        model_name=model_name,
                        model_path=model_path,
                    )
                elif backend == "transformers":
                    text = self._run_local_transformers_reasoning(prompt=prompt, model_path=model_path)
                elif backend == "llama_cpp":
                    text = self._run_local_llama_cpp_reasoning(prompt=prompt, model_path=model_path)
                else:
                    text = ""
            except Exception as exc:  # noqa: BLE001
                self.log.warning(f"Local model {model_name} ({backend}) failed: {exc}")
                self._mark_local_reasoning_runtime_failure(
                    model_name=model_name,
                    model_path=model_path,
                    backend=backend,
                    error=str(exc),
                )
                continue
            self._mark_local_reasoning_runtime_success(
                model_name=model_name,
                model_path=model_path,
                backend=backend,
                load_latency_s=max(0.0, time.monotonic() - started),
            )
            if str(text).strip():
                return (str(text).strip(), model_name)
        return ("", "")

    def _local_reasoning_candidates(self, *, preferred_model_name: str = "") -> List[Dict[str, str]]:
        registry = getattr(self.model_router, "registry", None)
        candidates: List[Dict[str, str]] = []
        preferred = str(preferred_model_name or "").strip().lower()
        bridge_payload = self.local_reasoning_bridge_status(probe=False)
        bridge_endpoint_ready = bool(bridge_payload.get("endpoint_configured", False))
        bridge_ready = bool(bridge_payload.get("ready", False))

        if registry is not None:
            try:
                rows = registry.list_by_task("reasoning")
            except Exception:
                rows = []
            for profile in rows:
                if str(getattr(profile, "provider", "")).strip().lower() != "local":
                    continue
                metadata = profile.metadata if isinstance(profile.metadata, dict) else {}
                path = str(metadata.get("path", "")).strip()
                if not path:
                    continue
                model_path = Path(path)
                if not model_path.exists():
                    continue
                backend = "transformers" if model_path.is_dir() else ("llama_cpp" if model_path.suffix.lower() == ".gguf" else "")
                if not backend:
                    continue
                candidates.append({"name": profile.name, "path": str(model_path), "backend": backend})

        explicit_path = str(os.getenv("JARVIS_LOCAL_REASONING_MODEL_PATH", "")).strip()
        if explicit_path:
            path_obj = Path(explicit_path)
            if path_obj.exists():
                backend = "transformers" if path_obj.is_dir() else ("llama_cpp" if path_obj.suffix.lower() == ".gguf" else "")
                if backend:
                    candidates.append({"name": "local-explicit-reasoning", "path": str(path_obj), "backend": backend})

        def _candidate_rank(item: Dict[str, str]) -> tuple[int, int]:
            name = str(item.get("name", "")).strip().lower()
            path = str(item.get("path", "")).replace("\\", "/").strip().lower()
            score = 0
            if preferred and name == preferred:
                score += 100
            if "/all_rounder/" in path or "/qwen3-14b" in path:
                score += 50
            if str(item.get("backend", "")).strip().lower() == "transformers":
                score += 10
            return (-score, len(path))

        candidates.sort(key=_candidate_rank)
        dedup: List[Dict[str, str]] = []
        seen = set()
        for row in candidates:
            key = f"{str(row.get('name','')).strip().lower()}::{str(row.get('path','')).strip().lower()}"
            if key in seen:
                continue
            seen.add(key)
            model_name = str(row.get("name", "")).strip().lower()
            model_path = str(row.get("path", "")).strip()
            backend = str(row.get("backend", "")).strip().lower()
            transport = "bridge" if backend == "llama_cpp" and bridge_endpoint_ready else backend
            if transport == "bridge":
                runtime_supported = True
                runtime_reason = "bridge_ready" if bridge_ready else "bridge_unready"
            else:
                runtime_supported, runtime_reason = self._local_reasoning_backend_supported(backend)
            runtime_row = self._local_reasoning_runtime_row(model_name=model_name, model_path=model_path, backend=backend)
            enriched = dict(row)
            enriched["runtime_supported"] = bool(runtime_supported)
            enriched["runtime_reason"] = runtime_reason
            enriched["runtime_transport"] = transport
            enriched["runtime_bridge_required"] = bool(transport == "bridge")
            enriched["runtime_bridge_ready"] = bool(transport != "bridge" or bridge_ready)
            enriched["runtime_loaded"] = bool(runtime_row.get("loaded", False))
            enriched["runtime_last_error"] = str(runtime_row.get("last_error", "")).strip()
            enriched["runtime_last_loaded_at"] = float(runtime_row.get("last_loaded_at", 0.0) or 0.0)
            enriched["runtime_load_latency_s"] = float(runtime_row.get("load_latency_s", 0.0) or 0.0)
            enriched["runtime_failure_streak"] = int(runtime_row.get("failure_streak", 0) or 0)
            enriched["runtime_cooldown_until"] = float(runtime_row.get("cooldown_until", 0.0) or 0.0)
            enriched["runtime_cooldown_remaining_s"] = round(
                max(0.0, float(runtime_row.get("cooldown_until", 0.0) or 0.0) - time.time()),
                3,
            )
            enriched["runtime_last_probe_at"] = float(runtime_row.get("last_probe_at", 0.0) or 0.0)
            enriched["runtime_last_probe_ok"] = bool(runtime_row.get("last_probe_ok", False))
            enriched["runtime_last_probe_error"] = str(runtime_row.get("last_probe_error", "")).strip()
            enriched["runtime_last_probe_latency_s"] = float(runtime_row.get("last_probe_latency_s", 0.0) or 0.0)
            enriched["runtime_probe_attempts"] = int(runtime_row.get("probe_attempts", 0) or 0)
            enriched["runtime_last_probe_preview"] = str(runtime_row.get("last_probe_preview", "")).strip()
            dedup.append(enriched)
        return dedup[:8]

    def _run_local_transformers_reasoning(self, *, prompt: str, model_path: str) -> str:
        bundle = self._ensure_local_transformers_bundle(model_path=model_path)

        tokenizer = bundle["tokenizer"]
        model = bundle["model"]
        max_input_tokens = 4096
        try:
            if int(getattr(tokenizer, "model_max_length", 0) or 0) > 0:
                max_input_tokens = max(512, min(int(tokenizer.model_max_length), 8192))
        except Exception:
            max_input_tokens = 4096

        encoded = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=max_input_tokens)
        try:
            if hasattr(model, "device"):
                encoded = {name: tensor.to(model.device) for name, tensor in encoded.items()}
        except Exception:
            pass

        generated = model.generate(
            **encoded,
            max_new_tokens=self.local_reasoning_max_new_tokens,
            do_sample=False,
            pad_token_id=getattr(tokenizer, "eos_token_id", None),
        )
        decoded = tokenizer.decode(generated[0], skip_special_tokens=True)
        if decoded.startswith(prompt):
            decoded = decoded[len(prompt) :]
        return str(decoded).strip()

    def _run_local_llama_cpp_reasoning(self, *, prompt: str, model_path: str) -> str:
        from backend.python.agents.local_llm import LLMMessage

        llm = self._ensure_local_llama_cpp_client(model_path=model_path)

        response = llm.generate([LLMMessage(role="user", content=prompt)], temperature=0.1)
        return str(getattr(response, "content", "")).strip()

    def _ensure_local_transformers_bundle(self, *, model_path: str) -> Dict[str, Any]:
        try:
            import torch
            from transformers import AutoModelForCausalLM, AutoTokenizer
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("transformers/torch are not available for local reasoning") from exc

        with self._local_reasoning_lock:
            bundle = self._local_transformers_cache.get(model_path)
            if isinstance(bundle, dict):
                return bundle
            kwargs: Dict[str, Any] = {"trust_remote_code": True}
            if bool(torch.cuda.is_available()):
                kwargs["device_map"] = "auto"
                kwargs["torch_dtype"] = torch.float16
            else:
                kwargs["torch_dtype"] = torch.float32
            tokenizer = AutoTokenizer.from_pretrained(model_path, trust_remote_code=True, use_fast=True)
            model = AutoModelForCausalLM.from_pretrained(model_path, **kwargs)
            bundle = {"tokenizer": tokenizer, "model": model}
            self._local_transformers_cache[model_path] = bundle
            return bundle

    def _ensure_local_llama_cpp_client(self, *, model_path: str) -> Any:
        try:
            from backend.python.agents.local_llm import LocalLLM
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("llama_cpp local client is not available") from exc

        with self._local_reasoning_lock:
            llm = self._local_llama_cpp_cache.get(model_path)
            if llm is None:
                llm = LocalLLM(model_path=model_path, max_tokens=self.local_reasoning_max_new_tokens, temperature=0.1)
                self._local_llama_cpp_cache[model_path] = llm
            return llm

    def _warm_local_reasoning_candidate(self, row: Dict[str, Any], *, force_reload: bool = False) -> Dict[str, Any]:
        model_name = str(row.get("name", "")).strip().lower()
        model_path = str(row.get("path", "")).strip()
        backend = str(row.get("backend", "")).strip().lower()
        transport = str(row.get("runtime_transport", backend)).strip().lower() or backend
        if not model_name or not model_path or not backend:
            return {"status": "error", "message": "invalid runtime candidate", "candidate": dict(row)}
        if not bool(row.get("runtime_supported", False)):
            message = str(row.get("runtime_reason", "runtime unavailable")).strip() or "runtime unavailable"
            self._mark_local_reasoning_runtime_failure(model_name=model_name, model_path=model_path, backend=backend, error=message)
            return {"status": "error", "model": model_name, "backend": backend, "message": message}
        cooldown_remaining_s = float(row.get("runtime_cooldown_remaining_s", 0.0) or 0.0)
        if cooldown_remaining_s > 0.0 and not force_reload:
            return {
                "status": "error",
                "model": model_name,
                "backend": backend,
                "message": f"runtime cooling down for {round(cooldown_remaining_s, 3)}s",
                "cooldown_remaining_s": round(cooldown_remaining_s, 3),
            }
        if force_reload:
            self.reset_local_reasoning_runtime(model_name=model_name, clear_all=False)
        started = time.monotonic()
        try:
            if transport == "bridge":
                bridge_payload = self.start_local_reasoning_bridge(
                    wait_ready=True,
                    reason=f"planner_warm:{model_name}",
                    force=bool(force_reload),
                )
                if not bool(bridge_payload.get("ready", False)):
                    raise RuntimeError(
                        str(bridge_payload.get("message", "local reasoning bridge is not ready")).strip()
                        or "local reasoning bridge is not ready"
                    )
            elif backend == "transformers":
                self._ensure_local_transformers_bundle(model_path=model_path)
            elif backend == "llama_cpp":
                self._ensure_local_llama_cpp_client(model_path=model_path)
            else:
                raise RuntimeError(f"Unsupported reasoning backend '{backend}'.")
        except Exception as exc:  # noqa: BLE001
            self._mark_local_reasoning_runtime_failure(model_name=model_name, model_path=model_path, backend=backend, error=str(exc))
            return {"status": "error", "model": model_name, "backend": backend, "message": str(exc)}
        latency_s = max(0.0, time.monotonic() - started)
        self._mark_local_reasoning_runtime_success(
            model_name=model_name,
            model_path=model_path,
            backend=backend,
            load_latency_s=latency_s,
        )
        return {
            "status": "success",
            "model": model_name,
            "backend": backend,
            "transport": transport,
            "path": model_path,
            "load_latency_s": round(latency_s, 4),
        }

    def local_reasoning_bridge_status(self, *, probe: bool = False) -> Dict[str, Any]:
        try:
            from backend.python.core.local_reasoning_bridge import LocalReasoningBridge
        except Exception as exc:  # noqa: BLE001
            return {
                "status": "error",
                "enabled": False,
                "configured": False,
                "managed": False,
                "ready": False,
                "message": str(exc),
            }
        try:
            payload = LocalReasoningBridge.shared().status(probe=bool(probe))
            return payload if isinstance(payload, dict) else {"status": "error", "message": "invalid bridge payload"}
        except Exception as exc:  # noqa: BLE001
            return {
                "status": "error",
                "enabled": False,
                "configured": False,
                "managed": False,
                "ready": False,
                "message": str(exc),
            }

    def start_local_reasoning_bridge(
        self,
        *,
        wait_ready: bool = True,
        timeout_s: Optional[float] = None,
        reason: str = "planner",
        force: bool = False,
    ) -> Dict[str, Any]:
        try:
            from backend.python.core.local_reasoning_bridge import LocalReasoningBridge
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}
        try:
            payload = LocalReasoningBridge.shared().start(
                wait_ready=bool(wait_ready),
                timeout_s=timeout_s,
                reason=str(reason or "planner"),
                force=bool(force),
            )
            return payload if isinstance(payload, dict) else {"status": "error", "message": "invalid bridge payload"}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    def stop_local_reasoning_bridge(self, *, reason: str = "planner") -> Dict[str, Any]:
        try:
            from backend.python.core.local_reasoning_bridge import LocalReasoningBridge
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}
        try:
            payload = LocalReasoningBridge.shared().stop(reason=str(reason or "planner"))
            return payload if isinstance(payload, dict) else {"status": "error", "message": "invalid bridge payload"}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    def probe_local_reasoning_bridge(self, *, force: bool = True) -> Dict[str, Any]:
        try:
            from backend.python.core.local_reasoning_bridge import LocalReasoningBridge
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}
        try:
            payload = LocalReasoningBridge.shared().probe(force=bool(force))
            return payload if isinstance(payload, dict) else {"status": "error", "message": "invalid bridge payload"}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    def restart_local_reasoning_bridge(
        self,
        *,
        wait_ready: bool = True,
        timeout_s: Optional[float] = None,
        reason: str = "planner",
        force: bool = True,
    ) -> Dict[str, Any]:
        try:
            from backend.python.core.local_reasoning_bridge import LocalReasoningBridge
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}
        try:
            payload = LocalReasoningBridge.shared().restart(
                wait_ready=bool(wait_ready),
                timeout_s=timeout_s,
                reason=str(reason or "planner"),
                force=bool(force),
            )
            return payload if isinstance(payload, dict) else {"status": "error", "message": "invalid bridge payload"}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    def apply_local_reasoning_bridge_overrides(
        self,
        *,
        updates: Dict[str, Any],
        profile_id: str = "",
        template_id: str = "",
        replace: bool = False,
    ) -> Dict[str, Any]:
        try:
            from backend.python.core.local_reasoning_bridge import LocalReasoningBridge
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}
        try:
            payload = LocalReasoningBridge.shared().set_runtime_overrides(
                updates=dict(updates or {}),
                profile_id=str(profile_id or "").strip(),
                template_id=str(template_id or "").strip(),
                replace=bool(replace),
            )
            return payload if isinstance(payload, dict) else {"status": "error", "message": "invalid bridge payload"}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    def clear_local_reasoning_bridge_overrides(self, *, keys: Optional[List[str]] = None) -> Dict[str, Any]:
        try:
            from backend.python.core.local_reasoning_bridge import LocalReasoningBridge
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}
        try:
            payload = LocalReasoningBridge.shared().clear_runtime_overrides(keys=list(keys) if isinstance(keys, list) else None)
            return payload if isinstance(payload, dict) else {"status": "error", "message": "invalid bridge payload"}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    def _run_local_reasoning_bridge_completion(self, *, prompt: str, model_name: str, model_path: str) -> str:
        try:
            from backend.python.core.local_reasoning_bridge import LocalReasoningBridge
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"local reasoning bridge unavailable: {exc}") from exc

        system_prompt = (
            "You are JARVIS local reasoning runtime. Answer directly and concisely. "
            "If asked for runtime readiness, summarize in one short sentence."
        )
        payload = LocalReasoningBridge.shared().complete(
            prompt=prompt,
            model=model_name or Path(model_path).name,
            max_tokens=self.local_reasoning_max_new_tokens,
            temperature=0.1,
            system_prompt=system_prompt,
            ensure_ready=True,
        )
        if str(payload.get("status", "")).strip().lower() != "success":
            raise RuntimeError(str(payload.get("message", "local reasoning bridge inference failed")).strip() or "local reasoning bridge inference failed")
        return str(payload.get("content", "")).strip()

    def _local_reasoning_backend_supported(self, backend: str) -> Tuple[bool, str]:
        clean_backend = str(backend or "").strip().lower()
        if clean_backend == "transformers":
            if importlib.util.find_spec("torch") is None:
                return (False, "torch_missing")
            if importlib.util.find_spec("transformers") is None:
                return (False, "transformers_missing")
            return (True, "available")
        if clean_backend == "llama_cpp":
            if importlib.util.find_spec("llama_cpp") is None:
                return (False, "llama_cpp_missing")
            return (True, "available")
        return (False, "unsupported_backend")

    def _runtime_key(self, model_name: str, model_path: str) -> str:
        return f"{str(model_name or '').strip().lower()}::{str(model_path or '').strip().lower()}"

    def _local_reasoning_runtime_row(self, *, model_name: str, model_path: str, backend: str) -> Dict[str, Any]:
        key = self._runtime_key(model_name, model_path)
        row = self._local_reasoning_runtime_state.get(key)
        if isinstance(row, dict):
            return row
        row = {
            "model": model_name,
            "path": model_path,
            "backend": backend,
            "loaded": False,
            "active_backend": "",
            "last_error": "",
            "attempts": 0,
            "successes": 0,
            "failures": 0,
            "failure_streak": 0,
            "cooldown_until": 0.0,
            "last_loaded_at": 0.0,
            "load_latency_s": 0.0,
            "last_probe_at": 0.0,
            "last_probe_ok": False,
            "last_probe_error": "",
            "last_probe_latency_s": 0.0,
            "probe_attempts": 0,
            "last_probe_preview": "",
            "last_probe_prompt": "",
        }
        self._local_reasoning_runtime_state[key] = row
        return row

    def _mark_local_reasoning_runtime_success(
        self,
        *,
        model_name: str,
        model_path: str,
        backend: str,
        load_latency_s: float,
    ) -> None:
        with self._local_reasoning_lock:
            row = self._local_reasoning_runtime_row(model_name=model_name, model_path=model_path, backend=backend)
            row["attempts"] = int(row.get("attempts", 0) or 0) + 1
            row["successes"] = int(row.get("successes", 0) or 0) + 1
            row["loaded"] = True
            row["active_backend"] = backend
            row["last_error"] = ""
            row["failure_streak"] = 0
            row["cooldown_until"] = 0.0
            row["last_loaded_at"] = time.time()
            row["load_latency_s"] = round(max(0.0, float(load_latency_s)), 4)

    def _mark_local_reasoning_runtime_failure(
        self,
        *,
        model_name: str,
        model_path: str,
        backend: str,
        error: str,
    ) -> None:
        with self._local_reasoning_lock:
            row = self._local_reasoning_runtime_row(model_name=model_name, model_path=model_path, backend=backend)
            row["attempts"] = int(row.get("attempts", 0) or 0) + 1
            row["failures"] = int(row.get("failures", 0) or 0) + 1
            row["failure_streak"] = int(row.get("failure_streak", 0) or 0) + 1
            row["loaded"] = False
            row["active_backend"] = ""
            row["last_error"] = str(error or "").strip()
            if int(row.get("failure_streak", 0) or 0) >= int(self.local_reasoning_failure_streak_threshold):
                row["cooldown_until"] = time.time() + float(self.local_reasoning_failure_cooldown_s)

    def _mark_local_reasoning_runtime_probe(
        self,
        *,
        model_name: str,
        model_path: str,
        backend: str,
        ok: bool,
        latency_s: float,
        prompt: str,
        response_preview: str,
        error: str,
    ) -> None:
        with self._local_reasoning_lock:
            row = self._local_reasoning_runtime_row(model_name=model_name, model_path=model_path, backend=backend)
            row["probe_attempts"] = int(row.get("probe_attempts", 0) or 0) + 1
            row["last_probe_at"] = time.time()
            row["last_probe_ok"] = bool(ok)
            row["last_probe_error"] = str(error or "").strip()
            row["last_probe_latency_s"] = round(max(0.0, float(latency_s)), 4)
            row["last_probe_prompt"] = str(prompt or "").strip()
            row["last_probe_preview"] = str(response_preview or "").strip()

    @staticmethod
    def _close_local_runtime_object(value: Any) -> None:
        if value is None:
            return
        try:
            close_fn = getattr(value, "close", None)
            if callable(close_fn):
                close_fn()
        except Exception:
            return

    def _build_llm_prompt(self, *, text: str, context: Dict[str, object], allowed_actions: Set[str]) -> str:
        failure_recovery = context.get("last_failure_recovery", {})
        failure_recovery_view: Dict[str, Any] = {}
        if isinstance(failure_recovery, dict):
            retry_history = failure_recovery.get("retry_history", [])
            if isinstance(retry_history, list):
                retry_history = retry_history[:3]
            else:
                retry_history = []
            failure_recovery_view = {
                "retry_count": failure_recovery.get("retry_count", 0),
                "last_category": failure_recovery.get("last_category", ""),
                "retry_history": retry_history,
            }
        context_view = {
            "source": context.get("source"),
            "policy_profile": context.get("policy_profile", ""),
            "replan_attempt": context.get("replan_attempt", 0),
            "replan_policy": context.get("replan_policy", {}),
            "last_failure_action": context.get("last_failure_action"),
            "last_failure_error": context.get("last_failure_error"),
            "last_failure_category": context.get("last_failure_category"),
            "last_failure_attempt": context.get("last_failure_attempt"),
            "last_failure_retry_count": context.get("last_failure_retry_count"),
            "last_failure_recovery": failure_recovery_view,
            "last_failure_confirm_policy": context.get("last_failure_confirm_policy", {}),
            "last_failure_desktop_state": context.get("last_failure_desktop_state", {}),
            "last_failure_external_reliability": context.get("last_failure_external_reliability", {}),
            "last_failure_external_contract": context.get("last_failure_external_contract", {}),
            "last_failure_request": context.get("last_failure_request", {}),
            "execution_feedback": context.get("execution_feedback", {}),
            "mission_feedback": context.get("mission_feedback", {}),
            "external_reliability_trend": context.get("external_reliability_trend", {}),
            "external_reliability_mission_analysis": context.get("external_reliability_mission_analysis", {}),
            "action_guardrails": context.get("action_guardrails", []),
            "action_guardrail_thresholds": context.get("action_guardrail_thresholds", {}),
            "guardrail_recommended_level": context.get("guardrail_recommended_level", ""),
            "guardrail_triggered_actions": context.get("guardrail_triggered_actions", []),
            "external_contract_guardrail": context.get("external_contract_guardrail", {}),
            "repair_memory_hints": context.get("repair_memory_hints", []),
            "external_failure_clusters": context.get("external_failure_clusters", []),
            "recent_goal_hints": context.get("recent_goal_hints", []),
            "retrieved_memory_hints": context.get("retrieved_memory_hints", []),
            "retrieved_episodic_hints": context.get("retrieved_episodic_hints", []),
            "retrieved_hybrid_hints": context.get("retrieved_hybrid_hints", []),
            "retrieved_episodic_strategy": context.get("retrieved_episodic_strategy", {}),
            "desktop_state_hints": context.get("desktop_state_hints", []),
        }
        schema_hint = {
            "intent": "string",
            "steps": [
                {
                    "action": "one_of_allowed_actions",
                    "args": {"key": "value"},
                    "verify": {"optional": True},
                    "can_retry": True,
                    "max_retries": 2,
                    "timeout_s": 30,
                }
            ],
        }
        return (
            "You are JARVIS desktop planner.\n"
            "Return ONLY strict JSON. No markdown.\n"
            f"Allowed actions: {sorted(allowed_actions)}\n"
            f"Schema: {json.dumps(schema_hint, ensure_ascii=True)}\n"
            "Rules:\n"
            "- Plan 1 to 4 steps.\n"
            "- Use safe minimal actions.\n"
            "- Never invent unknown actions.\n"
            "- If uncertain, use one step tts_speak.\n"
            f"Context: {json.dumps(context_view, ensure_ascii=True)}\n"
            f"User request: {text}\n"
        )
    def _normalize_llm_plan_payload(
        self,
        payload: Dict[str, Any],
        *,
        original_text: str,
        allowed_actions: Set[str],
    ) -> Optional[Tuple[str, List[PlanStep]]]:
        raw_steps = payload.get("steps")
        intent = str(payload.get("intent", "")).strip() or "llm_plan"

        if isinstance(raw_steps, list):
            steps = self._coerce_llm_steps(raw_steps, allowed_actions=allowed_actions)
            if steps:
                return (intent, steps)

        llm_intent = str(payload.get("intent", "")).strip().lower()
        llm_args = payload.get("arguments", {})
        if not isinstance(llm_args, dict):
            llm_args = {}

        mapped = self._map_reasoning_intent_to_step(intent=llm_intent, arguments=llm_args, original_text=original_text)
        if mapped is None:
            return None
        if mapped.action not in allowed_actions:
            return None
        return (mapped.action, [mapped])

    def _coerce_llm_steps(self, raw_steps: List[Any], *, allowed_actions: Set[str] | None = None) -> List[PlanStep]:
        effective_allowed = set(allowed_actions or self.allowed_actions)
        steps: List[PlanStep] = []
        for item in raw_steps[: self.max_llm_steps]:
            if not isinstance(item, dict):
                continue

            action = str(item.get("action", "")).strip()
            if action not in effective_allowed:
                continue

            raw_args = item.get("args", {})
            args = self._sanitize_args(raw_args if isinstance(raw_args, dict) else {})

            raw_verify = item.get("verify", {})
            verify = raw_verify if isinstance(raw_verify, dict) else {}

            can_retry = bool(item.get("can_retry", True))
            max_retries = self._clamp_int(item.get("max_retries", 2), minimum=0, maximum=4, default=2)
            timeout_s = self._clamp_int(item.get("timeout_s", 30), minimum=3, maximum=90, default=30)

            steps.append(
                self._step(
                    action,
                    args=args,
                    verify=verify,
                    can_retry=can_retry,
                    max_retries=max_retries,
                    timeout_s=timeout_s,
                )
            )
        return steps

    def _verification_template(self, action: str, args: Dict[str, Any]) -> Dict[str, Any]:
        template = dict(self.DEFAULT_VERIFY_BY_ACTION.get(action, {"expect_status": "success"}))
        retry_template = self.DEFAULT_RETRY_BY_ACTION.get(action)
        if isinstance(retry_template, dict) and retry_template:
            template = self._merge_verify_rules(template, {"retry": retry_template})
        app_name = str(args.get("app_name", "")).strip()
        title_arg = str(args.get("title", "")).strip()
        process_name = str(args.get("name", "")).strip()
        file_content = args.get("content")

        if action == "open_app":
            template = self._merge_verify_rules(
                template,
                {
                    "confirm": {
                        "action": "list_processes",
                        "args": {"limit": 120},
                        "required": False,
                        "attempts": 2,
                        "delay_s": 0.25,
                    }
                },
            )
            if app_name and "\\" not in app_name and "/" not in app_name:
                template = self._merge_verify_rules(
                    template,
                    {
                        "checks": [
                            {
                                "source": "confirm",
                                "type": "list_any_contains_arg",
                                "key": "process_names",
                                "arg": "app_name",
                                "normalize": "lower",
                                "strip_exe": True,
                            }
                        ]
                    },
                )
        elif action == "open_url":
            if str(args.get("url", "")).strip():
                template = self._merge_verify_rules(
                    template,
                    {
                        "checks": [
                            {
                                "source": "result",
                                "type": "contains_arg",
                                "key": "url",
                                "arg": "url",
                                "normalize": "lower",
                            }
                        ]
                    },
                )
        elif action == "media_search":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "contains", "key": "url", "value": "youtube.com/results"},
                        {"source": "result", "type": "equals_arg", "key": "query", "arg": "query"},
                    ]
                },
            )
        elif action == "defender_status":
            template = self._merge_verify_rules(
                template,
                {"checks": [{"source": "result", "type": "key_exists", "key": "raw"}]},
            )
        elif action == "system_snapshot":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "key_exists", "key": "metrics.cpu"},
                        {"source": "result", "type": "key_exists", "key": "metrics.memory"},
                        {"source": "result", "type": "key_exists", "key": "metrics.disk"},
                        {"source": "result", "type": "key_exists", "key": "metrics.network"},
                        {"source": "result", "type": "key_exists", "key": "metrics.system"},
                        {"source": "result", "type": "key_exists", "key": "metrics.timestamp"},
                    ]
                },
            )
        elif action == "list_processes":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "number_gte", "key": "count", "value": 0},
                        {"source": "result", "type": "key_exists", "key": "process_names"},
                    ]
                },
            )
        elif action == "terminate_process":
            template = self._merge_verify_rules(
                template,
                {
                    "confirm": {
                        "action": "list_processes",
                        "args": {"limit": 160},
                        "required": False,
                        "attempts": 2,
                        "delay_s": 0.25,
                    }
                },
            )
            if process_name:
                template = self._merge_verify_rules(
                    template,
                    {
                        "checks": [
                            {
                                "source": "confirm",
                                "type": "list_none_contains_arg",
                                "key": "process_names",
                                "arg": "name",
                                "normalize": "lower",
                                "strip_exe": True,
                            }
                        ]
                    },
                )
        elif action == "list_windows":
            template = self._merge_verify_rules(
                template,
                {"checks": [{"source": "result", "type": "number_gte", "key": "count", "value": 0}]},
            )
        elif action == "active_window":
            template = self._merge_verify_rules(
                template,
                {"checks": [{"source": "result", "type": "number_gte", "key": "window.hwnd", "value": 1}]},
            )
        elif action == "focus_window":
            template = self._merge_verify_rules(
                template,
                {
                    "confirm": {
                        "action": "active_window",
                        "args": {},
                        "required": False,
                        "attempts": 2,
                        "delay_s": 0.2,
                    },
                    "checks": [{"source": "confirm", "type": "number_gte", "key": "window.hwnd", "value": 1}],
                },
            )
            if title_arg:
                template = self._merge_verify_rules(
                    template,
                    {
                        "checks": [
                            {
                                "source": "confirm",
                                "type": "contains_arg",
                                "key": "window.title",
                                "arg": "title",
                                "normalize": "lower",
                            }
                        ]
                    },
                )
            if args.get("hwnd") is not None:
                template = self._merge_verify_rules(
                    template,
                    {"checks": [{"source": "confirm", "type": "equals_arg", "key": "window.hwnd", "arg": "hwnd"}]},
                )
        elif action == "send_notification":
            template = self._merge_verify_rules(
                template,
                {"checks": [{"source": "result", "type": "equals_arg", "key": "title", "arg": "title", "allow_missing_arg": True}]},
            )
        elif action == "search_files":
            template = self._merge_verify_rules(
                template,
                {
                    "confirm": {
                        "action": "list_folder",
                        "args": {"path": "{{result.base_dir}}"},
                        "required": False,
                        "attempts": 2,
                        "delay_s": 0.2,
                    },
                    "checks": [
                        {"source": "result", "type": "number_gte", "key": "count", "value": 0},
                        {"source": "result", "type": "key_exists", "key": "results"},
                        {
                            "source": "result",
                            "type": "equals_arg",
                            "key": "base_dir",
                            "arg": "base_dir",
                            "normalize": "lower",
                            "resolve_path": True,
                            "allow_missing_arg": True,
                        },
                        {"source": "confirm", "type": "key_exists", "key": "items"},
                    ],
                },
            )
        elif action == "search_text":
            template = self._merge_verify_rules(
                template,
                {
                    "confirm": {
                        "action": "list_folder",
                        "args": {"path": "{{result.base_dir}}"},
                        "required": False,
                        "attempts": 2,
                        "delay_s": 0.2,
                    },
                    "checks": [
                        {"source": "result", "type": "number_gte", "key": "count", "value": 0},
                        {"source": "result", "type": "key_exists", "key": "results"},
                        {
                            "source": "result",
                            "type": "equals_arg",
                            "key": "base_dir",
                            "arg": "base_dir",
                            "normalize": "lower",
                            "resolve_path": True,
                            "allow_missing_arg": True,
                        },
                        {"source": "confirm", "type": "key_exists", "key": "items"},
                    ],
                },
            )
        elif action == "scan_directory":
            template = self._merge_verify_rules(
                template,
                {
                    "confirm": {
                        "action": "list_folder",
                        "args": {"path": "{{result.path}}"},
                        "required": False,
                        "attempts": 2,
                        "delay_s": 0.2,
                    },
                    "checks": [
                        {"source": "result", "type": "number_gte", "key": "count", "value": 0},
                        {"source": "result", "type": "key_exists", "key": "results"},
                        {
                            "source": "result",
                            "type": "equals_arg",
                            "key": "path",
                            "arg": "path",
                            "normalize": "lower",
                            "resolve_path": True,
                            "allow_missing_arg": True,
                        },
                        {"source": "confirm", "type": "key_exists", "key": "items"},
                    ],
                },
            )
        elif action == "hash_file":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "regex", "key": "hash", "pattern": "^[0-9a-fA-F]{32,128}$"},
                        {
                            "source": "result",
                            "type": "equals_arg",
                            "key": "path",
                            "arg": "path",
                            "normalize": "lower",
                            "resolve_path": True,
                        },
                    ]
                },
            )
        elif action == "backup_file":
            template = self._merge_verify_rules(
                template,
                {
                    "confirm": {
                        "action": "hash_file",
                        "args": {"path": "{{result.backup_path}}", "algo": "sha256"},
                        "required": True,
                        "attempts": 2,
                        "delay_s": 0.2,
                    },
                    "checks": [{"source": "confirm", "type": "key_exists", "key": "hash"}],
                },
            )
        elif action == "copy_file":
            template = self._merge_verify_rules(
                template,
                {
                    "confirm": {
                        "action": "hash_file",
                        "args": {"path": "{{args.destination}}", "algo": "sha256"},
                        "required": True,
                        "attempts": 2,
                        "delay_s": 0.2,
                    },
                    "checks": [
                        {"source": "confirm", "type": "key_exists", "key": "hash"},
                        {
                            "source": "confirm",
                            "type": "equals_arg",
                            "key": "path",
                            "arg": "destination",
                            "normalize": "lower",
                            "resolve_path": True,
                        },
                    ],
                },
            )
        elif action == "list_folder":
            template = self._merge_verify_rules(
                template,
                {
                    "confirm": {
                        "action": "folder_size",
                        "args": {"path": "{{result.path}}"},
                        "required": False,
                        "attempts": 2,
                        "delay_s": 0.2,
                    },
                    "checks": [
                        {"source": "result", "type": "key_exists", "key": "items"},
                        {"source": "result", "type": "number_gte", "key": "count", "value": 0},
                        {
                            "source": "result",
                            "type": "equals_arg",
                            "key": "path",
                            "arg": "path",
                            "normalize": "lower",
                            "resolve_path": True,
                            "allow_missing_arg": True,
                        },
                        {"source": "confirm", "type": "number_gte", "key": "size_bytes", "value": 0},
                    ],
                },
            )
        elif action == "explorer_open_path":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "contains", "key": "adapter", "value": "explorer"},
                        {
                            "source": "result",
                            "type": "equals_arg",
                            "key": "path",
                            "arg": "path",
                            "normalize": "lower",
                            "resolve_path": True,
                            "allow_missing_arg": True,
                        },
                    ]
                },
            )
        elif action == "explorer_select_file":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "contains", "key": "adapter", "value": "explorer"},
                        {"source": "result", "type": "key_exists", "key": "path"},
                    ]
                },
            )
        elif action == "list_files":
            template = self._merge_verify_rules(
                template,
                {
                    "confirm": {
                        "action": "folder_size",
                        "args": {"path": "{{result.path}}"},
                        "required": False,
                        "attempts": 2,
                        "delay_s": 0.2,
                    },
                    "checks": [
                        {"source": "result", "type": "key_exists", "key": "items"},
                        {"source": "result", "type": "number_gte", "key": "count", "value": 0},
                        {
                            "source": "result",
                            "type": "equals_arg",
                            "key": "path",
                            "arg": "path",
                            "normalize": "lower",
                            "resolve_path": True,
                            "allow_missing_arg": True,
                        },
                        {"source": "confirm", "type": "number_gte", "key": "size_bytes", "value": 0},
                    ],
                },
            )
        elif action == "create_folder":
            template = self._merge_verify_rules(
                template,
                {
                    "confirm": {
                        "action": "folder_size",
                        "args": {"path": "{{args.path}}"},
                        "required": True,
                        "attempts": 2,
                        "delay_s": 0.25,
                    },
                    "checks": [
                        {
                            "source": "result",
                            "type": "equals_arg",
                            "key": "created",
                            "arg": "path",
                            "normalize": "lower",
                            "resolve_path": True,
                        },
                        {"source": "confirm", "type": "number_gte", "key": "size_bytes", "value": 0},
                    ],
                },
            )
        elif action == "folder_size":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "number_gte", "key": "size_bytes", "value": 0},
                        {
                            "source": "result",
                            "type": "equals_arg",
                            "key": "path",
                            "arg": "path",
                            "normalize": "lower",
                            "resolve_path": True,
                        },
                    ]
                },
            )
        elif action == "read_file":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {
                            "source": "result",
                            "type": "equals_arg",
                            "key": "path",
                            "arg": "path",
                            "normalize": "lower",
                            "resolve_path": True,
                        }
                    ]
                },
            )
        elif action == "write_file":
            template = self._merge_verify_rules(
                template,
                {
                    "confirm": {
                        "action": "read_file",
                        "args": {"path": "{{args.path}}", "max_chars": 350000},
                        "required": True,
                        "attempts": 2,
                        "delay_s": 0.25,
                    },
                    "checks": [
                        {"source": "result", "type": "number_gte", "key": "bytes", "value": 0},
                        {
                            "source": "confirm",
                            "type": "equals_arg",
                            "key": "path",
                            "arg": "path",
                            "normalize": "lower",
                            "resolve_path": True,
                        },
                    ],
                },
            )
            if isinstance(file_content, str) and 0 < len(file_content) <= 4096:
                template = self._merge_verify_rules(
                    template,
                    {"checks": [{"source": "confirm", "type": "contains_arg", "key": "content", "arg": "content"}]},
                )
        elif action == "clipboard_read":
            template = self._merge_verify_rules(
                template,
                {"checks": [{"source": "result", "type": "number_gte", "key": "chars", "value": 0}]},
            )
        elif action == "clipboard_write":
            template = self._merge_verify_rules(
                template,
                {
                    "confirm": {
                        "action": "clipboard_read",
                        "args": {},
                        "required": True,
                        "attempts": 2,
                        "delay_s": 0.15,
                    },
                    "checks": [
                        {"source": "result", "type": "number_gte", "key": "chars", "value": 0},
                        {
                            "source": "confirm",
                            "type": "contains_arg",
                            "key": "text",
                            "arg": "text",
                            "normalize": "lower",
                            "allow_missing_arg": True,
                        },
                    ],
                },
            )
        elif action == "keyboard_type":
            template = self._merge_verify_rules(
                template,
                {"checks": [{"source": "result", "type": "number_gte", "key": "chars", "value": 1}]},
            )
        elif action == "keyboard_hotkey":
            template = self._merge_verify_rules(
                template,
                {"checks": [{"source": "result", "type": "key_exists", "key": "keys"}]},
            )
        elif action == "mouse_move":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "number_gte", "key": "x", "value": 0},
                        {"source": "result", "type": "number_gte", "key": "y", "value": 0},
                    ]
                },
            )
        elif action == "mouse_click":
            template = self._merge_verify_rules(
                template,
                {"checks": [{"source": "result", "type": "number_gte", "key": "clicks", "value": 1}]},
            )
        elif action == "mouse_scroll":
            template = self._merge_verify_rules(
                template,
                {"checks": [{"source": "result", "type": "key_exists", "key": "amount"}]},
            )
        elif action == "screenshot_capture":
            template = self._merge_verify_rules(
                template,
                {"checks": [{"source": "result", "type": "contains_arg", "key": "path", "arg": "path", "allow_missing_arg": True}]},
            )
        elif action == "browser_read_dom":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "contains_arg", "key": "url", "arg": "url", "normalize": "lower"},
                        {"source": "result", "type": "number_gte", "key": "chars", "value": 0},
                    ]
                },
            )
        elif action == "browser_extract_links":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "contains_arg", "key": "url", "arg": "url", "normalize": "lower"},
                        {"source": "result", "type": "number_gte", "key": "count", "value": 0},
                    ]
                },
            )
        elif action == "browser_session_create":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "key_exists", "key": "session"},
                        {"source": "result", "type": "contains_arg", "key": "session.base_url", "arg": "base_url", "allow_missing_arg": True},
                    ]
                },
            )
        elif action == "browser_session_close":
            template = self._merge_verify_rules(
                template,
                {"checks": [{"source": "result", "type": "key_exists", "key": "session"}]},
            )
        elif action == "browser_session_request":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "contains_arg", "key": "session_id", "arg": "session_id"},
                        {"source": "result", "type": "contains_arg", "key": "request.url", "arg": "url", "normalize": "lower"},
                        {"source": "result", "type": "number_gte", "key": "response.status_code", "value": 100},
                    ]
                },
            )
        elif action == "browser_session_read_dom":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "contains_arg", "key": "session_id", "arg": "session_id"},
                        {"source": "result", "type": "contains_arg", "key": "url", "arg": "url", "normalize": "lower"},
                        {"source": "result", "type": "number_gte", "key": "chars", "value": 0},
                    ]
                },
            )
        elif action == "browser_session_extract_links":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "contains_arg", "key": "session_id", "arg": "session_id"},
                        {"source": "result", "type": "contains_arg", "key": "url", "arg": "url", "normalize": "lower"},
                        {"source": "result", "type": "number_gte", "key": "count", "value": 0},
                    ]
                },
            )
        elif action == "computer_observe":
            template = self._merge_verify_rules(
                template,
                {"checks": [{"source": "result", "type": "key_exists", "key": "screenshot_path"}]},
            )
        elif action == "computer_assert_text_visible":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "equals_arg", "key": "text", "arg": "text", "normalize": "lower"},
                        {"source": "result", "type": "key_exists", "key": "found"},
                    ]
                },
            )
        elif action == "computer_find_text_targets":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "number_gte", "key": "count", "value": 0},
                        {"source": "result", "type": "equals_arg", "key": "query", "arg": "query", "normalize": "lower"},
                    ]
                },
            )
        elif action == "computer_wait_for_text":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "equals_arg", "key": "text", "arg": "text", "normalize": "lower"},
                        {"source": "result", "type": "key_exists", "key": "found"},
                    ]
                },
            )
        elif action == "computer_click_text":
            template = self._merge_verify_rules(
                template,
                {
                    "confirm": {
                        "action": "computer_find_text_targets",
                        "args": {"query": "{{args.query}}", "limit": 5},
                        "required": False,
                        "attempts": 2,
                        "delay_s": 0.2,
                    },
                    "checks": [
                        {"source": "result", "type": "number_gte", "key": "x", "value": 0},
                        {"source": "result", "type": "number_gte", "key": "y", "value": 0},
                        {"source": "result", "type": "equals_arg", "key": "query", "arg": "query", "normalize": "lower"},
                        {"source": "confirm", "type": "number_gte", "key": "count", "value": 0},
                        {
                            "type": "any_of",
                            "checks": [
                                {"source": "result", "type": "equals", "key": "screen_changed", "value": True},
                                {"source": "desktop_state", "type": "desktop_state_changed"},
                                {"source": "desktop_state", "type": "changed_path_contains", "value": "input.mouse"},
                            ],
                        },
                    ],
                },
            )
        elif action == "computer_click_target":
            template = self._merge_verify_rules(
                template,
                {
                    "confirm": {
                        "action": "computer_find_text_targets",
                        "args": {"query": "{{args.query}}", "limit": 5},
                        "required": False,
                        "attempts": 2,
                        "delay_s": 0.2,
                    },
                    "checks": [
                        {"source": "result", "type": "equals_arg", "key": "query", "arg": "query", "normalize": "lower"},
                        {"source": "result", "type": "in", "key": "method", "values": ["accessibility", "ocr_text"]},
                        {"source": "confirm", "type": "number_gte", "key": "count", "value": 0},
                        {
                            "type": "any_of",
                            "checks": [
                                {"source": "result", "type": "equals", "key": "screen_changed", "value": True},
                                {"source": "desktop_state", "type": "desktop_state_changed"},
                                {"source": "desktop_state", "type": "changed_path_contains", "value": "visual"},
                            ],
                        },
                    ],
                },
            )
        elif action == "extract_text_from_image":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "number_gte", "key": "chars", "value": 0},
                        {
                            "source": "result",
                            "type": "equals_arg",
                            "key": "path",
                            "arg": "path",
                            "normalize": "lower",
                            "resolve_path": True,
                        },
                    ]
                },
            )
        elif action == "run_whitelisted_app":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "number_gte", "key": "pid", "value": 1},
                        {
                            "source": "result",
                            "type": "contains_arg",
                            "key": "app_name",
                            "arg": "app_name",
                            "normalize": "lower",
                            "strip_exe": True,
                        },
                    ]
                },
            )
        elif action == "run_trusted_script":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "number_gte", "key": "pid", "value": 1},
                        {
                            "source": "result",
                            "type": "contains_arg",
                            "key": "script_name",
                            "arg": "script_name",
                            "normalize": "lower",
                        },
                    ]
                },
            )
        elif action == "external_connector_status":
            template = self._merge_verify_rules(
                template,
                {"checks": [{"source": "result", "type": "key_exists", "key": "providers"}]},
            )
        elif action == "external_connector_preflight":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "key_exists", "key": "contract_diagnostic"},
                        {"source": "result", "type": "equals_arg", "key": "action", "arg": "action"},
                    ],
                    "confirm": {
                        "action": "external_connector_status",
                        "args": {},
                        "required": False,
                        "attempts": 1,
                        "delay_s": 0.1,
                    },
                },
            )
        elif action == "external_email_send":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "key_exists", "key": "provider"},
                        {"source": "result", "type": "key_exists", "key": "to"},
                    ]
                },
            )
        elif action == "external_email_list":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "key_exists", "key": "items"},
                        {"source": "result", "type": "number_gte", "key": "count", "value": 0},
                    ]
                },
            )
        elif action == "external_email_read":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "key_exists", "key": "message_id"},
                        {"source": "result", "type": "equals_arg", "key": "message_id", "arg": "message_id"},
                    ]
                },
            )
        elif action == "external_calendar_create_event":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "key_exists", "key": "provider"},
                        {"source": "result", "type": "key_exists", "key": "title"},
                    ]
                },
            )
        elif action == "external_calendar_list_events":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "key_exists", "key": "items"},
                        {"source": "result", "type": "number_gte", "key": "count", "value": 0},
                    ]
                },
            )
        elif action == "external_calendar_update_event":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "key_exists", "key": "event_id"},
                        {"source": "result", "type": "equals_arg", "key": "event_id", "arg": "event_id"},
                    ]
                },
            )
        elif action == "external_doc_create":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "key_exists", "key": "provider"},
                        {"source": "result", "type": "in", "key": "status", "values": ["success"]},
                    ]
                },
            )
        elif action == "external_doc_list":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "key_exists", "key": "items"},
                        {"source": "result", "type": "number_gte", "key": "count", "value": 0},
                    ]
                },
            )
        elif action == "external_doc_read":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "key_exists", "key": "document_id"},
                        {"source": "result", "type": "equals_arg", "key": "document_id", "arg": "document_id"},
                    ]
                },
            )
        elif action == "external_doc_update":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "key_exists", "key": "document_id"},
                        {"source": "result", "type": "equals_arg", "key": "document_id", "arg": "document_id"},
                    ]
                },
            )
        elif action == "external_task_list":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "key_exists", "key": "items"},
                        {"source": "result", "type": "number_gte", "key": "count", "value": 0},
                    ]
                },
            )
        elif action == "external_task_create":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "key_exists", "key": "task_id"},
                        {"source": "result", "type": "key_exists", "key": "provider"},
                        {"source": "result", "type": "contains_arg", "key": "title", "arg": "title", "normalize": "lower"},
                    ]
                },
            )
        elif action == "external_task_update":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "key_exists", "key": "task_id"},
                        {"source": "result", "type": "equals_arg", "key": "task_id", "arg": "task_id"},
                    ]
                },
            )
        elif action == "accessibility_status":
            template = self._merge_verify_rules(
                template,
                {"checks": [{"source": "result", "type": "in", "key": "status", "values": ["success", "degraded"]}]},
            )
        elif action == "accessibility_list_elements":
            template = self._merge_verify_rules(
                template,
                {"checks": [{"source": "result", "type": "number_gte", "key": "count", "value": 0}]},
            )
        elif action == "accessibility_find_element":
            template = self._merge_verify_rules(
                template,
                {
                    "checks": [
                        {"source": "result", "type": "equals_arg", "key": "query", "arg": "query", "normalize": "lower"},
                        {"source": "result", "type": "number_gte", "key": "count", "value": 0},
                    ]
                },
            )
        elif action == "accessibility_invoke_element":
            template = self._merge_verify_rules(
                template,
                {
                    "confirm": {
                        "action": "accessibility_find_element",
                        "args": {"query": "{{args.query}}", "max_results": 3},
                        "required": False,
                        "attempts": 2,
                        "delay_s": 0.2,
                    },
                    "checks": [
                        {"source": "result", "type": "in", "key": "action", "values": ["click", "double_click", "right_click", "focus"]},
                        {"source": "result", "type": "key_exists", "key": "element"},
                    ],
                },
            )
        elif action == "time_now":
            template = self._merge_verify_rules(
                template,
                {"checks": [{"source": "result", "type": "regex", "key": "iso", "pattern": r"^\d{4}-\d{2}-\d{2}T"}]},
            )
            if str(args.get("timezone", "")).strip():
                template = self._merge_verify_rules(
                    template,
                    {"checks": [{"source": "result", "type": "equals_arg", "key": "timezone", "arg": "timezone"}]},
                )

        named_templates = self._verification_named_templates(action)
        if named_templates:
            template = self._merge_verify_rules(template, {"templates": named_templates})

        return template

    @staticmethod
    def _verification_named_templates(action: str) -> list[str]:
        mapping: Dict[str, list[str]] = {
            "read_file": ["filesystem.path_exists"],
            "write_file": ["filesystem.write_integrity"],
            "copy_file": ["filesystem.path_exists"],
            "backup_file": ["filesystem.path_exists"],
            "hash_file": ["filesystem.path_exists"],
            "browser_read_dom": ["browser.dom_fetch"],
            "computer_click_text": ["desktop.click_effect"],
            "computer_click_target": ["desktop.click_effect"],
            "accessibility_invoke_element": ["desktop.click_effect"],
            "terminate_process": ["process.termination"],
        }
        return list(mapping.get(action, []))

    def _merge_verify_rules(self, base: Dict[str, Any], extra: Dict[str, Any]) -> Dict[str, Any]:
        merged: Dict[str, Any] = {}
        for key, value in base.items():
            if isinstance(value, dict):
                merged[key] = self._merge_verify_rules({}, value)
            elif isinstance(value, list):
                merged[key] = list(value)
            else:
                merged[key] = value

        for key, value in extra.items():
            if key == "checks":
                existing = merged.get("checks")
                if isinstance(existing, list) and isinstance(value, list):
                    merged["checks"] = [*existing, *value]
                elif isinstance(value, list):
                    merged["checks"] = list(value)
                continue

            existing = merged.get(key)
            if isinstance(existing, dict) and isinstance(value, dict):
                merged[key] = self._merge_verify_rules(existing, value)
            elif isinstance(value, dict):
                merged[key] = self._merge_verify_rules({}, value)
            elif isinstance(value, list):
                merged[key] = list(value)
            else:
                merged[key] = value
        return merged

    def _apply_profile_verification_overrides(self, steps: List[PlanStep], profile_name: str) -> List[PlanStep]:
        profile = str(profile_name or "").strip().lower()
        if not profile:
            return steps

        overrides = self.PROFILE_VERIFY_OVERRIDES.get(profile, {})
        if not overrides:
            return steps

        global_rules = overrides.get("*", {})
        for step in steps:
            verify_rules = step.verify if isinstance(step.verify, dict) else {}
            merged = verify_rules
            if isinstance(global_rules, dict) and global_rules:
                merged = self._merge_verify_rules(merged, global_rules)
            action_rules = overrides.get(step.action, {})
            if isinstance(action_rules, dict) and action_rules:
                merged = self._merge_verify_rules(merged, action_rules)
            step.verify = merged
        return steps

    def _apply_circuit_breaker_overrides(
        self,
        steps: List[PlanStep],
        planning_context: Dict[str, object],
    ) -> Dict[str, Any]:
        raw_open = planning_context.get("open_action_circuits")
        raw_provider_health = planning_context.get("external_provider_health")
        if not isinstance(raw_open, list) and not isinstance(raw_provider_health, list):
            return {}

        open_global: Dict[str, float] = {}
        open_scoped: Dict[str, Dict[str, float]] = {}
        for row in raw_open if isinstance(raw_open, list) else []:
            if not isinstance(row, dict):
                continue
            action = str(row.get("action", "")).strip()
            if not action:
                continue
            retry_after_s = self._clamp_float(row.get("retry_after_s", 0.0), minimum=0.0, maximum=86400.0, default=0.0)
            scope = self._normalize_provider(str(row.get("scope", "")).strip())
            if scope:
                scoped_rows = open_scoped.setdefault(action, {})
                previous = self._clamp_float(scoped_rows.get(scope, 0.0), minimum=0.0, maximum=86400.0, default=0.0)
                scoped_rows[scope] = max(previous, retry_after_s)
            else:
                previous_global = self._clamp_float(open_global.get(action, 0.0), minimum=0.0, maximum=86400.0, default=0.0)
                open_global[action] = max(previous_global, retry_after_s)

        provider_health: Dict[str, Dict[str, Any]] = {}
        for row in raw_provider_health if isinstance(raw_provider_health, list) else []:
            if not isinstance(row, dict):
                continue
            provider = self._normalize_provider(str(row.get("provider", "")).strip())
            if not provider:
                continue
            provider_health[provider] = {
                "provider": provider,
                "cooldown_active": bool(row.get("cooldown_active", False)),
                "retry_after_s": self._clamp_float(row.get("retry_after_s", 0.0), minimum=0.0, maximum=86400.0, default=0.0),
                "health_score": self._clamp_float(row.get("health_score", 0.5), minimum=0.0, maximum=1.0, default=0.5),
                "failure_ema": self._clamp_float(row.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "failure_trend_ema": self._clamp_float(row.get("failure_trend_ema", 0.0), minimum=-1.0, maximum=1.0, default=0.0),
                "consecutive_failures": self._clamp_int(row.get("consecutive_failures", 0), minimum=0, maximum=100_000, default=0),
                "samples": self._clamp_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                "top_action_risks": list(row.get("top_action_risks", [])) if isinstance(row.get("top_action_risks"), list) else [],
                "top_operation_risks": list(row.get("top_operation_risks", []))
                if isinstance(row.get("top_operation_risks"), list)
                else [],
            }
        mission_trend = planning_context.get("mission_trend_feedback", {})
        external_trend = planning_context.get("external_reliability_trend", {})
        external_trend_payload = external_trend if isinstance(external_trend, dict) else {}
        external_trend_mode = str(external_trend_payload.get("mode", "")).strip().lower()
        external_trend_pressure = self._clamp_float(
            external_trend_payload.get("trend_pressure", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        external_mission_profile = str(external_trend_payload.get("mission_profile", "")).strip().lower()

        tuned_actions: list[str] = []
        provider_switches: list[Dict[str, str]] = []
        open_actions: list[str] = []
        for step in steps:
            action = str(step.action or "").strip()
            if not action:
                continue
            verify_payload = step.verify if isinstance(step.verify, dict) else {}
            if action in open_global:
                verify_payload = self._merge_verify_rules(
                    verify_payload,
                    {
                        "circuit_breaker": {
                            "open": True,
                            "retry_after_s": round(open_global[action], 3),
                        }
                    },
                )
                step.verify = verify_payload
                step.max_retries = min(step.max_retries, 1)
                step.timeout_s = max(step.timeout_s, min(160, int(round(step.timeout_s * 1.12))))
                tuned_actions.append(action)
                open_actions.append(action)

            if not (action.startswith("external_") or action.startswith("oauth_token_")):
                continue

            candidates = self._external_provider_candidates(action=action, args=step.args if isinstance(step.args, dict) else {})
            if not candidates:
                continue
            current_provider = self._normalize_provider(str(step.args.get("provider", "")).strip()) if isinstance(step.args, dict) else ""
            scoped_blocks = open_scoped.get(action, {})
            blocked: set[str] = set()
            for provider in candidates:
                if provider in scoped_blocks:
                    blocked.add(provider)
                    continue
                health = provider_health.get(provider, {})
                if isinstance(health, dict) and bool(health.get("cooldown_active", False)):
                    blocked.add(provider)

            available = [provider for provider in candidates if provider not in blocked]
            selected_provider = ""
            candidate_scores: Dict[str, float] = {}
            if available:
                for provider in available:
                    candidate_scores[provider] = self._provider_health_penalty(
                        provider=provider,
                        health=provider_health.get(provider, {}),
                        preferred=current_provider,
                        action=action,
                        mission_trend=mission_trend,
                        external_trend=external_trend_payload,
                    )
                selected_provider = min(
                    available,
                    key=lambda provider: candidate_scores.get(provider, 1000.0),
                )

            if selected_provider and selected_provider != current_provider:
                step.args["provider"] = selected_provider
                provider_switches.append({"action": action, "from": current_provider, "to": selected_provider})
                tuned_actions.append(action)

            selection_patch: Dict[str, Any] = {}
            if candidate_scores and (blocked or (selected_provider and selected_provider != current_provider)):
                selection_patch = {
                    "provider_selection": {
                        "candidates": candidates,
                        "blocked": sorted(blocked),
                        "selected": selected_provider or current_provider or "",
                        "operation_class": self._external_operation_class(action),
                        "scores": {key: round(float(value), 6) for key, value in candidate_scores.items()},
                    }
                }

            if blocked:
                if not selection_patch:
                    selection_patch = {
                        "provider_selection": {
                            "candidates": candidates,
                            "blocked": sorted(blocked),
                            "selected": selected_provider or current_provider or "",
                            "operation_class": self._external_operation_class(action),
                            "scores": {},
                        }
                    }
                verify_patch: Dict[str, Any] = {
                    "external_preflight": {
                        "required": True,
                        "provider_cooldown_check": True,
                    },
                }
                verify_patch = self._merge_verify_rules(verify_patch, selection_patch)
                verify_payload = self._merge_verify_rules(verify_payload, verify_patch)
                if not available:
                    step.max_retries = min(step.max_retries, 1)
                    timeout_boost = max(step.timeout_s, min(180, int(round(step.timeout_s * 1.25))))
                    step.timeout_s = timeout_boost
                    open_actions.append(action)
                step.verify = verify_payload
                tuned_actions.append(action)
            elif selection_patch:
                step.verify = self._merge_verify_rules(verify_payload, selection_patch)

            operation_class = self._external_operation_class(action)
            if external_trend_mode == "worsening" and external_trend_pressure >= 0.62:
                trend_patch = {
                    "external_preflight": {
                        "required": True,
                        "provider_cooldown_check": True,
                    },
                    "external_trend": {
                        "mode": external_trend_mode,
                        "trend_pressure": round(external_trend_pressure, 6),
                        "mission_profile": external_mission_profile,
                    },
                }
                verify_payload = step.verify if isinstance(step.verify, dict) else {}
                step.verify = self._merge_verify_rules(verify_payload, trend_patch)
                if operation_class in {"write", "mutate", "auth"}:
                    step.max_retries = min(step.max_retries, 2)
                    step.timeout_s = max(step.timeout_s, min(190, int(round(step.timeout_s * 1.2))))
                elif external_trend_pressure >= 0.82:
                    step.max_retries = min(step.max_retries, 2)
                tuned_actions.append(action)

        if not tuned_actions and not provider_switches and not open_actions:
            return {}
        return {
            "tuned_steps": len(sorted(set(tuned_actions))),
            "tuned_actions": sorted(set(tuned_actions)),
            "open_actions": sorted(set(open_actions)),
            "provider_switches": provider_switches[:20],
        }

    @staticmethod
    def _provider_health_penalty(
        *,
        provider: str,
        health: Any,
        preferred: str = "",
        action: str = "",
        mission_trend: Any = None,
        external_trend: Any = None,
    ) -> float:
        payload = health if isinstance(health, dict) else {}
        health_score = float(payload.get("health_score", 0.5) or 0.5)
        health_score = max(0.0, min(1.0, health_score))
        failure_ema = float(payload.get("failure_ema", 0.0) or 0.0)
        failure_ema = max(0.0, min(1.0, failure_ema))
        failure_trend = float(payload.get("failure_trend_ema", 0.0) or 0.0)
        failure_trend = max(-1.0, min(1.0, failure_trend))
        cooldown = bool(payload.get("cooldown_active", False))
        consecutive_failures = int(payload.get("consecutive_failures", 0) or 0)
        consecutive_penalty = min(0.45, max(0.0, float(consecutive_failures)) * 0.08)
        penalty = failure_ema + consecutive_penalty + ((1.0 - health_score) * 0.55)
        if failure_trend > 0.0:
            penalty += min(0.35, failure_trend * 0.32)
        elif failure_trend < 0.0:
            penalty -= min(0.12, abs(failure_trend) * 0.08)
        operation_class = Planner._external_operation_class(action)
        action_rows = payload.get("top_action_risks", [])
        operation_rows = payload.get("top_operation_risks", [])
        penalty += Planner._risk_row_penalty(rows=action_rows, target_key="action", target_value=str(action or "").strip().lower())
        penalty += Planner._risk_row_penalty(rows=operation_rows, target_key="operation", target_value=operation_class)
        trend_payload = mission_trend if isinstance(mission_trend, dict) else {}
        trend_mode = str(trend_payload.get("mode", "")).strip().lower()
        trend_pressure = max(0.0, min(1.0, float(trend_payload.get("trend_pressure", 0.0) or 0.0)))
        if trend_mode == "worsening" and trend_pressure > 0.0:
            risk_exposure = (failure_ema * 0.52) + ((1.0 - health_score) * 0.48) + max(0.0, failure_trend) * 0.3
            penalty += trend_pressure * (0.08 + (risk_exposure * 0.28))
        elif trend_mode == "improving" and trend_pressure > 0.0:
            relief = trend_pressure * (0.02 + ((1.0 - failure_ema) * 0.05))
            penalty -= min(0.08, relief)
        external_payload = external_trend if isinstance(external_trend, dict) else {}
        external_mode = str(external_payload.get("mode", "")).strip().lower()
        try:
            external_pressure = float(external_payload.get("trend_pressure", 0.0) or 0.0)
        except Exception:
            external_pressure = 0.0
        external_pressure = max(0.0, min(1.0, external_pressure))
        top_provider_risks = external_payload.get("top_provider_risks", [])
        provider_risk_score = 0.0
        provider_cooldown = False
        provider_outage = False
        if isinstance(top_provider_risks, list):
            for row in top_provider_risks:
                if not isinstance(row, dict):
                    continue
                row_provider = str(row.get("provider", "")).strip().lower()
                if row_provider != provider:
                    continue
                try:
                    risk_raw = float(row.get("risk_score", 0.0) or 0.0)
                except Exception:
                    risk_raw = 0.0
                provider_risk_score = max(
                    0.0,
                    min(1.0, risk_raw),
                )
                provider_cooldown = bool(row.get("cooldown_active", False))
                provider_outage = bool(row.get("outage_active", False))
                break
        if external_mode == "worsening" and external_pressure > 0.0:
            penalty += external_pressure * (
                0.04 + (provider_risk_score * 0.42) + (0.08 if provider_cooldown else 0.0) + (0.1 if provider_outage else 0.0)
            )
        elif external_mode == "improving" and external_pressure > 0.0:
            relief = external_pressure * (0.02 + ((1.0 - provider_risk_score) * 0.08))
            penalty -= min(0.1, relief)
        mission_profile = str(external_payload.get("mission_profile", "")).strip().lower()
        if mission_profile in {"defensive", "cautious"} and provider_risk_score >= 0.6:
            penalty += min(0.2, provider_risk_score * 0.2)
        if cooldown:
            penalty += 2.0
        if preferred and provider == preferred:
            penalty -= 0.04
        return max(0.0, penalty)

    @classmethod
    def _external_provider_candidates(cls, *, action: str, args: Dict[str, Any]) -> List[str]:
        current = cls._normalize_provider(str(args.get("provider", "")).strip())
        clean_action = str(action or "").strip().lower()
        rule = cls.EXTERNAL_PROVIDER_RULES.get(clean_action, {})
        allowed_raw = rule.get("allow", []) if isinstance(rule, dict) else []
        allowed = [cls._normalize_provider(str(item)) for item in allowed_raw if cls._normalize_provider(str(item))]
        if current and current != "auto":
            if allowed and current not in allowed:
                return list(dict.fromkeys(allowed))
            return [current]
        if allowed:
            return list(dict.fromkeys(allowed))
        if clean_action in {"external_email_send", "external_email_list", "external_email_read"}:
            return ["google", "graph", "smtp"]
        if clean_action.startswith("external_") or clean_action.startswith("oauth_token_"):
            return ["google", "graph"]
        return []

    @classmethod
    def _external_operation_class(cls, action: str) -> str:
        clean_action = str(action or "").strip().lower()
        if not clean_action:
            return "default"
        return str(cls.EXTERNAL_OPERATION_CLASS.get(clean_action, "default")).strip().lower() or "default"

    @staticmethod
    def _risk_row_penalty(*, rows: Any, target_key: str, target_value: str) -> float:
        if not isinstance(rows, list) or not target_value:
            return 0.0
        for row in rows:
            if not isinstance(row, dict):
                continue
            name = str(row.get(target_key, "")).strip().lower()
            if name != target_value:
                continue
            failure_ema = max(0.0, min(1.0, float(row.get("failure_ema", 0.0) or 0.0)))
            failure_trend = max(-1.0, min(1.0, float(row.get("failure_trend_ema", 0.0) or 0.0)))
            consecutive = max(0, int(row.get("consecutive_failures", 0) or 0))
            penalty = (failure_ema * 0.45) + min(0.35, float(consecutive) * 0.04)
            if failure_trend > 0.0:
                penalty += min(0.22, failure_trend * 0.2)
            elif failure_trend < 0.0:
                penalty -= min(0.08, abs(failure_trend) * 0.06)
            return max(0.0, penalty)
        return 0.0

    @classmethod
    def _normalize_provider(cls, raw: str) -> str:
        text = str(raw or "").strip().lower()
        if not text:
            return ""
        if text in {"google", "gmail", "google_docs", "google_tasks", "google_calendar"}:
            return "google"
        if text in {
            "graph",
            "microsoft",
            "microsoft_graph",
            "microsoft_graph_mail",
            "microsoft_graph_todo",
            "microsoft_graph_calendar",
            "microsoft_graph_drive",
        }:
            return "graph"
        if text in {"smtp"}:
            return "smtp"
        if text == "auto":
            return "auto"
        return text

    def _apply_action_guardrail_overrides(
        self,
        steps: List[PlanStep],
        planning_context: Dict[str, object],
    ) -> Dict[str, Any]:
        raw_items = planning_context.get("action_guardrails")
        if not isinstance(raw_items, list) or not raw_items:
            return {}

        guardrail_by_action: Dict[str, Dict[str, Any]] = {}
        for row in raw_items:
            if not isinstance(row, dict):
                continue
            action = str(row.get("action", "")).strip()
            if not action:
                continue
            guardrail_by_action[action] = row
        if not guardrail_by_action:
            return {}

        tuned_actions: list[str] = []
        strict_tuned_actions: list[str] = []
        total_step_tunes = 0
        for step in steps:
            state = guardrail_by_action.get(step.action)
            if not isinstance(state, dict):
                continue

            try:
                unstable_score = float(state.get("unstable_score", 0.0) or 0.0)
            except Exception:
                unstable_score = 0.0
            unstable_score = max(0.0, min(1.0, unstable_score))
            try:
                reliability_score = float(state.get("reliability_score", 0.0) or 0.0)
            except Exception:
                reliability_score = 0.0
            reliability_score = max(0.0, min(1.0, reliability_score))
            risk_level = str(state.get("risk_level", "")).strip().lower()
            try:
                samples = int(state.get("samples", 0) or 0)
            except Exception:
                samples = 0

            if samples <= 0:
                continue

            verify_payload = step.verify if isinstance(step.verify, dict) else {}
            tuned = False
            if unstable_score >= 0.7 or (unstable_score >= 0.58 and risk_level in {"high", "critical"}):
                strict_tuned_actions.append(step.action)
                strict_guardrail_payload: Dict[str, Any] = {
                    "expect_result_status": "success",
                    "confirm_policy": {"mode": "all", "required": True},
                    "guardrail": {
                        "level": "strict",
                        "unstable_score": round(unstable_score, 4),
                        "reliability_score": round(reliability_score, 4),
                        "samples": samples,
                    },
                }
                if step.action in {"computer_click_target", "computer_click_text", "accessibility_invoke_element"}:
                    strict_guardrail_payload["desktop_anchor"] = {
                        "enabled": True,
                        "required": True,
                        "action": "accessibility_find_element",
                        "query": "{{args.query}}",
                        "timeout_s": 12,
                    }
                    strict_guardrail_payload["fallback_chain"] = [
                        {"action": "computer_observe", "args": {"ocr": False}, "timeout_s": 8},
                        {"action": "accessibility_find_element", "args": {"query": "{{args.query}}", "max_results": 4}, "timeout_s": 10},
                        {"action": "computer_find_text_targets", "args": {"query": "{{args.query}}", "max_results": 6}, "timeout_s": 12},
                    ]
                if step.action.startswith("external_"):
                    strict_guardrail_payload["external_preflight"] = {
                        "required": True,
                        "contract_enforced": True,
                        "provider_cooldown_check": True,
                    }
                verify_payload = self._merge_verify_rules(
                    verify_payload,
                    strict_guardrail_payload,
                )
                step.max_retries = min(step.max_retries, 1)
                step.timeout_s = max(step.timeout_s, min(180, int(round(step.timeout_s * 1.2))))
                tuned = True
            elif unstable_score >= 0.45:
                warning_guardrail_payload: Dict[str, Any] = {
                    "expect_result_status": "success",
                    "guardrail": {
                        "level": "warning",
                        "unstable_score": round(unstable_score, 4),
                        "reliability_score": round(reliability_score, 4),
                        "samples": samples,
                    },
                }
                if step.action in {"computer_click_target", "computer_click_text"}:
                    warning_guardrail_payload["desktop_anchor"] = {
                        "enabled": True,
                        "required": False,
                        "action": "accessibility_find_element",
                        "query": "{{args.query}}",
                        "timeout_s": 10,
                    }
                if step.action.startswith("external_"):
                    warning_guardrail_payload["external_preflight"] = {
                        "required": False,
                        "contract_enforced": True,
                        "provider_cooldown_check": True,
                    }
                verify_payload = self._merge_verify_rules(
                    verify_payload,
                    warning_guardrail_payload,
                )
                step.timeout_s = max(step.timeout_s, min(150, int(round(step.timeout_s * 1.1))))
                tuned = True

            if tuned:
                step.verify = verify_payload
                tuned_actions.append(step.action)
                total_step_tunes += 1

        if total_step_tunes <= 0:
            return {}
        return {
            "tuned_steps": total_step_tunes,
            "tuned_actions": sorted(set(tuned_actions)),
            "strict_tuned_actions": sorted(set(strict_tuned_actions)),
        }

    def _apply_episodic_strategy_overrides(
        self,
        steps: List[PlanStep],
        strategy_payload: Any,
    ) -> Dict[str, Any]:
        if not steps or not isinstance(strategy_payload, dict):
            return {}

        recommended_scores = self._strategy_action_scores(strategy_payload.get("recommended_actions"))
        avoid_scores = self._strategy_action_scores(strategy_payload.get("avoid_actions"))
        if not recommended_scores and not avoid_scores:
            return {}

        recommended_applied: list[str] = []
        avoid_applied: list[str] = []
        for step in steps:
            action = str(step.action or "").strip()
            if not action:
                continue

            rec_score = float(recommended_scores.get(action, 0.0))
            avoid_score = float(avoid_scores.get(action, 0.0))
            verify_rules = step.verify if isinstance(step.verify, dict) else {}

            if rec_score > 0.0:
                recommended_applied.append(action)
                rec_attempts = 3 if rec_score >= 1.1 else 2
                verify_rules = self._merge_verify_rules(
                    verify_rules,
                    {
                        "expect_result_status": "success",
                        "confirm": {
                            "required": False,
                            "attempts": rec_attempts,
                            "delay_s": 0.2,
                        },
                    },
                )
                if avoid_score <= 0.0:
                    step.max_retries = max(step.max_retries, 2)

            if avoid_score > 0.0:
                avoid_applied.append(action)
                avoid_attempts = 4 if avoid_score >= 1.1 else 3
                verify_rules = self._merge_verify_rules(
                    verify_rules,
                    {
                        "expect_result_status": "success",
                        "confirm": {
                            "required": True,
                            "attempts": avoid_attempts,
                            "delay_s": 0.3,
                        },
                        "strategy": {
                            "memory_avoid_action": True,
                            "avoid_score": round(avoid_score, 3),
                        },
                    },
                )
                step.max_retries = min(step.max_retries, 1)

            step.verify = verify_rules

        recommended_unique = sorted(set(recommended_applied))
        avoid_unique = sorted(set(avoid_applied))
        if not recommended_unique and not avoid_unique:
            return {}
        return {
            "recommended_actions": recommended_unique,
            "avoid_actions": avoid_unique,
            "recommended_count": len(recommended_unique),
            "avoid_count": len(avoid_unique),
        }

    @staticmethod
    def _strategy_action_scores(raw_items: Any) -> Dict[str, float]:
        if not isinstance(raw_items, list):
            return {}
        out: Dict[str, float] = {}
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            action = str(item.get("action", "")).strip()
            if not action:
                continue
            try:
                support = float(item.get("support", 0.0) or 0.0)
            except Exception:
                support = 0.0
            try:
                success = float(item.get("success_rate", 0.0) or 0.0)
            except Exception:
                success = 0.0
            try:
                failure = float(item.get("failure_rate", 0.0) or 0.0)
            except Exception:
                failure = 0.0
            score = max(0.0, support) + max(0.0, success - failure)
            previous = float(out.get(action, 0.0))
            if score > previous:
                out[action] = score
        return out

    def _filter_steps_by_allowed_actions(
        self,
        steps: List[PlanStep],
        allowed_actions: Set[str],
    ) -> Tuple[List[PlanStep], List[str]]:
        if not steps:
            return (steps, [])
        if not allowed_actions:
            return ([], [step.action for step in steps])

        filtered_steps: List[PlanStep] = []
        blocked_actions: List[str] = []
        for step in steps:
            if step.action in allowed_actions:
                filtered_steps.append(step)
            else:
                blocked_actions.append(step.action)

        if not filtered_steps:
            return ([], sorted(set(blocked_actions)))

        kept_ids = {step.step_id for step in filtered_steps}
        for step in filtered_steps:
            step.depends_on = [dep for dep in step.depends_on if dep in kept_ids]

        return (filtered_steps, sorted(set(blocked_actions)))

    def _map_reasoning_intent_to_step(self, *, intent: str, arguments: Dict[str, Any], original_text: str) -> Optional[PlanStep]:
        if intent in {"open_application", "open_app"}:
            app_name = str(arguments.get("app") or arguments.get("app_name") or self._extract_app_name(original_text))
            return self._step("open_app", args={"app_name": app_name}, verify={"expect_status": "success"})

        if intent in {"search_media", "media_search"}:
            query = str(arguments.get("query") or self._extract_media_query(original_text))
            return self._step("media_search", args={"query": query}, verify={"expect_status": "success", "expect_key": "url"})

        if intent in {"check_security", "defender_status"}:
            return self._step("defender_status", args={}, verify={"expect_status": "success"})

        if intent in {"time", "time_query"}:
            timezone = str(arguments.get("timezone") or "UTC")
            return self._step("time_now", args={"timezone": timezone}, verify={"expect_status": "success", "expect_key": "iso"})

        if intent in {"system_snapshot", "monitor"}:
            return self._step("system_snapshot", args={}, verify={"expect_status": "success", "expect_key": "metrics"})

        if intent in {"speak", "respond"}:
            text = str(arguments.get("text") or "I received your request.")
            return self._step("tts_speak", args={"text": text}, verify={"optional": True}, can_retry=False)

        if intent in {"screenshot", "capture_screen"}:
            path = str(arguments.get("path") or str(Path.home() / "Pictures" / "jarvis_capture.png"))
            return self._step("screenshot_capture", args={"path": path}, verify={"expect_status": "success", "expect_key": "path"})

        if intent in {"clipboard_read", "read_clipboard"}:
            return self._step("clipboard_read", args={}, verify={"expect_status": "success", "expect_key": "text"})

        if intent in {"read_webpage", "browser_read_dom", "summarize_page"}:
            url = str(arguments.get("url") or self._extract_url(original_text) or self._extract_domain_like(original_text))
            if url:
                return self._step("browser_read_dom", args={"url": url}, verify={"expect_status": "success"})

        if intent in {"extract_links", "browser_extract_links"}:
            url = str(arguments.get("url") or self._extract_url(original_text) or self._extract_domain_like(original_text))
            if url:
                return self._step("browser_extract_links", args={"url": url}, verify={"expect_status": "success"})

        if intent in {"browser_session_create", "create_browser_session"}:
            url = str(arguments.get("base_url") or self._extract_url(original_text) or self._extract_domain_like(original_text))
            args: Dict[str, Any] = {"name": str(arguments.get("name") or "jarvis-session")}
            if url:
                args["base_url"] = url
            provider = str(arguments.get("oauth_provider", "")).strip().lower()
            if provider:
                args["oauth_provider"] = provider
            return self._step("browser_session_create", args=args, verify={"expect_status": "success"})

        if intent in {"browser_session_list", "list_browser_sessions"}:
            return self._step("browser_session_list", args={}, verify={"expect_status": "success"})

        if intent in {"browser_session_request", "session_request"}:
            session_id = str(arguments.get("session_id") or self._extract_session_id(original_text))
            url = str(arguments.get("url") or self._extract_url(original_text) or self._extract_domain_like(original_text))
            if session_id and url:
                method = str(arguments.get("method", "GET")).strip().upper() or "GET"
                return self._step(
                    "browser_session_request",
                    args={"session_id": session_id, "url": url, "method": method},
                    verify={"expect_status": "success"},
                )

        if intent in {"browser_session_read_dom", "session_read_dom"}:
            session_id = str(arguments.get("session_id") or self._extract_session_id(original_text))
            url = str(arguments.get("url") or self._extract_url(original_text) or self._extract_domain_like(original_text))
            if session_id and url:
                return self._step(
                    "browser_session_read_dom",
                    args={"session_id": session_id, "url": url, "max_chars": 7000},
                    verify={"expect_status": "success"},
                )

        if intent in {"browser_session_extract_links", "session_extract_links"}:
            session_id = str(arguments.get("session_id") or self._extract_session_id(original_text))
            url = str(arguments.get("url") or self._extract_url(original_text) or self._extract_domain_like(original_text))
            if session_id and url:
                return self._step(
                    "browser_session_extract_links",
                    args={"session_id": session_id, "url": url, "max_links": 80},
                    verify={"expect_status": "success"},
                )

        if intent in {"observe_screen", "computer_observe"}:
            return self._step("computer_observe", args={}, verify={"expect_status": "success", "expect_key": "screenshot_path"})

        if intent in {"desktop_interact", "desktop_action", "desktop_click_and_type"}:
            step = self._build_desktop_interact_step(original_text=original_text, arguments=arguments)
            if step is not None:
                return step

        if intent in {"external_connector_status", "connector_status"}:
            return self._step("external_connector_status", args={}, verify={"expect_status": "success"})
        if intent in {"external_connector_preflight", "connector_preflight"}:
            action_name = str(arguments.get("action", "")).strip().lower()
            if not action_name:
                action_name = "external_email_send"
            provider = str(arguments.get("provider", "auto")).strip().lower() or "auto"
            return self._step(
                "external_connector_preflight",
                args={"action": action_name, "provider": provider},
                verify={"expect_key": "contract_diagnostic"},
            )

        if intent in {"external_email_list", "list_emails"}:
            query = str(arguments.get("query", "")).strip()
            try:
                max_results = int(arguments.get("max_results", 20) or 20)
            except Exception:
                max_results = 20
            provider = str(arguments.get("provider", "auto")).strip() or "auto"
            args: Dict[str, Any] = {"provider": provider, "max_results": max(1, min(max_results, 100))}
            if query:
                args["query"] = query
            return self._step("external_email_list", args=args, verify={"expect_status": "success"})

        if intent in {"external_email_read", "read_email"}:
            message_id = str(arguments.get("message_id") or self._extract_message_id(original_text)).strip()
            if message_id:
                return self._step(
                    "external_email_read",
                    args={"message_id": message_id, "provider": str(arguments.get("provider", "auto"))},
                    verify={"expect_status": "success"},
                )

        if intent in {"oauth_token_maintain", "oauth_maintain_tokens", "maintain_oauth_tokens", "oauth_refresh_all"}:
            lowered_text = str(original_text or "").strip().lower()
            provider = str(arguments.get("provider", "")).strip().lower()
            if not provider:
                if "google" in lowered_text or "gmail" in lowered_text:
                    provider = "google"
                elif any(token in lowered_text for token in ("graph", "microsoft", "outlook", "office 365")):
                    provider = "graph"
            account_id = str(arguments.get("account_id", "")).strip().lower()
            args: Dict[str, Any] = {"dry_run": bool(arguments.get("dry_run", False))}
            if provider:
                args["provider"] = provider
            if account_id:
                args["account_id"] = account_id
            refresh_window = str(arguments.get("refresh_window_s", "")).strip()
            if refresh_window:
                try:
                    args["refresh_window_s"] = max(0, min(int(refresh_window), 86400 * 7))
                except Exception:
                    pass
            return self._step("oauth_token_maintain", args=args, verify={"expect_status": "success"})

        if intent in {"computer_click_target", "click_target", "computer_click_text", "click_text"}:
            query = str(arguments.get("query") or self._extract_phrase(original_text) or self._extract_keyword(original_text))
            if query:
                args: Dict[str, Any] = {
                    "query": query,
                    "target_mode": str(arguments.get("target_mode", "auto")).strip().lower() or "auto",
                    "verify_mode": str(arguments.get("verify_mode", "state_or_visibility")).strip().lower() or "state_or_visibility",
                }
                if str(arguments.get("window_title", "")).strip():
                    args["window_title"] = str(arguments.get("window_title"))
                if str(arguments.get("control_type", "")).strip():
                    args["control_type"] = str(arguments.get("control_type"))
                if str(arguments.get("element_id", "")).strip():
                    args["element_id"] = str(arguments.get("element_id"))
                return self._step("computer_click_target", args=args, verify={"expect_status": "success"})

        if intent in {"computer_wait_for_text", "wait_for_text"}:
            phrase = str(arguments.get("text") or self._extract_phrase(original_text) or self._extract_keyword(original_text))
            expect_visible = bool(arguments.get("expect_visible", True))
            if phrase:
                return self._step(
                    "computer_wait_for_text",
                    args={"text": phrase, "expect_visible": expect_visible, "timeout_s": 10.0},
                    verify={"expect_status": "success"},
                )

        if intent in {"external_email_send", "send_email"}:
            to_list = arguments.get("to")
            if isinstance(to_list, str) and to_list.strip():
                to_list = [to_list.strip()]
            if not isinstance(to_list, list) or not to_list:
                to_list = self._extract_email_addresses(original_text)
            if to_list:
                return self._step(
                    "external_email_send",
                    args={
                        "to": to_list,
                        "subject": str(arguments.get("subject") or self._extract_email_subject(original_text)),
                        "body": str(arguments.get("body") or self._extract_email_body(original_text)),
                        "provider": str(arguments.get("provider", "auto")),
                    },
                    verify={"expect_status": "success"},
                )

        if intent in {"external_calendar_create_event", "create_calendar_event"}:
            start_iso, end_iso = self._extract_datetime_window(original_text)
            args: Dict[str, Any] = {
                "title": str(arguments.get("title") or self._extract_calendar_title(original_text)),
                "provider": str(arguments.get("provider", "auto")),
            }
            if str(arguments.get("start", "")).strip():
                args["start"] = str(arguments.get("start"))
            elif start_iso:
                args["start"] = start_iso
            if str(arguments.get("end", "")).strip():
                args["end"] = str(arguments.get("end"))
            elif end_iso:
                args["end"] = end_iso
            return self._step("external_calendar_create_event", args=args, verify={"expect_status": "success"})

        if intent in {"external_calendar_list_events", "list_calendar_events"}:
            try:
                max_results = int(arguments.get("max_results", 20) or 20)
            except Exception:
                max_results = 20
            args: Dict[str, Any] = {
                "provider": str(arguments.get("provider", "auto")),
                "max_results": max(1, min(max_results, 100)),
            }
            if str(arguments.get("time_min", "")).strip():
                args["time_min"] = str(arguments.get("time_min"))
            if str(arguments.get("time_max", "")).strip():
                args["time_max"] = str(arguments.get("time_max"))
            return self._step("external_calendar_list_events", args=args, verify={"expect_status": "success"})

        if intent in {"external_calendar_update_event", "update_calendar_event"}:
            event_id = str(arguments.get("event_id") or self._extract_event_id(original_text)).strip()
            if event_id:
                args: Dict[str, Any] = {
                    "event_id": event_id,
                    "provider": str(arguments.get("provider", "auto")),
                }
                for field in ("title", "description", "start", "end", "timezone"):
                    value = str(arguments.get(field, "")).strip()
                    if value:
                        args[field] = value
                attendees = arguments.get("attendees")
                if isinstance(attendees, list):
                    args["attendees"] = attendees
                if len(args) <= 2:
                    return None
                return self._step("external_calendar_update_event", args=args, verify={"expect_status": "success"})

        if intent in {"external_doc_create", "create_document"}:
            return self._step(
                "external_doc_create",
                args={
                    "title": str(arguments.get("title") or self._extract_document_title(original_text)),
                    "content": str(arguments.get("content") or self._extract_content(original_text)),
                    "provider": str(arguments.get("provider", "auto")),
                },
                verify={"expect_status": "success"},
            )

        if intent in {"external_doc_list", "list_documents"}:
            query = str(arguments.get("query", "")).strip()
            try:
                max_results = int(arguments.get("max_results", 20) or 20)
            except Exception:
                max_results = 20
            args: Dict[str, Any] = {
                "provider": str(arguments.get("provider", "auto")),
                "max_results": max(1, min(max_results, 100)),
            }
            if query:
                args["query"] = query
            return self._step("external_doc_list", args=args, verify={"expect_status": "success"})

        if intent in {"external_doc_read", "read_document"}:
            document_id = str(arguments.get("document_id") or self._extract_document_id(original_text)).strip()
            if document_id:
                return self._step(
                    "external_doc_read",
                    args={"document_id": document_id, "provider": str(arguments.get("provider", "auto"))},
                    verify={"expect_status": "success"},
                )

        if intent in {"external_doc_update", "update_document"}:
            document_id = str(arguments.get("document_id") or self._extract_document_id(original_text)).strip()
            if document_id:
                args: Dict[str, Any] = {
                    "document_id": document_id,
                    "provider": str(arguments.get("provider", "auto")),
                }
                title = str(arguments.get("title", "")).strip()
                content = str(arguments.get("content", "")).strip()
                if not title:
                    candidate_title = self._extract_document_title(original_text)
                    if candidate_title and candidate_title.lower() != document_id.lower():
                        title = candidate_title
                if not content:
                    content = self._extract_optional_content(original_text)
                if title:
                    args["title"] = title
                if content:
                    args["content"] = content
                if len(args) <= 2:
                    return None
                return self._step("external_doc_update", args=args, verify={"expect_status": "success"})

        if intent in {"external_task_list", "list_tasks", "list_todo_tasks"}:
            include_completed_arg = arguments.get("include_completed")
            include_completed = bool(include_completed_arg) if isinstance(include_completed_arg, bool) else True
            if not isinstance(include_completed_arg, bool):
                lowered_text = str(original_text or "").lower()
                if any(token in lowered_text for token in ("open tasks", "pending tasks", "incomplete tasks", "todo open")):
                    include_completed = False
            try:
                max_results = int(arguments.get("max_results", 25) or 25)
            except Exception:
                max_results = 25
            args = {
                "provider": str(arguments.get("provider", "auto")),
                "max_results": max(1, min(max_results, 200)),
                "include_completed": include_completed,
            }
            query = str(arguments.get("query") or self._extract_quoted(original_text)).strip()
            if query:
                args["query"] = query
            if str(arguments.get("list_id", "")).strip():
                args["list_id"] = str(arguments.get("list_id"))
            return self._step("external_task_list", args=args, verify={"expect_status": "success"})

        if intent in {"external_task_create", "create_task", "add_task", "todo_add"}:
            title = str(arguments.get("title") or self._extract_task_title(original_text)).strip()
            if title:
                args: Dict[str, Any] = {
                    "title": title,
                    "provider": str(arguments.get("provider", "auto")),
                }
                notes = str(arguments.get("notes") or arguments.get("content") or self._extract_optional_content(original_text)).strip()
                if notes:
                    args["notes"] = notes
                due = str(arguments.get("due") or "").strip()
                if due:
                    args["due"] = due
                status = str(arguments.get("status") or self._extract_task_status(original_text)).strip()
                if status:
                    args["status"] = status
                if str(arguments.get("list_id", "")).strip():
                    args["list_id"] = str(arguments.get("list_id"))
                return self._step("external_task_create", args=args, verify={"expect_status": "success"})

        if intent in {"external_task_update", "update_task", "complete_task", "mark_task_done"}:
            task_id = str(arguments.get("task_id") or self._extract_task_id(original_text)).strip()
            if task_id:
                args: Dict[str, Any] = {
                    "task_id": task_id,
                    "provider": str(arguments.get("provider", "auto")),
                }
                title = str(arguments.get("title", "")).strip()
                notes = str(arguments.get("notes") or arguments.get("content") or self._extract_optional_content(original_text)).strip()
                due = str(arguments.get("due", "")).strip()
                status = str(arguments.get("status") or self._extract_task_status(original_text)).strip()
                if title:
                    args["title"] = title
                if notes:
                    args["notes"] = notes
                if due:
                    args["due"] = due
                if status:
                    args["status"] = status
                if str(arguments.get("list_id", "")).strip():
                    args["list_id"] = str(arguments.get("list_id"))
                if len(args) <= 2:
                    return None
                return self._step("external_task_update", args=args, verify={"expect_status": "success"})

        if intent in {"accessibility_status", "ui_status"}:
            return self._step("accessibility_status", args={}, verify={"expect_status": "success"})

        if intent in {"accessibility_list_elements", "list_ui_elements"}:
            return self._step(
                "accessibility_list_elements",
                args={
                    "window_title": str(arguments.get("window_title", "")),
                    "query": str(arguments.get("query", "")),
                    "max_elements": int(arguments.get("max_elements", 120) or 120),
                },
                verify={"expect_status": "success"},
            )

        if intent in {"accessibility_find_element", "find_ui_element"}:
            query = str(arguments.get("query") or self._extract_phrase(original_text) or self._extract_keyword(original_text))
            if query:
                return self._step(
                    "accessibility_find_element",
                    args={
                        "query": query,
                        "window_title": str(arguments.get("window_title", "")),
                        "control_type": str(arguments.get("control_type", "")),
                    },
                    verify={"expect_status": "success"},
                )

        if intent in {"accessibility_invoke_element", "click_ui_element"}:
            query = str(arguments.get("query") or self._extract_phrase(original_text) or self._extract_keyword(original_text))
            element_id = str(arguments.get("element_id", "")).strip()
            if query or element_id:
                args: Dict[str, Any] = {
                    "action": str(arguments.get("action", "click")).strip() or "click",
                }
                if query:
                    args["query"] = query
                if element_id:
                    args["element_id"] = element_id
                if str(arguments.get("window_title", "")).strip():
                    args["window_title"] = str(arguments.get("window_title"))
                return self._step("accessibility_invoke_element", args=args, verify={"expect_status": "success"})

        return None

    @staticmethod
    def _extract_json_object(text: str) -> Dict[str, Any]:
        if not text:
            return {}
        clean = text.strip()

        try:
            obj = json.loads(clean)
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass

        fence = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", clean, flags=re.DOTALL | re.IGNORECASE)
        if fence:
            try:
                obj = json.loads(fence.group(1))
                if isinstance(obj, dict):
                    return obj
            except Exception:
                pass

        match = re.search(r"\{.*\}", clean, flags=re.DOTALL)
        if not match:
            return {}
        try:
            obj = json.loads(match.group(0))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _clamp_int(value: Any, *, minimum: int, maximum: int, default: int) -> int:
        try:
            parsed = int(value)
        except Exception:
            parsed = default
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _clamp_float(value: Any, *, minimum: float, maximum: float, default: float) -> float:
        try:
            parsed = float(value)
        except Exception:
            parsed = default
        return max(minimum, min(maximum, parsed))

    def _sanitize_args(self, data: Dict[str, Any], depth: int = 0) -> Dict[str, Any]:
        if depth > 4:
            return {}
        out: Dict[str, Any] = {}
        for key, value in data.items():
            if not isinstance(key, str):
                continue
            if isinstance(value, (str, int, float, bool)) or value is None:
                out[key] = value
            elif isinstance(value, dict):
                out[key] = self._sanitize_args(value, depth + 1)
            elif isinstance(value, list):
                out[key] = [self._sanitize_list_item(item, depth + 1) for item in value[:50]]
            else:
                out[key] = str(value)
        return out

    def _sanitize_list_item(self, value: Any, depth: int) -> Any:
        if depth > 4:
            return None
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        if isinstance(value, dict):
            return self._sanitize_args(value, depth + 1)
        if isinstance(value, list):
            return [self._sanitize_list_item(item, depth + 1) for item in value[:50]]
        return str(value)

    @staticmethod
    def _looks_like_action_clause(lowered: str) -> bool:
        action_markers = (
            "open ",
            "launch ",
            "start app",
            "run app",
            "read webpage",
            "extract links",
            "browser session",
            "session request",
            "session read",
            "session extract links",
            "security",
            "defender",
            "play ",
            "play pause",
            "play/pause",
            "toggle playback",
            "toggle media",
            "pause",
            "resume",
            "stop",
            "next track",
            "next song",
            "skip track",
            "skip song",
            "previous track",
            "prev track",
            "last track",
            "previous song",
            "metrics",
            "system status",
            "list processes",
            "active window",
            "list windows",
            "focus window",
            "list ui elements",
            "find ui element",
            "click ui element",
            "accessibility status",
            "notify",
            "clipboard",
            "hotkey",
            "search box",
            "find box",
            "type ",
            "move mouse",
            "click",
            "scroll",
            "screenshot",
            "observe screen",
            "text visible",
            "click text",
            "click target",
            "wait for text",
            "find text targets",
            "ocr image",
            "send email",
            "email to",
            "schedule meeting",
            "calendar event",
            "create document",
            "create doc",
            "list tasks",
            "create task",
            "add task",
            "update task",
            "complete task",
            "todo",
            "to do",
            "connector status",
            "run trusted script",
            "run whitelisted app",
            "terminate process",
            "search files",
            "search text",
            "scan directory",
            "list folder",
            "create folder",
            "rename selected",
            "open properties",
            "show properties",
            "properties dialog",
            "folder size",
            "read file",
            "write file",
            "copy file",
            "backup file",
            "hash file",
            "time",
        )
        return any(marker in lowered for marker in action_markers) or Planner._looks_like_desktop_followup_clause(lowered)

    def _split_compound_clauses(self, text: str) -> List[str]:
        normalized = re.sub(r"\s+", " ", text or "").strip()
        if not normalized:
            return []

        raw_parts = re.split(
            r"\s*(?:;|,\s*then\b|\.?\s+and then\b|\.?\s+after that\b|\.?\s+then\b|\.?\s+next\b(?!\s+(?:track|song|tab|step)\b))\s*",
            normalized,
            flags=re.IGNORECASE,
        )
        parts = [part.strip(" ,.;") for part in raw_parts if part and part.strip(" ,.;")]
        if len(parts) <= 1:
            parts = self._split_actionable_and_clauses(normalized)
            if len(parts) <= 1:
                return []

        actionable_count = sum(1 for part in parts if self._looks_like_action_clause(part.lower()))
        if actionable_count < 2:
            return []

        return parts[: self.max_llm_steps]

    def _split_actionable_and_clauses(self, normalized: str) -> List[str]:
        if " and " not in normalized.lower():
            return []

        # Avoid false-positive splitting for common single-intent noun phrases.
        lowered = normalized.lower()
        if any(
            marker in lowered
            for marker in (
                "between ",
                " from ",
                " among ",
                " and roll",
                " files and folders",
                " rock and roll",
            )
        ):
            return []

        raw = re.split(r"\s+\band\b\s+", normalized, flags=re.IGNORECASE)
        clauses = [part.strip(" ,.;") for part in raw if part and part.strip(" ,.;")]
        if len(clauses) <= 1:
            return []

        actionable_flags = [self._looks_like_action_clause(clause.lower()) for clause in clauses]
        if sum(1 for flag in actionable_flags if flag) < 2:
            return []
        if not all(actionable_flags):
            return []

        return clauses

    def _build_compound_steps(self, text: str) -> Optional[tuple[str, List[PlanStep]]]:
        clauses = self._split_compound_clauses(text)
        if len(clauses) <= 1:
            return None

        lowered_text = text.lower()
        has_temporal_connector = bool(
            " and then " in lowered_text
            or " then " in lowered_text
            or " after that " in lowered_text
            or ";" in lowered_text
            or re.search(r"\bnext\b(?!\s+(?:track|song|tab)\b)", lowered_text)
        )
        requested_parallelizable = (" and " in lowered_text) and not has_temporal_connector

        merged_steps: List[PlanStep] = []
        intents: List[str] = []
        total_clauses = len(clauses)
        previous_clause_tail: str | None = None
        current_desktop_context: Dict[str, str] = {}
        desktop_context_tail: str | None = None
        desktop_context_anchor: str | None = None
        has_contextual_dependency = False

        for index, clause in enumerate(clauses):
            segment = clause.strip()
            if not segment:
                continue

            inherited_desktop_context: Optional[Dict[str, str]] = None
            if current_desktop_context and self._looks_like_desktop_followup_clause(segment.lower()):
                explicit_app_name, explicit_window_title = self._extract_explicit_desktop_target_context(text=segment)
                if not explicit_app_name and not explicit_window_title:
                    inherited_desktop_context = dict(current_desktop_context)

            intent, clause_steps = self._build_primary_steps(
                segment,
                segment.lower(),
                allow_compound=False,
                desktop_context=inherited_desktop_context,
            )
            if intent == "speak":
                continue

            # Suppress per-clause spoken acknowledgements in multi-action chains.
            if clause_steps and clause_steps[-1].action == "tts_speak" and (requested_parallelizable or index < total_clauses - 1):
                clause_steps = clause_steps[:-1]

            if not clause_steps:
                continue

            clause_uses_inherited_context = bool(
                inherited_desktop_context and self._steps_use_desktop_context(clause_steps, inherited_desktop_context)
            )
            if clause_uses_inherited_context:
                has_contextual_dependency = True

            previous_in_clause: str | None = None
            for step in clause_steps:
                dependencies: List[str] = []
                if previous_in_clause:
                    dependencies.append(previous_in_clause)
                if not requested_parallelizable and previous_clause_tail:
                    dependencies.append(previous_clause_tail)
                if clause_uses_inherited_context and desktop_context_tail:
                    dependencies.append(desktop_context_tail)
                if clause_uses_inherited_context and desktop_context_anchor:
                    dependencies.append(desktop_context_anchor)
                if step.depends_on:
                    dependencies.extend(step.depends_on)
                deduped: List[str] = []
                for dep in dependencies:
                    clean = str(dep).strip()
                    if clean and clean not in deduped:
                        deduped.append(clean)
                step.depends_on = deduped
                previous_in_clause = step.step_id
                step_context = self._extract_desktop_context_from_step(step)
                if step_context:
                    previous_app = str(current_desktop_context.get("app_name") or "").strip().lower()
                    previous_window = str(current_desktop_context.get("window_title") or "").strip().lower()
                    next_app = str(step_context.get("app_name") or "").strip().lower()
                    next_window = str(step_context.get("window_title") or "").strip().lower()
                    if (
                        not desktop_context_anchor
                        or (next_app and next_app != previous_app)
                        or (next_window and next_window != previous_window)
                    ):
                        desktop_context_anchor = step.step_id
                    current_desktop_context.update(step_context)
                    desktop_context_tail = step.step_id
            if previous_in_clause:
                previous_clause_tail = previous_in_clause

            intents.append(intent)
            merged_steps.extend(clause_steps)
            if len(merged_steps) >= 8:
                break

        if not merged_steps:
            return None

        if intents:
            prefix = "compound_parallel_" if requested_parallelizable and not has_contextual_dependency else "compound_"
            intent_name = prefix + "_".join(intents[:3])
        else:
            intent_name = "compound_request"
        return (intent_name[:96], merged_steps[:8])

    def _build_desktop_interact_step(
        self,
        *,
        original_text: str,
        arguments: Optional[Dict[str, Any]] = None,
        require_target_context: bool = False,
        desktop_context: Optional[Dict[str, Any]] = None,
    ) -> Optional[PlanStep]:
        args = arguments if isinstance(arguments, dict) else {}
        text = str(original_text or "")
        lowered_text = text.lower()

        app_name, window_title = self._extract_explicit_desktop_target_context(text=text, arguments=args)
        if not app_name:
            app_name = self._extract_desktop_wizard_app_name(text)
        used_inherited_target_context = False
        if (
            not app_name
            and not window_title
            and isinstance(desktop_context, dict)
            and desktop_context
            and self._looks_like_desktop_followup_clause(lowered_text, arguments=args)
        ):
            inherited_app_name = str(desktop_context.get("app_name") or "").strip()
            inherited_window_title = str(desktop_context.get("window_title") or "").strip()
            if inherited_app_name and self._is_probable_desktop_app_name(inherited_app_name):
                app_name = inherited_app_name
            if inherited_window_title:
                window_title = inherited_window_title
            used_inherited_target_context = bool(app_name or window_title)

        query = str(args.get("query") or args.get("target") or "").strip()
        typed_text = str(args.get("text") or "").strip()
        navigation_target = str(args.get("url") or self._extract_url(text) or self._extract_domain_like(text) or "").strip()
        tab_target = str(args.get("tab_target") or query or "").strip()
        if not tab_target:
            tab_target = self._extract_desktop_tab_target(text)
        tab_search_query = self._extract_desktop_tab_search_query(text)
        tab_page_query = self._extract_desktop_tab_page_query(text, app_name=app_name)
        keys_raw = args.get("keys")
        action_name = str(args.get("action") or "").strip().lower()
        probable_terminal_context = bool(app_name and self._is_probable_terminal_app_name(app_name))
        probable_editor_context = bool(app_name and self._is_probable_editor_app_name(app_name))
        probable_browser_context = bool(app_name and self._is_probable_browser_app_name(app_name))
        probable_file_manager_context = bool(app_name and self._is_probable_file_manager_app_name(app_name))
        probable_chat_context = bool(app_name and self._is_probable_chat_app_name(app_name))
        probable_office_context = bool(app_name and self._is_probable_office_app_name(app_name))
        probable_mail_context = bool(app_name and self._is_probable_mail_app_name(app_name))
        probable_media_context = bool(app_name and self._is_probable_media_app_name(app_name))
        probable_form_context = bool(app_name and self._is_probable_form_app_name(app_name))
        probable_generic_context = bool(app_name or window_title)
        probable_sidebar_navigation_context = bool(app_name and self._is_probable_sidebar_navigation_app_name(app_name))
        probable_tree_navigation_context = bool(app_name and self._is_probable_tree_navigation_app_name(app_name))
        probable_table_surface_context = bool(app_name and self._is_probable_table_surface_app_name(app_name))
        sidebar_item_query = self._extract_desktop_sidebar_item_query(text, app_name=app_name)
        toolbar_action_query = self._extract_desktop_toolbar_action_query(text)
        context_menu_item_query = self._extract_desktop_context_menu_item_query(text)
        dialog_button_query = self._extract_desktop_dialog_button_query(text)
        field_query = self._extract_desktop_field_query(text, app_name=app_name)
        field_value = self._extract_desktop_field_value(text, app_name=app_name)
        dropdown_query = self._extract_desktop_dropdown_query(text, app_name=app_name)
        dropdown_option = self._extract_desktop_dropdown_option(text, app_name=app_name)
        checkbox_query = self._extract_desktop_checkbox_query(text)
        radio_option_query = self._extract_desktop_radio_option_query(text)
        toggle_query = self._extract_desktop_toggle_query(text)
        value_control_query = self._extract_desktop_value_control_query(text, app_name=app_name)
        value_control_target = self._extract_desktop_value_control_target(text, app_name=app_name)
        adjust_amount = self._extract_desktop_adjust_amount(text)
        tree_item_query = self._extract_desktop_tree_item_query(text, app_name=app_name)
        list_item_query = self._extract_desktop_list_item_query(text)
        table_row_query = self._extract_desktop_table_row_query(text, app_name=app_name)

        if isinstance(keys_raw, list):
            hotkey_keys = [str(item).strip().lower() for item in keys_raw if str(item).strip()]
        elif isinstance(keys_raw, str):
            hotkey_keys = [part.strip().lower() for part in re.split(r"[+,]", keys_raw) if part.strip()]
        else:
            hotkey_keys = []

        if not action_name:
            if any(token in lowered_text for token in ("run terminal command", "execute terminal command", "run shell command", "execute shell command")):
                action_name = "terminal_command"
            elif any(token in lowered_text for token in ("command palette", "run command", "execute command")):
                action_name = "command"
            elif re.search(r"\b(?:focus|open)\s+address bar\b|\baddress bar\b", lowered_text) and (probable_browser_context or probable_file_manager_context):
                action_name = "focus_address_bar"
            elif re.search(r"\b(?:open\s+)?bookmarks\b", lowered_text) and bool(app_name or window_title):
                action_name = "open_bookmarks"
            elif re.search(r"\b(?:open\s+)?history\b", lowered_text) and bool(app_name or window_title):
                action_name = "open_history"
            elif re.search(r"\b(?:open\s+)?downloads\b", lowered_text) and bool(app_name or window_title):
                action_name = "open_downloads"
            elif re.search(r"\b(?:open\s+)?(?:developer tools|devtools)\b", lowered_text) and bool(app_name or window_title):
                action_name = "open_devtools"
            elif re.search(r"\b(?:go\s+back|back)\b", lowered_text) and (probable_browser_context or probable_file_manager_context):
                action_name = "go_back"
            elif re.search(r"\b(?:go\s+forward|forward)\b", lowered_text) and (probable_browser_context or probable_file_manager_context):
                action_name = "go_forward"
            elif probable_browser_context and tab_search_query:
                action_name = "search_tabs"
            elif re.search(r"\b(?:open|show|focus)\s+(?:the\s+)?(?:tab search|search open tabs|search tabs)\b|\btab search\b", lowered_text) and probable_browser_context:
                action_name = "open_tab_search"
            elif re.search(r"\b(?:focus|open)\s+(?:the\s+)?(?:search|find)\s+box\b|\bsearch box\b|\bfind box\b", lowered_text) and bool(app_name or window_title):
                action_name = "focus_search_box"
            elif re.search(r"\b(?:new chat|start new chat|new conversation|start conversation)\b", lowered_text) and probable_chat_context:
                action_name = "new_chat"
            elif re.search(r"\b(?:open chat with|new chat with|open conversation with|jump to conversation|switch conversation|switch to chat|switch to conversation|chat with|conversation with|dm with)\b", lowered_text) and probable_chat_context:
                action_name = "jump_to_conversation"
            elif re.search(r"\b(?:send message|message(?: to)?|reply(?: to)?)\b", lowered_text) and probable_chat_context:
                action_name = "send_message"
            elif re.search(r"\b(?:new email|compose email|draft email|new mail|compose mail)\b", lowered_text) and probable_mail_context:
                action_name = "new_email_draft"
            elif re.search(r"\b(?:reply all|reply to all)\b", lowered_text) and probable_mail_context:
                action_name = "reply_all_email"
            elif re.search(r"\b(?:reply(?: to)?(?: the)?(?: email|mail|message)?)\b", lowered_text) and probable_mail_context:
                action_name = "reply_email"
            elif re.search(r"\b(?:forward(?: the)?(?: email|mail|message)?)\b", lowered_text) and probable_mail_context:
                action_name = "forward_email"
            elif re.search(r"\b(?:new calendar event|create calendar event|new event|create event|new meeting|schedule meeting|create meeting)\b", lowered_text) and probable_mail_context:
                action_name = "new_calendar_event"
            elif re.search(r"\b(?:new document|new workbook|new presentation|new note)\b", lowered_text) and (probable_office_context or probable_editor_context):
                action_name = "new_document"
            elif re.search(r"\b(?:save(?: document| file| workbook| sheet| presentation)?|save as)\b", lowered_text) and (probable_office_context or probable_editor_context):
                action_name = "save_document"
            elif re.search(r"\b(?:open print dialog|print dialog|print)\b", lowered_text) and (probable_office_context or probable_browser_context or probable_editor_context):
                action_name = "open_print_dialog"
            elif re.search(r"\b(?:start presentation|start slideshow|slideshow|slide show|present presentation)\b", lowered_text) and probable_office_context:
                action_name = "start_presentation"
            elif re.search(r"\b(?:play\s*/\s*pause|play pause|toggle playback|toggle media playback|toggle media)\b", lowered_text) and probable_media_context:
                action_name = "play_pause_media"
            elif re.search(r"\b(?:next track|next song|skip track|skip song|skip ahead)\b", lowered_text) and probable_media_context:
                action_name = "next_track"
            elif re.search(r"\b(?:previous track|prev track|last track|previous song|back track|rewind track)\b", lowered_text) and probable_media_context:
                action_name = "previous_track"
            elif re.search(r"\b(?:pause(?: playback| music| media| song)?|hold music)\b", lowered_text) and probable_media_context:
                action_name = "pause_media"
            elif re.search(r"\b(?:resume(?: playback| music| media| song)?|continue playback|continue music)\b", lowered_text) and probable_media_context:
                action_name = "resume_media"
            elif re.search(r"\b(?:stop(?: playback| media| music| song)?)\b", lowered_text) and probable_media_context:
                action_name = "stop_media"
            elif re.search(r"\b(?:new|create)\s+folder\b", lowered_text) and probable_file_manager_context:
                action_name = "new_folder"
            elif re.search(r"\b(?:focus|open)\s+(?:the\s+)?(?:folder tree|navigation pane)\b", lowered_text) and probable_file_manager_context:
                action_name = "focus_folder_tree"
            elif re.search(r"\b(?:focus|open)\s+(?:the\s+)?(?:file list|items view|list view)\b", lowered_text) and probable_file_manager_context:
                action_name = "focus_file_list"
            elif re.search(r"\brename(?:\s+the)?\s+(?:selected\s+)?(?:file|folder|item|selection)\b", lowered_text) and probable_file_manager_context:
                action_name = "rename_selection"
            elif re.search(r"\b(?:open|show)\s+(?:the\s+)?properties(?:\s+dialog)?\b|\bproperties(?:\s+dialog)?\b", lowered_text) and probable_file_manager_context and not re.search(r"\b(?:context menu|shortcut menu|right click menu)\b", lowered_text):
                action_name = "open_properties_dialog"
            elif re.search(r"\b(?:open|show)\s+(?:the\s+)?preview pane\b|\bpreview pane\b", lowered_text) and probable_file_manager_context:
                action_name = "open_preview_pane"
            elif re.search(r"\b(?:open|show)\s+(?:the\s+)?details pane\b|\bdetails pane\b", lowered_text) and probable_file_manager_context:
                action_name = "open_details_pane"
            elif re.search(r"\b(?:refresh|reload)(?:\s+(?:view|window|page))?\b", lowered_text) and bool(app_name or window_title):
                action_name = "refresh_view"
            elif re.search(r"\b(?:go up|up one level|parent folder)\b", lowered_text) and probable_file_manager_context:
                action_name = "go_up_level"
            elif re.search(r"\b(?:focus|open)\s+explorer\b", lowered_text) and probable_editor_context:
                action_name = "focus_explorer"
            elif re.search(r"\b(?:workspace search|search workspace|find in files)\b", lowered_text) and probable_editor_context:
                action_name = "workspace_search"
            elif re.search(r"\b(?:find and replace|replace)\b", lowered_text) and (probable_editor_context or probable_office_context):
                action_name = "find_replace"
            elif re.search(r"\b(?:focus|open)\s+(?:the\s+)?folder pane\b", lowered_text) and probable_mail_context:
                action_name = "focus_folder_pane"
            elif re.search(r"\b(?:focus|open)\s+(?:the\s+)?message list\b", lowered_text) and probable_mail_context:
                action_name = "focus_message_list"
            elif re.search(r"\b(?:focus|open)\s+(?:the\s+)?(?:reading pane|preview pane)\b", lowered_text) and probable_mail_context:
                action_name = "focus_reading_pane"
            elif re.search(r"\b(?:focus|open)\s+(?:the\s+)?(?:tree|navigation tree|tree view)\b", lowered_text) and probable_generic_context:
                action_name = "focus_navigation_tree"
            elif tree_item_query and (probable_tree_navigation_context or re.search(r"\bin\s+(?:the\s+)?(?:tree|navigation tree|tree view)\b", lowered_text)):
                action_name = "expand_tree_item" if re.search(r"\bexpand\b", lowered_text) else "select_tree_item"
            elif re.search(r"\b(?:focus|open)\s+(?:the\s+)?(?:list|results list|list surface|list pane)\b", lowered_text) and probable_generic_context:
                action_name = "focus_list_surface"
            elif list_item_query and re.search(r"\bin\s+(?:the\s+)?(?:list|results list|list view|list surface|list pane)\b", lowered_text):
                action_name = "select_list_item"
            elif re.search(r"\b(?:focus|open)\s+(?:the\s+)?(?:table|grid|data grid)\b", lowered_text) and probable_generic_context:
                action_name = "focus_data_table"
            elif table_row_query and (probable_table_surface_context or re.search(r"\b(?:row|table|grid|data grid)\b", lowered_text)):
                action_name = "select_table_row"
            elif re.search(r"\b(?:focus|open)\s+(?:the\s+)?(?:form|form surface)\b", lowered_text) and probable_generic_context:
                action_name = "focus_form_surface"
            elif field_query and re.search(r"\b(?:focus|open|select)\b", lowered_text) and re.search(r"\b(?:field|input|text box|textbox|edit box)\b", lowered_text):
                action_name = "focus_input_field"
            elif value_control_query and value_control_target and re.search(r"\b(?:set|adjust|move)\b", lowered_text):
                action_name = "set_value_control"
            elif field_query and field_value and (probable_form_context or re.search(r"\b(?:field|input|text box|textbox|edit box)\b", lowered_text)) and re.search(r"\b(?:set|fill|enter)\b", lowered_text):
                action_name = "set_field_value"
            elif dropdown_query and re.search(r"\b(?:focus|open|show)\b", lowered_text) and re.search(r"\b(?:dropdown|combo box)\b", lowered_text):
                action_name = "open_dropdown"
            elif dropdown_query and dropdown_option and re.search(r"\b(?:select|choose)\b", lowered_text):
                action_name = "select_dropdown_option"
            elif checkbox_query and re.search(r"\b(?:focus|select)\b", lowered_text):
                action_name = "focus_checkbox"
            elif toggle_query and re.search(r"\b(?:turn on|enable|switch on)\b", lowered_text):
                action_name = "enable_switch"
            elif toggle_query and re.search(r"\b(?:turn off|disable|switch off)\b", lowered_text):
                action_name = "disable_switch"
            elif checkbox_query and re.search(r"\buncheck\b", lowered_text):
                action_name = "uncheck_checkbox"
            elif checkbox_query and re.search(r"\bcheck\b", lowered_text):
                action_name = "check_checkbox"
            elif radio_option_query and re.search(r"\b(?:select|choose|pick)\b", lowered_text):
                action_name = "select_radio_option"
            elif value_control_query and re.search(r"\b(?:focus|open|select)\b", lowered_text) and re.search(r"\b(?:slider|spinner|stepper|value control|number input|numeric field|value)\b", lowered_text):
                action_name = "focus_value_control"
            elif value_control_query and re.search(r"\b(?:increase|raise|bump up|turn up)\b", lowered_text):
                action_name = "increase_value"
            elif value_control_query and re.search(r"\b(?:decrease|lower|reduce|turn down)\b", lowered_text):
                action_name = "decrease_value"
            elif toggle_query and re.search(r"\btoggle\b", lowered_text):
                action_name = "toggle_switch"
            elif tab_page_query and not probable_browser_context and not probable_editor_context and not probable_file_manager_context and not probable_terminal_context and (probable_form_context or probable_generic_context):
                action_name = "select_tab_page"
            elif (probable_form_context or bool(app_name or window_title)) and re.search(r"\b(?:apply|save|submit|commit)\s+(?:the\s+)?(?:settings|changes|form|dialog|options|properties)(?:\s+(?:page|step))?\b|\b(?:save|apply)\s+changes\b|\bcomplete\s+(?:the\s+)?(?:form|dialog|settings)\s+page\b", lowered_text):
                action_name = "complete_form_page"
            elif (probable_form_context or bool(app_name or window_title)) and re.search(r"\b(?:continue|work|move|run|go)\s+(?:through|across)\s+(?:the\s+)?(?:form|dialog|settings|options|properties)\b|\b(?:apply|save|submit|finish|complete)\s+(?:the\s+)?(?:form|dialog|settings|options|properties)\s+(?:flow|all the way)\b|\b(?:run|take)\s+(?:the\s+)?(?:form|dialog|settings)\s+to\s+the\s+end\b", lowered_text):
                action_name = "complete_form_flow"
            elif probable_form_context and re.search(r"\b(?:continue|work|move|run|go)\s+(?:through|across)\s+(?:the\s+)?(?:installer|installation|setup(?: wizard)?|wizard)\b|\b(?:complete|finish)\s+(?:the\s+)?(?:installer|installation|setup(?: wizard)?|wizard)\s+(?:flow|all the way)\b|\b(?:run|take)\s+(?:the\s+)?(?:installer|installation|setup(?: wizard)?|wizard)\s+to\s+the\s+end\b", lowered_text):
                action_name = "complete_wizard_flow"
            elif probable_form_context and re.search(r"\b(?:go to|move to|continue(?: to)?|advance(?: to)?)\s+(?:the\s+)?next\s+step\b|\bnext step\b", lowered_text):
                action_name = "next_wizard_step"
            elif probable_form_context and re.search(r"\b(?:go back|move back|return to|previous step|prior step|back step)\b", lowered_text):
                action_name = "previous_wizard_step"
            elif probable_form_context and re.search(r"\b(?:finish|complete)\s+(?:the\s+)?(?:installer|installation|setup(?: wizard)?|wizard)\b|\bfinish setup\b|\bcomplete setup\b", lowered_text):
                action_name = "finish_wizard"
            elif probable_form_context and re.search(r"\b(?:continue|advance)\s+(?:the\s+)?(?:installer|installation|setup(?: wizard)?|wizard)(?:\s+(?:page|step))?\b|\bcontinue setup\b|\bcontinue installer\b|\bcomplete\s+(?:the\s+)?(?:installer|installation|setup(?: wizard)?|wizard)\s+(?:page|step)\b", lowered_text):
                action_name = "complete_wizard_page"
            elif re.search(r"\b(?:focus|open)\s+(?:the\s+)?(?:sidebar|side panel)\b", lowered_text) and probable_generic_context:
                action_name = "focus_sidebar"
            elif sidebar_item_query and (probable_sidebar_navigation_context or re.search(r"\bin\s+(?:the\s+)?(?:sidebar|side panel)\b", lowered_text)):
                action_name = "select_sidebar_item"
            elif re.search(r"\b(?:focus|open)\s+(?:the\s+)?(?:main content|content area|main pane|document area)\b", lowered_text) and probable_generic_context:
                action_name = "focus_main_content"
            elif re.search(r"\b(?:focus|open)\s+(?:the\s+)?(?:toolbar|command bar|menu bar)\b", lowered_text) and probable_generic_context:
                action_name = "focus_toolbar"
            elif toolbar_action_query and probable_generic_context:
                action_name = "invoke_toolbar_action"
            elif context_menu_item_query and probable_generic_context:
                action_name = "select_context_menu_item"
            elif re.search(r"\b(?:open|show)\s+(?:the\s+)?(?:context menu|shortcut menu|right click menu)\b|\bcontext menu\b", lowered_text) and probable_generic_context:
                action_name = "open_context_menu"
            elif re.search(r"\b(?:dismiss|close|cancel)\s+(?:the\s+)?(?:dialog|popup|modal|menu)\b", lowered_text) and probable_generic_context:
                action_name = "dismiss_dialog"
            elif re.search(r"\b(?:confirm|accept|approve|ok)\s+(?:the\s+)?(?:dialog|popup|modal)\b|\bpress ok\b", lowered_text) and probable_generic_context:
                action_name = "confirm_dialog"
            elif dialog_button_query and probable_generic_context:
                action_name = "press_dialog_button"
            elif re.search(r"\b(?:open|switch to|show)\s+(?:the\s+)?(?:people|contacts)(?: view)?\b|\b(?:people|contacts) view\b", lowered_text) and probable_mail_context:
                action_name = "open_people_view"
            elif re.search(r"\b(?:open|switch to|show)\s+(?:the\s+)?(?:tasks|to do|todo)(?: view)?\b|\b(?:tasks|to do|todo) view\b", lowered_text) and probable_mail_context:
                action_name = "open_tasks_view"
            elif re.search(r"\b(?:open|switch to|show)\s+(?:the\s+)?calendar(?: view)?\b|\bcalendar view\b", lowered_text) and probable_mail_context:
                action_name = "open_calendar_view"
            elif re.search(r"\b(?:open|switch to|show)\s+(?:the\s+)?(?:mail|inbox)(?: view)?\b|\bmail view\b|\binbox\b", lowered_text) and probable_mail_context:
                action_name = "open_mail_view"
            elif re.search(r"\b(?:go to symbol|symbol search|find symbol|open symbol)\b", lowered_text) and probable_editor_context:
                action_name = "go_to_symbol"
            elif re.search(r"\brename symbol\b|\brename to\b", lowered_text) and probable_editor_context:
                action_name = "rename_symbol"
            elif re.search(r"\b(?:toggle|open)\s+terminal\b", lowered_text) and probable_editor_context:
                action_name = "toggle_terminal"
            elif re.search(r"\bformat\s+(?:document|file|code)\b", lowered_text) and probable_editor_context:
                action_name = "format_document"
            elif any(token in lowered_text for token in ("quick open", "switch to file", "go to file")):
                action_name = "quick_open"
            elif "open file " in lowered_text and probable_editor_context:
                action_name = "quick_open"
            elif probable_browser_context and tab_search_query and re.search(r"\btab\b", lowered_text):
                action_name = "search_tabs"
            elif tab_target and bool(app_name or window_title):
                action_name = "switch_tab"
            elif any(token in lowered_text for token in ("open new tab", "new tab")) and bool(app_name or window_title):
                action_name = "new_tab"
            elif any(token in lowered_text for token in ("reopen tab", "restore tab", "restore closed tab")) and bool(app_name or window_title):
                action_name = "reopen_tab"
            elif "close tab" in lowered_text and bool(app_name or window_title):
                action_name = "close_tab"
            elif re.search(r"\b(?:reset zoom|zoom reset|actual size|normal size)\b", lowered_text) and bool(app_name or window_title):
                action_name = "reset_zoom"
            elif re.search(r"\bzoom in\b", lowered_text) and bool(app_name or window_title):
                action_name = "zoom_in"
            elif re.search(r"\bzoom out\b", lowered_text) and bool(app_name or window_title):
                action_name = "zoom_out"
            elif probable_terminal_context and re.search(r"\b(?:run|execute)\b", lowered_text):
                action_name = "terminal_command"
            elif bool(navigation_target) and bool(app_name or window_title):
                action_name = "navigate"
            elif any(token in lowered_text for token in ("navigate to", "go to ", "browse to")) and bool(app_name or window_title):
                action_name = "navigate"
            elif any(token in lowered_text for token in ("search for ", "find ")) and bool(app_name or window_title):
                action_name = "search"
            elif any(token in lowered_text for token in ("press key", "hotkey", "shortcut")) or hotkey_keys:
                action_name = "hotkey"
            elif ("type " in lowered_text or " type" in lowered_text) and "type of" not in lowered_text:
                action_name = "type"
            elif any(token in lowered_text for token in ("click ", "press button", "click button", "select target")):
                action_name = "click"
            elif app_name and not used_inherited_target_context:
                action_name = "launch"

        if action_name == "navigate" and not query:
            query = navigation_target
        if action_name == "search" and not query:
            query = self._extract_desktop_search_query(text)
        if action_name == "command" and not typed_text:
            typed_text = self._extract_desktop_command_text(text)
        if action_name == "quick_open" and not query:
            query = self._extract_desktop_quick_open_query(text)
        if action_name == "switch_tab" and not query:
            query = tab_target
        if action_name == "search_tabs" and not query:
            query = tab_search_query
        if action_name == "jump_to_conversation" and not query:
            query = self._extract_desktop_conversation_query(text)
        if action_name == "send_message" and not query:
            query = self._extract_desktop_conversation_query(text)
        if action_name == "send_message" and not typed_text:
            typed_text = self._extract_desktop_message_text(text)
        if action_name == "select_sidebar_item" and not query:
            query = sidebar_item_query
        if action_name == "focus_input_field" and not query:
            query = field_query
        if action_name == "set_field_value":
            if not query:
                query = field_query
            if not typed_text:
                typed_text = field_value
        if action_name == "set_value_control":
            if not query:
                query = value_control_query
            if not typed_text:
                typed_text = value_control_target
        if action_name == "open_dropdown" and not query:
            query = dropdown_query
        if action_name == "select_dropdown_option":
            if not query:
                query = dropdown_query
            if not typed_text:
                typed_text = dropdown_option
        if action_name == "focus_checkbox" and not query:
            query = checkbox_query
        if action_name == "check_checkbox" and not query:
            query = checkbox_query
        if action_name == "uncheck_checkbox" and not query:
            query = checkbox_query
        if action_name == "select_radio_option" and not query:
            query = radio_option_query
        if action_name in {"focus_value_control", "increase_value", "decrease_value"} and not query:
            query = value_control_query
        if action_name == "select_tab_page" and not query:
            query = tab_page_query
        if action_name == "toggle_switch" and not query:
            query = toggle_query
        if action_name == "enable_switch" and not query:
            query = toggle_query
        if action_name == "disable_switch" and not query:
            query = toggle_query
        if action_name in {"select_tree_item", "expand_tree_item"} and not query:
            query = tree_item_query
        if action_name == "select_list_item" and not query:
            query = list_item_query
        if action_name == "select_table_row" and not query:
            query = table_row_query
        if action_name == "workspace_search" and not query:
            query = self._extract_desktop_workspace_search_query(text)
        if action_name == "find_replace" and (not query or not typed_text):
            replace_query, replace_text = self._extract_desktop_replace_terms(text, app_name=app_name)
            if not query:
                query = replace_query
            if not typed_text:
                typed_text = replace_text
        if action_name == "invoke_toolbar_action" and not query:
            query = toolbar_action_query
        if action_name == "rename_selection" and not typed_text:
            typed_text = self._extract_desktop_selection_rename_text(text)
        if action_name == "select_context_menu_item" and not query:
            query = context_menu_item_query
        if action_name == "press_dialog_button" and not query:
            query = dialog_button_query
        if action_name == "go_to_symbol" and not query:
            query = self._extract_desktop_symbol_query(text)
        if action_name == "rename_symbol" and not typed_text:
            typed_text = self._extract_desktop_rename_text(text)
        if action_name == "terminal_command" and not typed_text:
            typed_text = self._extract_desktop_terminal_command_text(text, permissive=probable_terminal_context)
        if action_name == "type" and not typed_text:
            typed_text = self._extract_clipboard_text(text)
        if action_name in {"click", "click_and_type"} and not query:
            query = self._extract_phrase(text) or self._extract_keyword(text)
        if action_name == "hotkey" and not hotkey_keys:
            hotkey_keys = self._extract_hotkey_keys(text)
        if action_name == "command" and not typed_text:
            typed_text = query
        if action_name == "quick_open" and not query:
            query = typed_text
        if action_name == "jump_to_conversation" and not query:
            query = typed_text
        if action_name == "workspace_search" and not query:
            query = typed_text
        if action_name == "go_to_symbol" and not query:
            query = typed_text
        if action_name == "rename_symbol" and not typed_text:
            typed_text = query
        if action_name == "terminal_command" and not typed_text:
            typed_text = query

        if action_name == "type" and query and typed_text:
            action_name = "click_and_type"

        has_target_context = bool(app_name or window_title)
        if require_target_context and not has_target_context:
            return None

        if action_name not in {"launch", "click", "type", "click_and_type", "hotkey", "navigate", "search", "focus_search_box", "command", "quick_open", "new_chat", "jump_to_conversation", "send_message", "new_email_draft", "reply_email", "reply_all_email", "forward_email", "new_calendar_event", "open_mail_view", "open_calendar_view", "open_people_view", "open_tasks_view", "focus_folder_pane", "focus_message_list", "focus_reading_pane", "focus_navigation_tree", "focus_list_surface", "focus_data_table", "select_tree_item", "expand_tree_item", "select_list_item", "select_table_row", "focus_sidebar", "select_sidebar_item", "focus_main_content", "focus_toolbar", "invoke_toolbar_action", "focus_form_surface", "focus_input_field", "set_field_value", "set_value_control", "open_dropdown", "select_dropdown_option", "focus_checkbox", "check_checkbox", "uncheck_checkbox", "enable_switch", "disable_switch", "select_radio_option", "select_tab_page", "complete_form_page", "complete_form_flow", "focus_value_control", "increase_value", "decrease_value", "toggle_switch", "open_context_menu", "select_context_menu_item", "dismiss_dialog", "confirm_dialog", "press_dialog_button", "next_wizard_step", "previous_wizard_step", "finish_wizard", "complete_wizard_page", "complete_wizard_flow", "new_document", "save_document", "open_print_dialog", "start_presentation", "play_pause_media", "pause_media", "resume_media", "next_track", "previous_track", "stop_media", "focus_address_bar", "open_bookmarks", "new_folder", "focus_folder_tree", "focus_file_list", "rename_selection", "open_properties_dialog", "open_preview_pane", "open_details_pane", "refresh_view", "go_back", "go_forward", "go_up_level", "focus_explorer", "workspace_search", "find_replace", "go_to_symbol", "rename_symbol", "new_tab", "switch_tab", "close_tab", "reopen_tab", "open_history", "open_downloads", "open_devtools", "open_tab_search", "search_tabs", "toggle_terminal", "format_document", "zoom_in", "zoom_out", "reset_zoom", "terminal_command"}:
            return None
        if action_name == "launch" and not app_name:
            return None
        if action_name in {"navigate", "search", "quick_open", "switch_tab", "jump_to_conversation", "select_tree_item", "expand_tree_item", "select_list_item", "select_table_row", "select_sidebar_item", "invoke_toolbar_action", "focus_input_field", "set_field_value", "set_value_control", "open_dropdown", "select_dropdown_option", "focus_checkbox", "check_checkbox", "uncheck_checkbox", "enable_switch", "disable_switch", "select_radio_option", "select_tab_page", "focus_value_control", "increase_value", "decrease_value", "toggle_switch", "select_context_menu_item", "press_dialog_button", "workspace_search", "go_to_symbol", "search_tabs"} and not query:
            return None
        if action_name in {"click", "click_and_type"} and not query:
            return None
        if action_name == "find_replace" and (not query or not typed_text):
            return None
        if action_name in {"type", "click_and_type", "command", "set_field_value", "set_value_control", "select_dropdown_option", "rename_selection", "rename_symbol", "send_message", "terminal_command"} and not typed_text:
            return None
        if action_name == "hotkey" and not hotkey_keys:
            return None

        step_args: Dict[str, Any] = {
            "action": action_name,
            "focus_first": True,
            "ensure_app_launch": bool(app_name),
        }
        if app_name:
            step_args["app_name"] = app_name
        if window_title:
            step_args["window_title"] = window_title
        if query:
            step_args["query"] = query
        if typed_text:
            step_args["text"] = typed_text
        if action_name in {"increase_value", "decrease_value"}:
            step_args["amount"] = max(1, int(adjust_amount or 1))
        if hotkey_keys:
            step_args["keys"] = hotkey_keys
        if (
            bool(args.get("press_enter"))
            or "press enter" in lowered_text
            or "hit enter" in lowered_text
            or "submit" in lowered_text
            or action_name in {"navigate", "command", "quick_open", "jump_to_conversation", "rename_selection", "rename_symbol", "send_message", "terminal_command"}
        ):
            step_args["press_enter"] = True

        return self._step("desktop_interact", args=step_args, verify={"expect_status": "success"})

    def _build_primary_steps(
        self,
        text: str,
        lowered: str,
        allow_compound: bool = True,
        desktop_context: Optional[Dict[str, Any]] = None,
    ) -> tuple[str, List[PlanStep]]:
        chained_desktop_interact = self._build_desktop_interact_step(
            original_text=text,
            require_target_context=True,
            desktop_context=desktop_context,
        )
        if chained_desktop_interact is not None and chained_desktop_interact.args.get("action") != "launch":
            compound_stateful_actions = {
                "set_field_value",
                "set_value_control",
                "open_dropdown",
                "select_dropdown_option",
                "focus_checkbox",
                "check_checkbox",
                "uncheck_checkbox",
                "enable_switch",
                "disable_switch",
                "select_radio_option",
                "select_tab_page",
                "complete_form_page",
                "complete_form_flow",
                "focus_value_control",
                "increase_value",
                "decrease_value",
                "toggle_switch",
                "open_context_menu",
                "select_context_menu_item",
                "dismiss_dialog",
                "confirm_dialog",
                "press_dialog_button",
                "next_wizard_step",
                "previous_wizard_step",
                "finish_wizard",
                "complete_wizard_page",
                "complete_wizard_flow",
            }
            chained_action = str(chained_desktop_interact.args.get("action") or "").strip()
            has_open_prefix = bool(re.match(r"^\s*(?:open|launch|start)\b", text, flags=re.IGNORECASE))
            has_clause_separator = any(marker in lowered for marker in (" and ", " then ", " after ", " next ", ";"))
            preserve_compound_open_chain = (
                allow_compound
                and has_open_prefix
                and has_clause_separator
                and chained_action in compound_stateful_actions
            )
            if (
                chained_action in {"navigate", "search", "focus_search_box", "command", "quick_open", "new_chat", "jump_to_conversation", "send_message", "new_email_draft", "reply_email", "reply_all_email", "forward_email", "new_calendar_event", "open_mail_view", "open_calendar_view", "open_people_view", "open_tasks_view", "focus_folder_pane", "focus_message_list", "focus_reading_pane", "focus_navigation_tree", "focus_list_surface", "focus_data_table", "select_tree_item", "expand_tree_item", "select_list_item", "select_table_row", "focus_sidebar", "select_sidebar_item", "focus_main_content", "focus_toolbar", "invoke_toolbar_action", "focus_form_surface", "focus_input_field", "set_field_value", "set_value_control", "open_dropdown", "select_dropdown_option", "focus_checkbox", "check_checkbox", "uncheck_checkbox", "enable_switch", "disable_switch", "select_radio_option", "select_tab_page", "complete_form_page", "complete_form_flow", "focus_value_control", "increase_value", "decrease_value", "toggle_switch", "open_context_menu", "select_context_menu_item", "dismiss_dialog", "confirm_dialog", "press_dialog_button", "next_wizard_step", "previous_wizard_step", "finish_wizard", "complete_wizard_page", "complete_wizard_flow", "new_document", "save_document", "open_print_dialog", "start_presentation", "play_pause_media", "pause_media", "resume_media", "next_track", "previous_track", "stop_media", "focus_address_bar", "open_bookmarks", "new_folder", "focus_folder_tree", "focus_file_list", "rename_selection", "open_properties_dialog", "open_preview_pane", "open_details_pane", "refresh_view", "go_back", "go_forward", "go_up_level", "focus_explorer", "workspace_search", "find_replace", "go_to_symbol", "rename_symbol", "new_tab", "switch_tab", "close_tab", "reopen_tab", "open_history", "open_downloads", "open_devtools", "open_tab_search", "search_tabs", "toggle_terminal", "format_document", "zoom_in", "zoom_out", "reset_zoom", "terminal_command"}
                or has_clause_separator
            ) and not preserve_compound_open_chain:
                return ("desktop_interact", [chained_desktop_interact])

        if allow_compound:
            compound = self._build_compound_steps(text)
            if compound is not None:
                return compound

        file_intent_markers = (
            "read file",
            "show file",
            "write file",
            "save file",
            "create file",
            "copy file",
            "duplicate file",
            "copy and hash",
            "copy then hash",
            "backup file",
            "backup and hash",
            "backup then hash",
            "hash file",
            "checksum",
            "search files",
            "find file",
            "locate file",
            "search text",
            "find text",
            "contains text",
            "scan directory",
            "scan folder",
            "index folder",
            "list folder",
            "list directory",
            "show folder",
            "show directory",
            "create folder",
            "make folder",
            "new folder",
            "folder size",
            "directory size",
        )
        is_file_intent = any(marker in lowered for marker in file_intent_markers)
        is_email_intent = any(marker in lowered for marker in ("send email", "email to", "draft email"))

        url = self._extract_url(text)
        domain_like = self._extract_domain_like(text)
        if any(token in lowered for token in ("read webpage", "summarize webpage", "analyze webpage", "read page content", "page text")) and not is_file_intent and not is_email_intent:
            target_url = url or domain_like
            if target_url:
                return (
                    "browser_read_dom",
                    [self._step("browser_read_dom", args={"url": target_url, "max_chars": 7000}, verify={"expect_status": "success"})],
                )

        if any(token in lowered for token in ("extract links", "list links", "page links", "get links from")) and not is_file_intent and not is_email_intent:
            target_url = url or domain_like
            if target_url:
                return (
                    "browser_extract_links",
                    [self._step("browser_extract_links", args={"url": target_url, "max_links": 80}, verify={"expect_status": "success"})],
                )

        if any(token in lowered for token in ("create browser session", "new browser session", "open browser session", "authenticated browser session")):
            args: Dict[str, Any] = {"name": "jarvis-session"}
            target_url = url or domain_like
            if target_url:
                args["base_url"] = target_url
            if "google" in lowered:
                args["oauth_provider"] = "google"
            elif "graph" in lowered or "microsoft" in lowered:
                args["oauth_provider"] = "graph"
            return ("browser_session_create", [self._step("browser_session_create", args=args, verify={"expect_status": "success"})])

        if any(token in lowered for token in ("list browser sessions", "show browser sessions", "session list")):
            return ("browser_session_list", [self._step("browser_session_list", args={}, verify={"expect_status": "success"})])

        if any(token in lowered for token in ("close browser session", "end browser session", "delete browser session")):
            session_id = self._extract_session_id(text)
            if session_id:
                return (
                    "browser_session_close",
                    [self._step("browser_session_close", args={"session_id": session_id}, verify={"expect_status": "success"})],
                )

        if any(token in lowered for token in ("session read webpage", "browser session read", "session dom read")):
            session_id = self._extract_session_id(text)
            target_url = url or domain_like
            if session_id and target_url:
                return (
                    "browser_session_read_dom",
                    [
                        self._step(
                            "browser_session_read_dom",
                            args={"session_id": session_id, "url": target_url, "max_chars": 7000},
                            verify={"expect_status": "success"},
                        )
                    ],
                )

        if any(token in lowered for token in ("session extract links", "browser session links")):
            session_id = self._extract_session_id(text)
            target_url = url or domain_like
            if session_id and target_url:
                return (
                    "browser_session_extract_links",
                    [
                        self._step(
                            "browser_session_extract_links",
                            args={"session_id": session_id, "url": target_url, "max_links": 80},
                            verify={"expect_status": "success"},
                        )
                    ],
                )

        if any(token in lowered for token in ("session request", "browser session request", "authenticated request")):
            session_id = self._extract_session_id(text)
            target_url = url or domain_like
            if session_id and target_url:
                method = "GET"
                if " post " in f" {lowered} ":
                    method = "POST"
                elif " put " in f" {lowered} ":
                    method = "PUT"
                elif " delete " in f" {lowered} ":
                    method = "DELETE"
                return (
                    "browser_session_request",
                    [
                        self._step(
                            "browser_session_request",
                            args={"session_id": session_id, "url": target_url, "method": method},
                            verify={"expect_status": "success"},
                        )
                    ],
                )

        if (url or domain_like or any(token in lowered for token in ("open website", "open url", "go to ", "browse "))) and not is_file_intent and not is_email_intent:
            chosen_url = url or domain_like
            if chosen_url:
                return ("open_url", [self._step("open_url", args={"url": chosen_url}, verify={"expect_status": "success", "expect_key": "url"})])

        if any(token in lowered for token in ("open ", "launch ", "start app", "run app")) and not any(
            marker in lowered
            for marker in ("open folder", "open directory", "open in explorer", "show folder in explorer")
        ):
            app_name = self._extract_app_name(text)
            return (
                "open_application",
                [
                    self._step("open_app", args={"app_name": app_name}, verify={"expect_status": "success"}, max_retries=2),
                    self._step("tts_speak", args={"text": f"Launching {app_name}."}, can_retry=False, verify={"optional": True}),
                ],
            )

        if any(token in lowered for token in ("security", "defender", "virus", "threat")):
            return ("check_security", [self._step("defender_status", args={}, verify={"expect_key": "status", "expect_status": "success"})])

        if any(token in lowered for token in ("next track", "skip", "next song")):
            return ("media_next", [self._step("media_next", args={}, verify={"expect_status": "success"})])
        if any(token in lowered for token in ("previous track", "prev track", "last track", "previous song")):
            return ("media_previous", [self._step("media_previous", args={}, verify={"expect_status": "success"})])
        if any(token in lowered for token in ("pause", "hold music", "pause music")):
            return ("media_pause", [self._step("media_pause", args={}, verify={"expect_status": "success"})])
        if any(token in lowered for token in ("resume", "continue music", "playback")):
            return ("media_play", [self._step("media_play", args={}, verify={"expect_status": "success"})])
        if any(token in lowered for token in ("stop music", "stop media", "stop playback")):
            return ("media_stop", [self._step("media_stop", args={}, verify={"expect_status": "success"})])

        if any(token in lowered for token in ("play", "search", "youtube", "song", "music")):
            query = self._extract_media_query(text)
            return ("search_media", [self._step("media_search", args={"query": query}, verify={"expect_status": "success", "expect_key": "url"})])

        if any(token in lowered for token in ("metrics", "system status", "cpu", "ram", "disk usage", "resource usage", "performance")):
            return ("system_snapshot", [self._step("system_snapshot", args={}, verify={"expect_status": "success", "expect_key": "metrics"})])

        if any(
            token in lowered
            for token in (
                "connector preflight",
                "preflight connector",
                "preflight check connector",
                "connector contract check",
                "connector diagnostic preflight",
            )
        ):
            preflight_action = "external_email_send"
            if any(token in lowered for token in ("calendar", "event")):
                preflight_action = "external_calendar_create_event"
            elif any(token in lowered for token in ("document", "doc", "drive")):
                preflight_action = "external_doc_create"
            elif any(token in lowered for token in ("task", "todo")):
                preflight_action = "external_task_create"
            return (
                "external_connector_preflight",
                [
                    self._step(
                        "external_connector_preflight",
                        args={"action": preflight_action, "provider": "auto"},
                        verify={"expect_key": "contract_diagnostic"},
                    )
                ],
            )

        if any(token in lowered for token in ("connector status", "integration status", "email connector status", "cloud connector status")):
            return ("external_connector_status", [self._step("external_connector_status", args={}, verify={"expect_status": "success"})])

        if any(token in lowered for token in ("list emails", "show emails", "inbox messages", "email list", "mailbox list")):
            args: Dict[str, Any] = {"provider": "auto", "max_results": 20}
            quoted = self._extract_quoted(text)
            if quoted:
                args["query"] = quoted
            return ("external_email_list", [self._step("external_email_list", args=args, verify={"expect_status": "success"})])

        if any(token in lowered for token in ("read email", "open email", "show email message", "email message id")):
            message_id = self._extract_message_id(text)
            if message_id:
                return (
                    "external_email_read",
                    [self._step("external_email_read", args={"message_id": message_id, "provider": "auto"}, verify={"expect_status": "success"})],
                )

        if any(token in lowered for token in ("list calendar events", "show calendar events", "upcoming events", "calendar agenda")):
            return (
                "external_calendar_list_events",
                [self._step("external_calendar_list_events", args={"provider": "auto", "max_results": 20}, verify={"expect_status": "success"})],
            )

        if any(token in lowered for token in ("list tasks", "show tasks", "todo list", "to do list", "list todo")):
            include_completed = not any(token in lowered for token in ("open tasks", "pending tasks", "incomplete tasks"))
            args: Dict[str, Any] = {"provider": "auto", "max_results": 25, "include_completed": include_completed}
            quoted = self._extract_quoted(text)
            if quoted:
                args["query"] = quoted
            return ("external_task_list", [self._step("external_task_list", args=args, verify={"expect_status": "success"})])

        if any(token in lowered for token in ("create task", "add task", "new task", "todo add", "to do add")):
            title = self._extract_task_title(text)
            if title:
                args = {"title": title, "provider": "auto"}
                notes = self._extract_optional_content(text)
                if notes:
                    args["notes"] = notes
                due_match = re.search(r"\bdue\s*[:=]?\s*([0-9]{4}-[0-9]{2}-[0-9]{2}(?:[T ][0-9:.+-Z]+)?)", text, flags=re.IGNORECASE)
                if due_match:
                    args["due"] = due_match.group(1).strip()
                status = self._extract_task_status(text)
                if status:
                    args["status"] = status
                return ("external_task_create", [self._step("external_task_create", args=args, verify={"expect_status": "success"})])

        if any(token in lowered for token in ("update task", "complete task", "mark task done", "task done", "finish task")):
            task_id = self._extract_task_id(text)
            if task_id:
                args: Dict[str, Any] = {"task_id": task_id, "provider": "auto"}
                title_match = re.search(r"title\s*[:=]\s*(.+?)(?:\s+(?:notes|content|due|status)\s*[:=]|$)", text, flags=re.IGNORECASE)
                if title_match:
                    explicit_title = title_match.group(1).strip().strip(".")
                    if explicit_title:
                        args["title"] = explicit_title
                notes_match = re.search(r"(?:notes|content)\s*[:=]\s*(.+?)(?:\s+(?:title|due|status)\s*[:=]|$)", text, flags=re.IGNORECASE)
                if notes_match:
                    explicit_notes = notes_match.group(1).strip()
                    if explicit_notes:
                        args["notes"] = explicit_notes
                due_match = re.search(r"\bdue\s*[:=]?\s*([0-9]{4}-[0-9]{2}-[0-9]{2}(?:[T ][0-9:.+-Z]+)?)", text, flags=re.IGNORECASE)
                if due_match:
                    args["due"] = due_match.group(1).strip()
                status = self._extract_task_status(text)
                if status:
                    args["status"] = status
                if len(args) <= 2:
                    return ("speak", [self._step("tts_speak", args={"text": "Provide at least one task field to update, such as status, title, notes, or due date."}, can_retry=False, verify={"optional": True})])
                return ("external_task_update", [self._step("external_task_update", args=args, verify={"expect_status": "success"})])

        if any(
            token in lowered
            for token in (
                "maintain oauth",
                "maintain oauth tokens",
                "refresh oauth tokens",
                "refresh all tokens",
                "rotate oauth tokens",
                "token maintenance",
            )
        ):
            args: Dict[str, Any] = {}
            if "google" in lowered or "gmail" in lowered:
                args["provider"] = "google"
            elif any(token in lowered for token in ("graph", "microsoft", "outlook", "office 365")):
                args["provider"] = "graph"
            return ("oauth_token_maintain", [self._step("oauth_token_maintain", args=args, verify={"expect_status": "success"})])

        if any(token in lowered for token in ("list processes", "running processes", "running apps")):
            return (
                "list_processes",
                [
                    self._step(
                        "list_processes",
                        args={"limit": 40},
                        verify={"expect_status": "success", "expect_key": "processes", "expect_numeric_min": {"count": 1}},
                    )
                ],
            )

        if any(token in lowered for token in ("active window", "foreground window")):
            return ("active_window", [self._step("active_window", args={}, verify={"expect_status": "success", "expect_key": "window"})])

        if any(token in lowered for token in ("list windows", "open windows")):
            return ("list_windows", [self._step("list_windows", args={"limit": 80}, verify={"expect_status": "success", "expect_key": "windows"})])

        desktop_interact_step = self._build_desktop_interact_step(
            original_text=text,
            require_target_context=True,
        )
        if desktop_interact_step is not None:
            if desktop_interact_step.args.get("action") != "launch":
                return ("desktop_interact", [desktop_interact_step])

        if any(token in lowered for token in ("accessibility status", "ui automation status", "ui status")):
            return ("accessibility_status", [self._step("accessibility_status", args={}, verify={"expect_status": "success"})])

        if any(token in lowered for token in ("list ui elements", "show ui elements", "inspect ui tree", "read ui tree")):
            phrase = self._extract_phrase(text)
            args: Dict[str, Any] = {"max_elements": 120}
            if phrase:
                args["query"] = phrase
            return ("accessibility_list_elements", [self._step("accessibility_list_elements", args=args, verify={"expect_status": "success"})])

        if any(token in lowered for token in ("find ui element", "locate ui element", "find button", "find control")):
            phrase = self._extract_phrase(text) or self._extract_keyword(text)
            if phrase:
                return (
                    "accessibility_find_element",
                    [self._step("accessibility_find_element", args={"query": phrase}, verify={"expect_status": "success"})],
                )

        if any(token in lowered for token in ("click ui element", "invoke ui element", "activate ui element", "click button")):
            phrase = self._extract_phrase(text) or self._extract_keyword(text)
            if phrase:
                return (
                    "accessibility_invoke_element",
                    [
                        self._step(
                            "accessibility_invoke_element",
                            args={"query": phrase, "action": "click"},
                            verify={"expect_status": "success"},
                            max_retries=2,
                        )
                    ],
                )

        if any(token in lowered for token in ("focus window", "switch to window", "bring window")):
            title = self._extract_window_title(text)
            if title:
                return ("focus_window", [self._step("focus_window", args={"title": title}, verify={"expect_status": "success", "expect_key": "window"})])

        if any(token in lowered for token in ("notify", "notification", "remind me")):
            message = self._extract_notification_message(text)
            return ("notification", [self._step("send_notification", args={"title": "JARVIS", "message": message}, verify={"expect_status": "success"})])

        if any(token in lowered for token in ("copy to clipboard", "set clipboard", "clipboard write", "clipboard copy")):
            clip_text = self._extract_clipboard_text(text) or text.strip()
            return ("clipboard_write", [self._step("clipboard_write", args={"text": clip_text}, verify={"expect_status": "success"})])

        if any(token in lowered for token in ("read clipboard", "show clipboard", "clipboard content", "pasteboard")):
            return ("clipboard_read", [self._step("clipboard_read", args={}, verify={"expect_status": "success", "expect_key": "text"})])

        if any(token in lowered for token in ("hotkey", "shortcut", "press key")):
            keys = self._extract_hotkey_keys(text)
            if keys:
                return ("keyboard_hotkey", [self._step("keyboard_hotkey", args={"keys": keys}, verify={"expect_status": "success"})])

        if any(token in lowered for token in ("type text", "type this", "type ")) and "type of" not in lowered:
            typed = self._extract_clipboard_text(text)
            if typed:
                return ("keyboard_type", [self._step("keyboard_type", args={"text": typed}, verify={"expect_status": "success"})])

        if any(token in lowered for token in ("move mouse", "move cursor")):
            x, y = self._extract_coordinates(text)
            if x is not None and y is not None:
                return ("mouse_move", [self._step("mouse_move", args={"x": x, "y": y, "duration": 0.1}, verify={"expect_status": "success"})])

        if any(
            token in lowered
            for token in (
                "click text",
                "click on text",
                "select text on screen",
                "press text",
                "click button",
                "press button",
                "click target",
                "select target on screen",
            )
        ):
            phrase = self._extract_phrase(text) or self._extract_keyword(text)
            if phrase:
                return (
                    "computer_click_target",
                    [
                        self._step(
                            "computer_click_target",
                            args={"query": phrase, "target_mode": "auto", "verify_mode": "state_or_visibility"},
                            verify={"expect_status": "success"},
                            max_retries=2,
                        )
                    ],
                )

        if any(token in lowered for token in ("click mouse", "mouse click", "left click", "right click")):
            x, y = self._extract_coordinates(text)
            button = "right" if "right click" in lowered else "left"
            args: Dict[str, Any] = {"button": button, "clicks": 1}
            if x is not None and y is not None:
                args.update({"x": x, "y": y})
            return ("mouse_click", [self._step("mouse_click", args=args, verify={"expect_status": "success"})])

        if any(token in lowered for token in ("scroll up", "scroll down", "mouse scroll")):
            amount = 600
            if "down" in lowered:
                amount = -600
            return ("mouse_scroll", [self._step("mouse_scroll", args={"amount": amount}, verify={"expect_status": "success"})])

        if any(token in lowered for token in ("take screenshot", "capture screen", "screenshot")):
            target = self._extract_path(text) or str(Path.home() / "Pictures" / "jarvis_capture.png")
            return ("screenshot_capture", [self._step("screenshot_capture", args={"path": target}, verify={"expect_status": "success", "expect_key": "path"})])

        if any(token in lowered for token in ("observe screen", "what is on screen", "inspect screen", "analyze screen")):
            return ("computer_observe", [self._step("computer_observe", args={}, verify={"expect_status": "success", "expect_key": "screenshot_path"})])

        if any(token in lowered for token in ("is text visible", "find text on screen", "screen contains text")):
            phrase = self._extract_phrase(text)
            if phrase:
                return (
                    "computer_assert_text_visible",
                    [
                        self._step(
                            "computer_assert_text_visible",
                            args={"text": phrase},
                            verify={"expect_status": "success", "expect_key": "found"},
                        )
                    ],
                )

        if any(token in lowered for token in ("find text targets", "locate text target", "detect text target")):
            phrase = self._extract_phrase(text) or self._extract_keyword(text)
            if phrase:
                return (
                    "computer_find_text_targets",
                    [
                        self._step(
                            "computer_find_text_targets",
                            args={"query": phrase, "match_mode": "contains", "limit": 20},
                            verify={"expect_status": "success"},
                        )
                    ],
                )

        if any(token in lowered for token in ("wait for text", "wait until text", "wait until appears", "wait until disappears", "wait until gone")):
            phrase = self._extract_phrase(text) or self._extract_keyword(text)
            if phrase:
                expect_visible = not any(token in lowered for token in ("disappear", "gone", "not visible", "vanish"))
                return (
                    "computer_wait_for_text",
                    [
                        self._step(
                            "computer_wait_for_text",
                            args={"text": phrase, "expect_visible": expect_visible, "timeout_s": 12.0},
                            verify={"expect_status": "success"},
                            max_retries=1,
                        )
                    ],
                )

        if any(token in lowered for token in ("ocr image", "read image text", "extract text from image")):
            target = self._extract_path(text)
            if target:
                return ("extract_text_from_image", [self._step("extract_text_from_image", args={"path": target}, verify={"expect_status": "success", "expect_key": "text"})])

        if any(token in lowered for token in ("send email", "email to", "draft email")):
            recipients = self._extract_email_addresses(text)
            if recipients:
                subject = self._extract_email_subject(text)
                body = self._extract_email_body(text)
                return (
                    "external_email_send",
                    [
                        self._step(
                            "external_email_send",
                            args={"to": recipients, "subject": subject, "body": body, "provider": "auto"},
                            verify={"expect_status": "success"},
                            max_retries=2,
                        )
                    ],
                )

        if any(token in lowered for token in ("update calendar event", "edit calendar event", "reschedule event")):
            event_id = self._extract_event_id(text)
            if event_id:
                args: Dict[str, Any] = {"event_id": event_id, "provider": "auto"}
                title_match = re.search(
                    r"title\s*[:=]\s*(.+?)(?:\s+(?:description|start|end|timezone)\s*[:=]|$)",
                    text,
                    flags=re.IGNORECASE,
                )
                if title_match:
                    explicit_title = title_match.group(1).strip().strip(".")
                    if explicit_title:
                        args["title"] = explicit_title
                description_match = re.search(
                    r"description\s*[:=]\s*(.+?)(?:\s+(?:title|start|end|timezone)\s*[:=]|$)",
                    text,
                    flags=re.IGNORECASE,
                )
                if description_match:
                    explicit_description = description_match.group(1).strip()
                    if explicit_description:
                        args["description"] = explicit_description
                start_iso, end_iso = self._extract_datetime_window(text)
                if start_iso:
                    args["start"] = start_iso
                if end_iso:
                    args["end"] = end_iso
                if len(args) > 2:
                    return (
                        "external_calendar_update_event",
                        [self._step("external_calendar_update_event", args=args, verify={"expect_status": "success"}, max_retries=2)],
                    )

        if any(token in lowered for token in ("create calendar event", "schedule meeting", "add to calendar", "calendar event")):
            title = self._extract_calendar_title(text)
            start_iso, end_iso = self._extract_datetime_window(text)
            args: Dict[str, Any] = {"title": title, "provider": "auto"}
            if start_iso:
                args["start"] = start_iso
            if end_iso:
                args["end"] = end_iso
            return (
                "external_calendar_create_event",
                [
                    self._step(
                        "external_calendar_create_event",
                        args=args,
                        verify={"expect_status": "success"},
                        max_retries=2,
                        )
                    ],
                )

        if any(token in lowered for token in ("list documents", "show documents", "documents list", "docs list", "list cloud docs")):
            args: Dict[str, Any] = {"provider": "auto", "max_results": 20}
            quoted = self._extract_quoted(text)
            if quoted:
                args["query"] = quoted
            return ("external_doc_list", [self._step("external_doc_list", args=args, verify={"expect_status": "success"})])

        if any(token in lowered for token in ("read document", "open document", "show document content")):
            document_id = self._extract_document_id(text)
            if document_id:
                return (
                    "external_doc_read",
                    [self._step("external_doc_read", args={"document_id": document_id, "provider": "auto"}, verify={"expect_status": "success"})],
                )

        if any(token in lowered for token in ("update document", "edit document", "revise document", "append to document")):
            document_id = self._extract_document_id(text)
            if document_id:
                args: Dict[str, Any] = {"document_id": document_id, "provider": "auto"}
                title_match = re.search(r"title\s*[:=]\s*(.+?)(?:\s+content\s*[:=]|$)", text, flags=re.IGNORECASE)
                if title_match:
                    title = title_match.group(1).strip().strip(".")
                else:
                    title = ""
                content = self._extract_optional_content(text)
                if title and title.lower() != document_id.lower():
                    args["title"] = title
                if content:
                    args["content"] = content
                if len(args) > 2:
                    return ("external_doc_update", [self._step("external_doc_update", args=args, verify={"expect_status": "success"}, max_retries=2)])

        if any(token in lowered for token in ("create document", "new document", "create doc", "write doc", "create note in docs")):
            title = self._extract_document_title(text)
            content = self._extract_content(text)
            return (
                "external_doc_create",
                [
                    self._step(
                        "external_doc_create",
                        args={"title": title, "content": content, "provider": "auto"},
                        verify={"expect_status": "success"},
                        max_retries=2,
                    )
                ],
            )

        if any(token in lowered for token in ("run trusted script", "execute trusted script")):
            script_name = self._extract_script_name(text)
            if script_name:
                return ("run_trusted_script", [self._step("run_trusted_script", args={"script_name": script_name}, verify={"expect_status": "success"}, max_retries=1)])

        if any(token in lowered for token in ("run whitelisted app", "launch whitelisted app")):
            app_name = self._extract_app_name(text)
            if app_name:
                return ("run_whitelisted_app", [self._step("run_whitelisted_app", args={"app_name": app_name}, verify={"expect_status": "success"})])

        if any(token in lowered for token in ("kill process", "terminate process", "end process")):
            proc_name = self._extract_process_name(text)
            if proc_name:
                return (
                    "terminate_process",
                    [self._step("terminate_process", args={"name": proc_name, "max_count": 2}, verify={"expect_status": "success", "expect_numeric_min": {"count": 1}}, max_retries=1)],
                )

        if any(token in lowered for token in ("search files", "find file", "locate file")):
            pattern = self._extract_file_pattern(text) or "*"
            base_dir = self._extract_path(text) or str(Path.home())
            return ("search_files", [self._step("search_files", args={"base_dir": base_dir, "pattern": pattern, "max_results": 120}, verify={"expect_status": "success", "expect_key": "results"})])

        if any(token in lowered for token in ("search text", "find text", "contains text", "grep")):
            keyword = self._extract_keyword(text) or text
            base_dir = self._extract_path(text) or str(Path.home())
            return ("search_text", [self._step("search_text", args={"base_dir": base_dir, "keyword": keyword, "max_results": 120}, verify={"expect_status": "success", "expect_key": "results"})])

        if any(token in lowered for token in ("scan directory", "scan folder", "index folder")):
            folder = self._extract_path(text) or str(Path.home())
            return ("scan_directory", [self._step("scan_directory", args={"path": folder, "max_results": 500}, verify={"expect_status": "success", "expect_key": "results"})])

        if any(token in lowered for token in ("open folder", "open directory", "open in explorer", "show folder in explorer")):
            folder = self._extract_path(text) or str(Path.home())
            return ("explorer_open_path", [self._step("explorer_open_path", args={"path": folder}, verify={"expect_status": "success", "expect_key": "path"})])

        if any(token in lowered for token in ("show file in explorer", "reveal file", "select file in explorer")):
            target = self._extract_path(text)
            if target:
                return (
                    "explorer_select_file",
                    [self._step("explorer_select_file", args={"path": target}, verify={"expect_status": "success", "expect_key": "path"})],
                )

        if any(token in lowered for token in ("list folder", "list directory", "show folder", "show directory")):
            folder = self._extract_path(text) or str(Path.home())
            return ("list_folder", [self._step("list_folder", args={"path": folder}, verify={"expect_status": "success"})])

        if any(token in lowered for token in ("create folder", "make folder", "new folder")):
            folder = self._extract_path(text)
            if folder:
                return ("create_folder", [self._step("create_folder", args={"path": folder}, verify={"expect_status": "success"})])

        if any(token in lowered for token in ("folder size", "directory size")):
            folder = self._extract_path(text) or str(Path.home())
            return ("folder_size", [self._step("folder_size", args={"path": folder}, verify={"expect_status": "success", "expect_key": "size_bytes"})])

        if any(token in lowered for token in ("read file", "show file", "open file")):
            target = self._extract_path(text)
            if target:
                return ("read_file", [self._step("read_file", args={"path": target}, verify={"expect_status": "success", "expect_key": "content"})])

        if any(token in lowered for token in ("write file", "save file", "create file")):
            target = self._extract_path(text)
            content = self._extract_content(text)
            if target:
                return ("write_file", [self._step("write_file", args={"path": target, "content": content, "overwrite": True}, verify={"expect_status": "success", "expect_key": "bytes"})])

        if any(
            token in lowered
            for token in ("copy file and hash", "copy and hash", "copy then hash", "duplicate file and hash")
        ):
            src, dst = self._extract_two_paths(text)
            if src and dst:
                copy_step = self._step("copy_file", args={"source": src, "destination": dst}, verify={"expect_status": "success"})
                hash_step = self._step(
                    "hash_file",
                    args={"path": f"{{{{steps.{copy_step.step_id}.output.destination}}}}", "algo": "sha256"},
                    depends_on=[copy_step.step_id],
                    verify={"expect_status": "success", "expect_key": "hash"},
                )
                return ("copy_and_hash_file", [copy_step, hash_step])

        if any(token in lowered for token in ("copy file", "duplicate file")):
            src, dst = self._extract_two_paths(text)
            if src and dst:
                return ("copy_file", [self._step("copy_file", args={"source": src, "destination": dst}, verify={"expect_status": "success"})])

        if any(
            token in lowered
            for token in ("backup file and hash", "backup and hash", "backup then hash", "backup this file and hash")
        ):
            src = self._extract_path(text)
            if src:
                backup_step = self._step(
                    "backup_file",
                    args={"source": src},
                    verify={"expect_status": "success", "expect_key": "backup_path"},
                )
                hash_step = self._step(
                    "hash_file",
                    args={"path": f"{{{{steps.{backup_step.step_id}.output.backup_path}}}}", "algo": "sha256"},
                    depends_on=[backup_step.step_id],
                    verify={"expect_status": "success", "expect_key": "hash"},
                )
                return ("backup_and_hash_file", [backup_step, hash_step])

        if any(token in lowered for token in ("backup file", "backup this file")):
            src = self._extract_path(text)
            if src:
                return ("backup_file", [self._step("backup_file", args={"source": src}, verify={"expect_status": "success", "expect_key": "backup_path"})])

        if any(token in lowered for token in ("hash file", "checksum")):
            src = self._extract_path(text)
            if src:
                return ("hash_file", [self._step("hash_file", args={"path": src, "algo": "sha256"}, verify={"expect_status": "success", "expect_key": "hash"})])

        if "time" in lowered:
            timezone = self._extract_timezone(text)
            return ("time_query", [self._step("time_now", args={"timezone": timezone}, verify={"expect_status": "success", "expect_key": "iso"})])

        return (
            "speak",
            [
                self._step(
                    "tts_speak",
                    args={"text": "I received your request and I am ready to help."},
                    can_retry=False,
                    verify={"optional": True},
                )
            ],
        )
    def _build_replan_steps(self, text: str, lowered: str, context: Dict[str, object]) -> tuple[str, List[PlanStep]]:
        failed_action = str(context.get("last_failure_action", "")).strip()
        failure_reason = str(context.get("last_failure_error", "")).strip() or "Execution failed."
        failure_category = str(context.get("last_failure_category", "")).strip().lower() or "unknown"
        failure_attempt = self._clamp_int(context.get("last_failure_attempt", 1), minimum=1, maximum=1000, default=1)
        failure_retry_count = self._clamp_int(context.get("last_failure_retry_count", 0), minimum=0, maximum=1000, default=0)
        retry_note = f" after {failure_retry_count} adaptive retries" if failure_retry_count > 0 else ""
        execution_feedback = context.get("execution_feedback", {})
        execution_feedback_map = execution_feedback if isinstance(execution_feedback, dict) else {}
        try:
            execution_quality_score = float(execution_feedback_map.get("quality_score", 0.0))
        except Exception:  # noqa: BLE001
            execution_quality_score = 0.0
        execution_quality_score = max(0.0, min(execution_quality_score, 1.0))
        confirm_policy_raw = context.get("last_failure_confirm_policy", {})
        confirm_policy = confirm_policy_raw if isinstance(confirm_policy_raw, dict) else {}
        confirm_policy_satisfied = bool(confirm_policy.get("satisfied", True))
        confirm_policy_total = self._clamp_int(confirm_policy.get("total_count", 0), minimum=0, maximum=1000, default=0)
        mission_feedback_raw = context.get("mission_feedback", {})
        mission_feedback = mission_feedback_raw if isinstance(mission_feedback_raw, dict) else {}
        mission_risk_level = str(mission_feedback.get("risk_level", "")).strip().lower()
        recommended_recovery_profile = str(mission_feedback.get("recommended_recovery_profile", "")).strip().lower()

        if failed_action in {
            "open_url",
            "browser_read_dom",
            "browser_extract_links",
            "browser_session_request",
            "browser_session_read_dom",
            "browser_session_extract_links",
        } and failure_category in {
            "timeout",
            "transient",
            "rate_limited",
        }:
            url = self._extract_url(text) or self._extract_domain_like(text)
            if url:
                if not url.startswith(("http://", "https://")):
                    url = f"https://{url}"
                return (
                    "browser_timeout_replan",
                    [
                        self._step("open_url", args={"url": url}, verify={"expect_status": "success", "expect_key": "url"}),
                        self._step(
                            "tts_speak",
                            args={
                                "text": (
                                    f"The browser task failed with {failure_category} on attempt {failure_attempt}{retry_note}. "
                                    "I opened the URL directly so you can continue manually."
                                )
                            },
                            can_retry=False,
                            verify={"optional": True},
                        ),
                    ],
                )

        if failed_action == "open_app":
            app_name = self._extract_app_name(text)
            return (
                "open_application_replan",
                [
                    self._step("search_files", args={"base_dir": str(Path.home()), "pattern": f"*{app_name}*.exe", "max_results": 40}, verify={"optional": True}),
                    self._step(
                        "tts_speak",
                        args={
                            "text": (
                                f"I could not launch {app_name} ({failure_category}, attempt {failure_attempt}{retry_note}). "
                                "I searched your user folders for matching executables. If needed, provide an exact executable path."
                            )
                        },
                        can_retry=False,
                        verify={"optional": True},
                    ),
                ],
            )

        if failed_action == "media_search":
            query = self._extract_media_query(text)
            url = f"https://www.youtube.com/results?search_query={query.replace(' ', '+')}"
            return ("media_search_replan", [self._step("open_url", args={"url": url}, verify={"expect_status": "success", "expect_key": "url"})])

        if failed_action == "write_file":
            return (
                "write_file_replan",
                [
                    self._step(
                        "tts_speak",
                        args={
                            "text": (
                                "I could not write that file due to policy or path constraints. "
                                f"Failure category: {failure_category}, attempt {failure_attempt}{retry_note}. "
                                "Try a path inside your home or workspace directory."
                            )
                        },
                        can_retry=False,
                        verify={"optional": True},
                    )
                ],
            )

        if failed_action in {"computer_click_text", "computer_click_target"}:
            phrase = self._extract_phrase(text) or self._extract_keyword(text)
            if phrase:
                if (confirm_policy_total > 0 and not confirm_policy_satisfied) or execution_quality_score <= 0.46:
                    return (
                        "computer_click_accessibility_replan",
                        [
                            self._step(
                                "accessibility_find_element",
                                args={"query": phrase, "max_results": 12},
                                verify={"expect_status": "success"},
                            ),
                            self._step(
                                "accessibility_invoke_element",
                                args={"query": phrase, "prefer_focused": True},
                                verify={"expect_status": "success"},
                                max_retries=1,
                            ),
                            self._step(
                                "tts_speak",
                                args={
                                    "text": (
                                        f"Click automation for '{phrase}' had low reliability "
                                        f"(quality score {execution_quality_score:.2f}). "
                                        "I switched to accessibility-grounded targeting to improve precision."
                                    )
                                },
                                can_retry=False,
                                verify={"optional": True},
                            ),
                        ],
                    )
                return (
                    "computer_click_target_replan",
                    [
                        self._step(
                            "computer_find_text_targets",
                            args={"query": phrase, "match_mode": "contains", "limit": 20},
                            verify={"expect_status": "success"},
                        ),
                        self._step(
                            "tts_speak",
                            args={
                                "text": (
                                    f"I could not click '{phrase}' on attempt {failure_attempt}{retry_note}. "
                                    "I identified available text targets so you can refine the next target click."
                                )
                            },
                            can_retry=False,
                            verify={"optional": True},
                        ),
                    ],
                )

        if failed_action == "accessibility_invoke_element":
            phrase = self._extract_phrase(text) or self._extract_keyword(text)
            if phrase:
                return (
                    "accessibility_invoke_replan",
                    [
                        self._step(
                            "accessibility_find_element",
                            args={"query": phrase, "max_results": 10},
                            verify={"expect_status": "success"},
                        ),
                        self._step(
                            "tts_speak",
                            args={
                                "text": (
                                    f"I could not invoke UI element '{phrase}' on attempt {failure_attempt}{retry_note}. "
                                    "I extracted matching UI elements so the action can be refined."
                                )
                            },
                            can_retry=False,
                            verify={"optional": True},
                        ),
                    ],
                )

        if failed_action in {
            "external_email_send",
            "external_email_list",
            "external_email_read",
            "external_calendar_create_event",
            "external_calendar_list_events",
            "external_calendar_update_event",
            "external_doc_create",
            "external_doc_list",
            "external_doc_read",
            "external_doc_update",
            "external_task_list",
            "external_task_create",
            "external_task_update",
        }:
            contract_replan = self._build_external_contract_replan(
                text=text,
                failed_action=failed_action,
                failure_category=failure_category,
                failure_attempt=failure_attempt,
                retry_note=retry_note,
                context=context,
            )
            if contract_replan is not None:
                return contract_replan

            preflight_steps: List[PlanStep] = []
            if failure_category in {"timeout", "transient", "rate_limited", "unknown"}:
                preflight_steps.append(
                    self._step(
                        "oauth_token_maintain",
                        args={"refresh_window_s": 300, "dry_run": False},
                        verify={"expect_status": "success"},
                    )
                )
            preflight_steps.append(
                self._step(
                    "external_connector_preflight",
                    args={"action": failed_action, "provider": "auto"},
                    verify={"expect_key": "contract_diagnostic"},
                )
            )
            preflight_steps.append(
                self._step(
                    "external_connector_status",
                    args={},
                    verify={"expect_status": "success"},
                )
            )
            return (
                "external_connector_replan",
                preflight_steps
                + [
                    self._step(
                        "tts_speak",
                        args={
                            "text": (
                                f"The external action {failed_action} failed with {failure_category} on attempt "
                                f"{failure_attempt}{retry_note}. I ran token maintenance and connector diagnostics; "
                                f"verify OAuth scopes, account binding, and recovery profile"
                                f"{' (' + recommended_recovery_profile + ')' if recommended_recovery_profile else ''}."
                            )
                        },
                        can_retry=False,
                        verify={"optional": True},
                    ),
                ],
            )

        if failure_category == "non_retryable":
            return (
                "non_retryable_replan",
                [
                    self._step(
                        "tts_speak",
                        args={
                            "text": (
                                f"The step {failed_action} failed with a non-retryable condition on attempt {failure_attempt}: "
                                f"{failure_reason}. Please adjust permissions, arguments, or approval and retry."
                            )
                        },
                        can_retry=False,
                        verify={"optional": True},
                    )
                ],
            )

        if any(token in lowered for token in ("open ", "launch ", "start app", "run app")):
            app_name = self._extract_app_name(text)
            return (
                "open_application_replan",
                [
                    self._step("open_url", args={"url": f"https://www.google.com/search?q={app_name.replace(' ', '+')}+download"}, verify={"expect_status": "success", "expect_key": "url"}),
                    self._step("tts_speak", args={"text": f"I could not open {app_name}. I opened search results to help you install or locate it."}, can_retry=False, verify={"optional": True}),
                ],
            )

        return (
            "replan_fallback",
            [
                self._step(
                    "tts_speak",
                    args={
                            "text": (
                                f"I retried, but {failed_action} still failed ({failure_category}, attempt {failure_attempt}{retry_note}): "
                                f"{failure_reason}. "
                                f"Mission risk={mission_risk_level or 'unknown'}, "
                                f"execution quality={execution_quality_score:.2f}."
                            )
                        },
                        can_retry=False,
                        verify={"optional": True},
                    )
            ],
        )

    def _build_external_contract_replan(
        self,
        *,
        text: str,
        failed_action: str,
        failure_category: str,
        failure_attempt: int,
        retry_note: str,
        context: Dict[str, object],
    ) -> Optional[tuple[str, List[PlanStep]]]:
        contract_raw = context.get("last_failure_external_contract", {})
        contract = contract_raw if isinstance(contract_raw, dict) else {}
        contract_code = str(contract.get("code", "")).strip().lower()
        if not contract_code:
            return None

        request_raw = context.get("last_failure_request", {})
        request = request_raw if isinstance(request_raw, dict) else {}
        request_args_raw = request.get("args", {})
        failure_args = dict(request_args_raw) if isinstance(request_args_raw, dict) else {}

        missing_fields_raw = contract.get("missing_fields", [])
        missing_fields = (
            [str(item).strip() for item in missing_fields_raw if str(item).strip()]
            if isinstance(missing_fields_raw, list)
            else []
        )
        any_of_raw = contract.get("any_of", [])
        any_of_groups: List[List[str]] = []
        if isinstance(any_of_raw, list):
            for row in any_of_raw[:8]:
                if not isinstance(row, list):
                    continue
                normalized = [str(item).strip() for item in row if str(item).strip()]
                if normalized:
                    any_of_groups.append(normalized[:8])

        allowed_raw = contract.get("allowed_providers", [])
        allowed_providers = [
            self._normalize_provider(str(item).strip())
            for item in (allowed_raw if isinstance(allowed_raw, list) else [])
            if self._normalize_provider(str(item).strip())
        ]
        auth_blocked_raw = contract.get("auth_blocked_providers", [])
        auth_blocked = [
            self._normalize_provider(str(item).strip())
            for item in (auth_blocked_raw if isinstance(auth_blocked_raw, list) else [])
            if self._normalize_provider(str(item).strip())
        ]
        blocked_providers_raw = contract.get("blocked_providers", [])
        blocked_providers = [
            self._normalize_provider(str(item).strip())
            for item in (blocked_providers_raw if isinstance(blocked_providers_raw, list) else [])
            if self._normalize_provider(str(item).strip())
        ]
        retry_after_s = self._clamp_float(
            contract.get("retry_after_s", 0.0),
            minimum=0.0,
            maximum=86_400.0,
            default=0.0,
        )
        blocked_ratio = self._clamp_float(
            contract.get("blocked_ratio", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        checks_raw = contract.get("checks", [])
        checks = [dict(row) for row in checks_raw if isinstance(row, dict)] if isinstance(checks_raw, list) else []
        remediation_plan_raw = contract.get("remediation_plan", [])
        remediation_plan = [dict(row) for row in remediation_plan_raw if isinstance(row, dict)] if isinstance(remediation_plan_raw, list) else []

        remediation_rows = contract.get("remediation_hints", [])
        hint_ids: set[str] = set()
        remediation_tool_actions: list[tuple[str, Dict[str, Any], int, float]] = []
        if isinstance(remediation_rows, list):
            for row in remediation_rows[:12]:
                if not isinstance(row, dict):
                    continue
                hint_id = str(row.get("id", "")).strip().lower()
                if hint_id:
                    hint_ids.add(hint_id)
                tool_action = row.get("tool_action")
                if isinstance(tool_action, dict):
                    tool_name = str(tool_action.get("action", "")).strip().lower()
                    tool_args = tool_action.get("args", {})
                    if tool_name and isinstance(tool_args, dict):
                        remediation_tool_actions.append(
                            (
                                tool_name,
                                dict(tool_args),
                                self._clamp_int(row.get("priority", 99), minimum=1, maximum=999, default=99),
                                self._clamp_float(row.get("confidence", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                            )
                        )
        for row in remediation_plan[:10]:
            tool_action = row.get("tool_action")
            if not isinstance(tool_action, dict):
                continue
            tool_name = str(tool_action.get("action", "")).strip().lower()
            tool_args = tool_action.get("args", {})
            if not tool_name or not isinstance(tool_args, dict):
                continue
            phase = str(row.get("phase", "")).strip().lower()
            priority = 1 if phase in {"repair_dependency", "diagnose"} else 4
            remediation_tool_actions.append((tool_name, dict(tool_args), priority, 0.72))
        repair_memory_hints_raw = context.get("repair_memory_hints", [])
        repair_memory_hints = [dict(row) for row in repair_memory_hints_raw if isinstance(row, dict)] if isinstance(repair_memory_hints_raw, list) else []
        failure_clusters_raw = context.get("external_failure_clusters", [])
        failure_clusters = [dict(row) for row in failure_clusters_raw if isinstance(row, dict)] if isinstance(failure_clusters_raw, list) else []
        runtime_reliability_codes = {
            "provider_cooldown_blocked",
            "provider_outage_blocked",
            "provider_runtime_blocked",
        }
        runtime_reliability_contract = contract_code in runtime_reliability_codes
        runtime_outage = contract_code == "provider_outage_blocked"

        repaired_args = self._repair_external_action_args(
            action=failed_action,
            text=text,
            failure_args=failure_args,
            contract_code=contract_code,
            missing_fields=missing_fields,
            any_of_groups=any_of_groups,
            allowed_providers=allowed_providers,
        )
        if not repaired_args and runtime_reliability_contract and failed_action.startswith("external_"):
            repaired_args = {"provider": "auto"}
        cluster_patch = self._repair_patch_from_failure_clusters(
            action=failed_action,
            contract_code=contract_code,
            failure_clusters=failure_clusters,
            allowed_providers=allowed_providers,
        )
        cluster_patch_applied = False
        if cluster_patch:
            for key, value in cluster_patch.items():
                clean_key = str(key).strip()
                if not clean_key:
                    continue
                if clean_key == "provider":
                    candidate_provider = self._normalize_provider(str(value).strip())
                    if not candidate_provider:
                        continue
                    if allowed_providers and candidate_provider not in allowed_providers:
                        continue
                    if repaired_args.get("provider") != candidate_provider:
                        repaired_args["provider"] = candidate_provider
                        cluster_patch_applied = True
                    continue
                if self._payload_has_value(repaired_args.get(clean_key)):
                    continue
                if self._payload_has_value(value):
                    repaired_args[clean_key] = value
                    cluster_patch_applied = True
        memory_patch = self._repair_patch_from_memory_hints(
            action=failed_action,
            contract_code=contract_code,
            repair_memory_hints=repair_memory_hints,
            allowed_providers=allowed_providers,
        )
        memory_patch_applied = False
        if memory_patch:
            for key, value in memory_patch.items():
                clean_key = str(key).strip()
                if not clean_key:
                    continue
                if clean_key == "provider":
                    candidate_provider = self._normalize_provider(str(value).strip())
                    if not candidate_provider:
                        continue
                    if allowed_providers and candidate_provider not in allowed_providers:
                        continue
                    if repaired_args.get("provider") != candidate_provider:
                        repaired_args["provider"] = candidate_provider
                        memory_patch_applied = True
                    continue
                if self._payload_has_value(repaired_args.get(clean_key)):
                    continue
                if self._payload_has_value(value):
                    repaired_args[clean_key] = value
                    memory_patch_applied = True
        if not repaired_args:
            return None
        runtime_provider_switched = False
        if runtime_reliability_contract and failed_action.startswith("external_"):
            current_provider = self._normalize_provider(str(repaired_args.get("provider", "")).strip())
            candidates = self._external_provider_candidates(action=failed_action, args=repaired_args)
            if allowed_providers:
                ordered_candidates = [provider for provider in allowed_providers if provider in candidates]
                ordered_candidates.extend([provider for provider in candidates if provider not in ordered_candidates])
                candidates = ordered_candidates
            if not candidates:
                candidates = allowed_providers[:]
            blocked_set = {provider for provider in blocked_providers if provider}
            if current_provider and current_provider in blocked_set:
                fallback = next((provider for provider in candidates if provider and provider not in blocked_set), "")
                if fallback:
                    repaired_args["provider"] = fallback
                    runtime_provider_switched = True
                else:
                    repaired_args["provider"] = "auto"
                    runtime_provider_switched = True
            elif (not current_provider or current_provider == "auto") and candidates:
                preferred = next((provider for provider in candidates if provider and provider not in blocked_set), "")
                if preferred:
                    repaired_args["provider"] = preferred
                    runtime_provider_switched = True

        steps: List[PlanStep] = []
        planned_signatures: set[str] = set()

        def append_step_once(action_name: str, *, args: Dict[str, Any], verify: Dict[str, Any]) -> bool:
            signature = f"{action_name}|{json.dumps(args, ensure_ascii=True, sort_keys=True, separators=(',', ':'))}"
            if signature in planned_signatures:
                return False
            steps.append(self._step(action_name, args=args, verify=verify))
            planned_signatures.add(signature)
            return True

        requires_auth_maintenance = contract_code == "auth_preflight_failed" or bool(
            hint_ids.intersection({"reauthorize_with_scopes", "refresh_access_token", "connect_provider_account", "run_auth_maintenance"})
        )
        if runtime_reliability_contract:
            runtime_provider_hint = ""
            for provider in blocked_providers[:2]:
                if provider:
                    runtime_provider_hint = provider
                    break
            if not runtime_provider_hint:
                runtime_provider_hint = self._normalize_provider(str(repaired_args.get("provider", "")).strip())
            status_args: Dict[str, Any] = {}
            if runtime_provider_hint:
                status_args["provider"] = runtime_provider_hint
            append_step_once(
                "external_connector_status",
                args=status_args,
                verify={"expect_status": "success"},
            )
            if runtime_outage or blocked_ratio >= 0.65 or retry_after_s >= 12.0:
                maintain_args: Dict[str, Any] = {
                    "refresh_window_s": max(420, int(round(min(3600.0, max(420.0, retry_after_s * 1.5))))),
                    "dry_run": False,
                }
                if runtime_provider_hint and runtime_provider_hint != "auto":
                    maintain_args["provider"] = runtime_provider_hint
                append_step_once(
                    "oauth_token_maintain",
                    args=maintain_args,
                    verify={"expect_status": "success"},
                )

        if requires_auth_maintenance:
            providers = auth_blocked[:2]
            if not providers:
                provider_hint = self._normalize_provider(str(repaired_args.get("provider", "")).strip())
                if provider_hint and provider_hint != "auto":
                    providers = [provider_hint]
            if providers:
                for provider in providers:
                    append_step_once(
                        "oauth_token_maintain",
                        args={"provider": provider, "refresh_window_s": 420, "dry_run": False},
                        verify={"expect_status": "success"},
                    )
            else:
                append_step_once(
                    "oauth_token_maintain",
                    args={"refresh_window_s": 420, "dry_run": False},
                    verify={"expect_status": "success"},
                )

        tool_allowlist = {"oauth_token_maintain", "oauth_token_refresh", "external_connector_status", "external_connector_preflight"}
        ranked_tool_actions = self._rank_external_remediation_tool_actions(
            action=failed_action,
            contract_code=contract_code,
            remediation_tool_actions=remediation_tool_actions,
            blocked_providers=blocked_providers,
            allowed_providers=allowed_providers,
            repair_memory_hints=repair_memory_hints,
            failure_clusters=failure_clusters,
            context=context,
        )
        budget_profile = self._external_replan_confidence_budget(
            contract_code=contract_code,
            failure_category=failure_category,
            blocked_ratio=blocked_ratio,
            retry_after_s=retry_after_s,
            context=context,
        )
        selected_remediation_actions = 0
        max_remediation_actions = self._clamp_int(
            budget_profile.get("max_remediation_actions", 4),
            minimum=1,
            maximum=12,
            default=4,
        )
        min_remediation_confidence = self._clamp_float(
            budget_profile.get("min_confidence", self.external_replan_min_confidence_floor),
            minimum=0.0,
            maximum=1.0,
            default=self.external_replan_min_confidence_floor,
        )
        critical_action_set = {"external_connector_status", "external_connector_preflight", "oauth_token_maintain"}
        for tool_name, tool_args, _priority, _confidence in ranked_tool_actions[:12]:
            if tool_name not in tool_allowlist:
                continue
            confidence_value = self._clamp_float(_confidence, minimum=0.0, maximum=1.0, default=0.0)
            priority_value = self._clamp_int(_priority, minimum=1, maximum=999, default=99)
            critical = bool(tool_name in critical_action_set and priority_value <= 2)
            if (
                self.external_replan_confidence_budget_enabled
                and not critical
                and confidence_value < min_remediation_confidence
            ):
                continue
            if self.external_replan_confidence_budget_enabled and selected_remediation_actions >= max_remediation_actions:
                break
            args_patch = dict(tool_args)
            if tool_name == "external_connector_preflight":
                args_patch.setdefault("action", failed_action)
                args_patch.setdefault("provider", str(repaired_args.get("provider", "auto")) or "auto")
                appended = append_step_once(
                    tool_name,
                    args=args_patch,
                    verify={
                        "expect_key": "contract_diagnostic",
                        "planner_replan_confidence": round(confidence_value, 6),
                        "planner_replan_priority": int(priority_value),
                        "planner_replan_budget_mode": str(budget_profile.get("mode", "")),
                    },
                )
                if appended:
                    selected_remediation_actions += 1
                continue
            if tool_name == "external_connector_status":
                if not args_patch:
                    provider_hint = self._normalize_provider(str(repaired_args.get("provider", "")).strip())
                    if provider_hint:
                        args_patch["provider"] = provider_hint
                appended = append_step_once(
                    tool_name,
                    args=args_patch,
                    verify={
                        "expect_status": "success",
                        "planner_replan_confidence": round(confidence_value, 6),
                        "planner_replan_priority": int(priority_value),
                        "planner_replan_budget_mode": str(budget_profile.get("mode", "")),
                    },
                )
                if appended:
                    selected_remediation_actions += 1
                continue
            appended = append_step_once(
                tool_name,
                args=args_patch,
                verify={
                    "expect_status": "success",
                    "planner_replan_confidence": round(confidence_value, 6),
                    "planner_replan_priority": int(priority_value),
                    "planner_replan_budget_mode": str(budget_profile.get("mode", "")),
                },
            )
            if appended:
                selected_remediation_actions += 1

        append_step_once(
            "external_connector_preflight",
            args={"action": failed_action, "provider": str(repaired_args.get("provider", "auto")) or "auto"},
            verify={"expect_key": "contract_diagnostic"},
        )

        append_step_once(
            "external_connector_status",
            args={},
            verify={"expect_status": "success"},
        )

        identifier_recovery = self._external_identifier_recovery_step(action=failed_action, repaired_args=repaired_args)
        if identifier_recovery is not None:
            steps.append(identifier_recovery)
            steps.append(
                self._step(
                    "tts_speak",
                    args={
                        "text": (
                            f"The external action {failed_action} failed with contract issue '{contract_code}' "
                            f"on attempt {failure_attempt}{retry_note}. I ran connector diagnostics and fetched candidate "
                            "records to recover the missing identifier before retry."
                        )
                    },
                    can_retry=False,
                    verify={"optional": True},
                )
            )
            return ("external_contract_discovery_replan", steps)

        retry_step = self._step(
            failed_action,
            args=repaired_args,
            verify={
                "expect_status": "success",
                "planner_retry_confidence": round(
                    self._external_replan_retry_confidence(
                        contract_code=contract_code,
                        repaired_args=repaired_args,
                        runtime_reliability_contract=runtime_reliability_contract,
                        runtime_provider_switched=runtime_provider_switched,
                        memory_patch_applied=memory_patch_applied,
                        cluster_patch_applied=cluster_patch_applied,
                        blocked_ratio=blocked_ratio,
                        retry_after_s=retry_after_s,
                        budget_profile=budget_profile,
                    ),
                    6,
                ),
                "planner_replan_budget_mode": str(budget_profile.get("mode", "")),
            },
            max_retries=0 if self.external_replan_confidence_budget_enabled and min_remediation_confidence >= 0.56 else 1,
        )
        steps.append(retry_step)
        steps.append(
            self._step(
                "tts_speak",
                    args={
                        "text": (
                            f"I repaired external contract '{contract_code}' for {failed_action} "
                            f"(category {failure_category}, attempt {failure_attempt}{retry_note}) and retried with "
                            "provider/auth/payload corrections."
                            + (
                                f" Runtime reliability block ratio was {blocked_ratio:.2f} with suggested retry delay {retry_after_s:.1f}s."
                                if runtime_reliability_contract
                                else ""
                            )
                            + (" I rerouted away from blocked providers before retry." if runtime_provider_switched else "")
                            + (" I used aggregated external failure clusters to select a more reliable patch." if cluster_patch_applied else "")
                            + (" I reused historical successful repair memory for this action." if memory_patch_applied else "")
                        )
                    },
                can_retry=False,
                verify={"optional": True},
            )
        )
        return ("external_contract_repair_replan", steps)

    def _repair_external_action_args(
        self,
        *,
        action: str,
        text: str,
        failure_args: Dict[str, Any],
        contract_code: str,
        missing_fields: List[str],
        any_of_groups: List[List[str]],
        allowed_providers: List[str],
    ) -> Dict[str, Any]:
        args: Dict[str, Any] = {str(key): value for key, value in failure_args.items()}
        runtime_reliability_codes = {
            "provider_cooldown_blocked",
            "provider_outage_blocked",
            "provider_runtime_blocked",
        }
        if not args:
            if action.startswith("external_") and contract_code in runtime_reliability_codes:
                return {"provider": "auto"}
            return {}

        if action.startswith("external_") and "provider" not in args:
            args["provider"] = "auto"

        current_provider = self._normalize_provider(str(args.get("provider", "")).strip())
        if current_provider:
            args["provider"] = current_provider
        if contract_code in {"provider_not_supported_for_action", "no_provider_candidates_after_contract", *runtime_reliability_codes}:
            if allowed_providers:
                args["provider"] = allowed_providers[0]
            elif not current_provider or current_provider == "auto":
                candidates = self._external_provider_candidates(action=action, args=args)
                if candidates:
                    args["provider"] = candidates[0]

        wanted_fields = {str(field).strip() for field in missing_fields if str(field).strip()}
        if contract_code in {"missing_any_of_fields", "invalid_field_type_or_range"}:
            for group in any_of_groups:
                for field in group:
                    if str(field).strip():
                        wanted_fields.add(str(field).strip())

        for field in sorted(wanted_fields):
            if self._payload_has_value(args.get(field)):
                continue
            inferred = self._infer_external_field_value(action=action, field=field, text=text, args=args)
            if self._payload_has_value(inferred):
                args[field] = inferred

        for group in any_of_groups:
            normalized_group = [str(item).strip() for item in group if str(item).strip()]
            if not normalized_group:
                continue
            if any(self._payload_has_value(args.get(field)) for field in normalized_group):
                continue
            for field in normalized_group:
                inferred = self._infer_external_field_value(action=action, field=field, text=text, args=args)
                if self._payload_has_value(inferred):
                    args[field] = inferred
                    break

        return args

    def _repair_patch_from_memory_hints(
        self,
        *,
        action: str,
        contract_code: str,
        repair_memory_hints: List[Dict[str, Any]],
        allowed_providers: List[str],
    ) -> Dict[str, Any]:
        clean_action = str(action or "").strip().lower()
        clean_contract_code = str(contract_code or "").strip().lower()
        if not clean_action or not isinstance(repair_memory_hints, list):
            return {}

        ranked: List[tuple[float, Dict[str, Any]]] = []
        for hint in repair_memory_hints[:12]:
            if not isinstance(hint, dict):
                continue
            memory_score = self._clamp_float(hint.get("memory_score", 0.0), minimum=0.0, maximum=10.0, default=0.0)
            signals_raw = hint.get("signals", [])
            if not isinstance(signals_raw, list):
                continue
            for signal in signals_raw[:4]:
                if not isinstance(signal, dict):
                    continue
                signal_action = str(signal.get("action", "")).strip().lower()
                if signal_action != clean_action:
                    continue
                signal_status = str(signal.get("status", "")).strip().lower()
                signal_provider = self._normalize_provider(str(signal.get("provider", "")).strip())
                signal_code = str(signal.get("contract_code", "")).strip().lower()
                signal_args_raw = signal.get("args", {})
                signal_args = self._sanitize_args(signal_args_raw) if isinstance(signal_args_raw, dict) else {}
                if not signal_args and not signal_provider:
                    continue
                score = 0.0
                if signal_status == "success":
                    score += 0.7
                elif signal_status == "failed":
                    score += 0.12
                else:
                    score += 0.25
                score += min(0.26, memory_score * 0.09)
                if clean_contract_code and signal_code == clean_contract_code:
                    score += 0.42
                elif clean_contract_code and signal_code:
                    score += 0.1
                if signal_provider:
                    if allowed_providers and signal_provider in allowed_providers:
                        score += 0.18
                    elif not allowed_providers:
                        score += 0.08
                ranked.append((score, {"provider": signal_provider, "args": signal_args}))

        if not ranked:
            return {}
        ranked.sort(key=lambda item: item[0], reverse=True)
        top = ranked[0][1]
        patch: Dict[str, Any] = {}
        provider = self._normalize_provider(str(top.get("provider", "")).strip())
        if provider and (not allowed_providers or provider in allowed_providers):
            patch["provider"] = provider
        args_payload = top.get("args", {})
        if isinstance(args_payload, dict):
            for key, value in args_payload.items():
                clean_key = str(key).strip()
                if not clean_key:
                    continue
                if clean_key == "provider":
                    continue
                patch[clean_key] = value
        return patch

    def _repair_patch_from_failure_clusters(
        self,
        *,
        action: str,
        contract_code: str,
        failure_clusters: List[Dict[str, Any]],
        allowed_providers: List[str],
    ) -> Dict[str, Any]:
        clean_action = str(action or "").strip().lower()
        clean_contract_code = str(contract_code or "").strip().lower()
        if not clean_action or not isinstance(failure_clusters, list):
            return {}

        ranked: List[tuple[float, Dict[str, Any]]] = []
        for cluster in failure_clusters[:20]:
            if not isinstance(cluster, dict):
                continue
            cluster_action = str(cluster.get("action", "")).strip().lower()
            if cluster_action != clean_action:
                continue
            cluster_code = str(cluster.get("contract_code", "")).strip().lower()
            samples = self._clamp_int(cluster.get("samples", 0), minimum=0, maximum=100_000, default=0)
            success_ratio = self._clamp_float(cluster.get("success_ratio", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            risk_score = self._clamp_float(cluster.get("risk_score", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            relevance = self._clamp_float(cluster.get("relevance", 0.0), minimum=0.0, maximum=10.0, default=0.0)
            preferred_provider = self._normalize_provider(str(cluster.get("preferred_provider", "")).strip())
            patch_raw = cluster.get("suggested_patch", {})
            patch = self._sanitize_args(patch_raw) if isinstance(patch_raw, dict) else {}
            score = (relevance * 0.36) + (success_ratio * 1.2) + min(0.42, float(samples) * 0.04) - (risk_score * 0.28)
            if clean_contract_code and cluster_code == clean_contract_code:
                score += 0.58
            elif clean_contract_code and cluster_code == "unknown":
                score += 0.08
            elif clean_contract_code and cluster_code:
                score += 0.12
            if preferred_provider:
                if allowed_providers and preferred_provider in allowed_providers:
                    score += 0.24
                elif not allowed_providers:
                    score += 0.08
                else:
                    score -= 0.2
            ranked.append((score, {"provider": preferred_provider, "args": patch}))

        if not ranked:
            return {}
        ranked.sort(key=lambda item: item[0], reverse=True)
        top = ranked[0][1]
        patch: Dict[str, Any] = {}
        provider = self._normalize_provider(str(top.get("provider", "")).strip())
        if provider and (not allowed_providers or provider in allowed_providers):
            patch["provider"] = provider
        args_payload = top.get("args", {})
        if isinstance(args_payload, dict):
            for key, value in args_payload.items():
                clean_key = str(key).strip()
                if not clean_key:
                    continue
                if clean_key == "provider":
                    continue
                patch[clean_key] = value
        return patch

    def _external_replan_confidence_budget(
        self,
        *,
        contract_code: str,
        failure_category: str,
        blocked_ratio: float,
        retry_after_s: float,
        context: Dict[str, object],
    ) -> Dict[str, Any]:
        if not bool(self.external_replan_confidence_budget_enabled):
            return {
                "mode": "disabled",
                "min_confidence": self.external_replan_min_confidence_floor,
                "max_remediation_actions": self.external_replan_max_actions_base,
            }

        clean_contract_code = str(contract_code or "").strip().lower()
        clean_failure_category = str(failure_category or "").strip().lower()
        mission_analysis_raw = context.get("external_reliability_mission_analysis", {})
        mission_analysis = mission_analysis_raw if isinstance(mission_analysis_raw, dict) else {}
        volatility_index = self._clamp_float(
            mission_analysis.get("volatility_index", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        volatility_mode = str(mission_analysis.get("volatility_mode", "")).strip().lower()
        at_risk_ratio = self._clamp_float(
            mission_analysis.get("at_risk_ratio", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )

        pressure = self._clamp_float(
            (self._clamp_float(blocked_ratio, minimum=0.0, maximum=1.0, default=0.0) * 0.42)
            + (min(1.0, max(0.0, retry_after_s) / 90.0) * 0.18)
            + (volatility_index * 0.24)
            + (at_risk_ratio * 0.16),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        runtime_reliability_codes = {
            "provider_cooldown_blocked",
            "provider_outage_blocked",
            "provider_runtime_blocked",
        }
        severe_contract = clean_contract_code in {
            "auth_preflight_failed",
            "provider_not_supported_for_action",
            "no_provider_candidates_after_contract",
        } or clean_contract_code in runtime_reliability_codes
        severe_failure = clean_failure_category in {"non_retryable", "blocked", "unknown", "auth"}
        if severe_contract:
            pressure = min(1.0, pressure + 0.16)
        if severe_failure:
            pressure = min(1.0, pressure + 0.12)
        if volatility_mode in {"surging", "elevated"}:
            pressure = min(1.0, pressure + 0.08)

        mode = "stable"
        if pressure >= 0.66:
            mode = "strict"
        elif pressure >= 0.4:
            mode = "guarded"
        min_confidence = self.external_replan_min_confidence_floor
        max_actions = self.external_replan_max_actions_base
        if mode == "strict":
            min_confidence = self._clamp_float(
                max(self.external_replan_min_confidence_floor, 0.56, pressure * 0.74),
                minimum=0.3,
                maximum=0.9,
                default=0.58,
            )
            max_actions = self._clamp_int(
                max(2, self.external_replan_max_actions_base - 1),
                minimum=2,
                maximum=10,
                default=self.external_replan_max_actions_base,
            )
        elif mode == "guarded":
            min_confidence = self._clamp_float(
                max(self.external_replan_min_confidence_floor, 0.46, pressure * 0.62),
                minimum=0.3,
                maximum=0.86,
                default=0.5,
            )
            max_actions = self._clamp_int(
                self.external_replan_max_actions_base,
                minimum=2,
                maximum=10,
                default=self.external_replan_max_actions_base,
            )
        else:
            min_confidence = self._clamp_float(
                max(self.external_replan_min_confidence_floor, 0.34),
                minimum=0.2,
                maximum=0.8,
                default=self.external_replan_min_confidence_floor,
            )
            max_actions = self._clamp_int(
                self.external_replan_max_actions_base + 1,
                minimum=2,
                maximum=12,
                default=self.external_replan_max_actions_base + 1,
            )

        return {
            "mode": mode,
            "pressure": round(pressure, 6),
            "min_confidence": round(min_confidence, 6),
            "max_remediation_actions": int(max_actions),
            "volatility_mode": volatility_mode,
            "volatility_index": round(volatility_index, 6),
            "at_risk_ratio": round(at_risk_ratio, 6),
        }

    def _external_replan_retry_confidence(
        self,
        *,
        contract_code: str,
        repaired_args: Dict[str, Any],
        runtime_reliability_contract: bool,
        runtime_provider_switched: bool,
        memory_patch_applied: bool,
        cluster_patch_applied: bool,
        blocked_ratio: float,
        retry_after_s: float,
        budget_profile: Dict[str, Any],
    ) -> float:
        clean_contract_code = str(contract_code or "").strip().lower()
        args_payload = repaired_args if isinstance(repaired_args, dict) else {}
        provider = self._normalize_provider(str(args_payload.get("provider", "")).strip())
        confidence = 0.54
        if provider and provider != "auto":
            confidence += 0.08
        elif provider == "auto":
            confidence -= 0.04
        if memory_patch_applied:
            confidence += 0.12
        if cluster_patch_applied:
            confidence += 0.1
        if runtime_reliability_contract:
            confidence += 0.05
        if runtime_provider_switched:
            confidence += 0.06
        if clean_contract_code in {"auth_preflight_failed", "missing_required_fields", "missing_any_of_fields"}:
            confidence -= 0.08
        confidence -= self._clamp_float(blocked_ratio, minimum=0.0, maximum=1.0, default=0.0) * 0.1
        confidence -= min(0.12, self._clamp_float(retry_after_s, minimum=0.0, maximum=86_400.0, default=0.0) / 300.0)
        profile_mode = str((budget_profile or {}).get("mode", "")).strip().lower()
        if profile_mode == "strict":
            confidence -= 0.06
        elif profile_mode == "stable":
            confidence += 0.04
        return self._clamp_float(confidence, minimum=0.0, maximum=1.0, default=0.5)

    def _rank_external_remediation_tool_actions(
        self,
        *,
        action: str,
        contract_code: str,
        remediation_tool_actions: List[tuple[str, Dict[str, Any], int, float]],
        blocked_providers: List[str],
        allowed_providers: List[str],
        repair_memory_hints: List[Dict[str, Any]],
        failure_clusters: List[Dict[str, Any]],
        context: Dict[str, object],
    ) -> List[tuple[str, Dict[str, Any], int, float]]:
        clean_action = str(action or "").strip().lower()
        clean_contract_code = str(contract_code or "").strip().lower()
        rows = [row for row in remediation_tool_actions if isinstance(row, tuple) and len(row) == 4]
        if not rows:
            return []

        blocked_set = {
            self._normalize_provider(str(provider).strip())
            for provider in blocked_providers
            if self._normalize_provider(str(provider).strip())
        }
        allowed_set = {
            self._normalize_provider(str(provider).strip())
            for provider in allowed_providers
            if self._normalize_provider(str(provider).strip())
        }
        external_trend_raw = context.get("external_reliability_trend", {})
        external_trend = external_trend_raw if isinstance(external_trend_raw, dict) else {}
        mission_analysis_raw = context.get("external_reliability_mission_analysis", {})
        mission_analysis = mission_analysis_raw if isinstance(mission_analysis_raw, dict) else {}
        top_risk_rows_raw = external_trend.get("top_provider_risks", [])
        risk_by_provider: Dict[str, float] = {}
        if isinstance(top_risk_rows_raw, list):
            for row in top_risk_rows_raw[:20]:
                if not isinstance(row, dict):
                    continue
                provider = self._normalize_provider(str(row.get("provider", "")).strip())
                if not provider:
                    continue
                risk = self._clamp_float(row.get("risk_score", 0.0), minimum=0.0, maximum=1.0, default=0.0)
                risk_by_provider[provider] = max(risk_by_provider.get(provider, 0.0), risk)
        volatility_mode = str(mission_analysis.get("volatility_mode", "")).strip().lower()
        volatility_index = self._clamp_float(
            mission_analysis.get("volatility_index", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        at_risk_ratio = self._clamp_float(
            mission_analysis.get("at_risk_ratio", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )

        memory_provider_success: Dict[str, float] = {}
        for hint in repair_memory_hints[:18]:
            if not isinstance(hint, dict):
                continue
            memory_score = self._clamp_float(hint.get("memory_score", 0.0), minimum=0.0, maximum=10.0, default=0.0)
            signals_raw = hint.get("signals", [])
            if not isinstance(signals_raw, list):
                continue
            for signal in signals_raw[:8]:
                if not isinstance(signal, dict):
                    continue
                signal_action = str(signal.get("action", "")).strip().lower()
                if signal_action != clean_action:
                    continue
                signal_provider = self._normalize_provider(str(signal.get("provider", "")).strip())
                if not signal_provider:
                    continue
                status = str(signal.get("status", "")).strip().lower()
                base = 0.0
                if status == "success":
                    base = 0.52
                elif status == "failed":
                    base = -0.18
                else:
                    base = 0.08
                base += min(0.28, memory_score * 0.06)
                if clean_contract_code:
                    signal_code = str(signal.get("contract_code", "")).strip().lower()
                    if signal_code == clean_contract_code:
                        base += 0.18
                    elif signal_code and signal_code != clean_contract_code:
                        base -= 0.05
                memory_provider_success[signal_provider] = memory_provider_success.get(signal_provider, 0.0) + base

        cluster_provider_score: Dict[str, float] = {}
        for cluster in failure_clusters[:24]:
            if not isinstance(cluster, dict):
                continue
            cluster_action = str(cluster.get("action", "")).strip().lower()
            if cluster_action != clean_action:
                continue
            provider = self._normalize_provider(str(cluster.get("preferred_provider", "")).strip())
            if not provider:
                continue
            success_ratio = self._clamp_float(cluster.get("success_ratio", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            risk_score = self._clamp_float(cluster.get("risk_score", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            relevance = self._clamp_float(cluster.get("relevance", 0.0), minimum=0.0, maximum=10.0, default=0.0)
            samples = self._clamp_int(cluster.get("samples", 0), minimum=0, maximum=100_000, default=0)
            score = (success_ratio * 0.48) - (risk_score * 0.34) + min(0.26, relevance * 0.04) + min(0.22, float(samples) * 0.02)
            cluster_code = str(cluster.get("contract_code", "")).strip().lower()
            if clean_contract_code and cluster_code == clean_contract_code:
                score += 0.16
            cluster_provider_score[provider] = max(cluster_provider_score.get(provider, -1.0), score)

        contract_raw = context.get("last_failure_external_contract", {})
        contract = contract_raw if isinstance(contract_raw, dict) else {}
        checks_raw = contract.get("checks", [])
        checks = [dict(row) for row in checks_raw if isinstance(row, dict)] if isinstance(checks_raw, list) else []
        retry_after_s = self._clamp_float(contract.get("retry_after_s", 0.0), minimum=0.0, maximum=86_400.0, default=0.0)
        blocked_ratio = self._clamp_float(contract.get("blocked_ratio", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        auth_pressure = 0.0
        cooldown_pressure = 0.0
        outage_pressure = 0.0
        for row in checks[:24]:
            check_name = str(row.get("check", "")).strip().lower()
            status = str(row.get("status", "")).strip().lower()
            if status not in {"failed", "warning"}:
                continue
            if check_name.startswith("auth_"):
                auth_pressure = min(1.0, auth_pressure + (0.5 if status == "failed" else 0.25))
            elif "cooldown" in check_name:
                cooldown_pressure = min(1.0, cooldown_pressure + (0.45 if status == "failed" else 0.22))
            elif "outage" in check_name:
                outage_pressure = min(1.0, outage_pressure + (0.52 if status == "failed" else 0.24))

        runtime_reliability_codes = {
            "provider_cooldown_blocked",
            "provider_outage_blocked",
            "provider_runtime_blocked",
        }
        ranked: List[tuple[float, int, float, str, Dict[str, Any]]] = []
        for tool_name, tool_args, priority, confidence in rows:
            action_name = str(tool_name or "").strip().lower()
            if not action_name:
                continue
            args_payload = self._sanitize_args(tool_args if isinstance(tool_args, dict) else {})
            provider = self._normalize_provider(str(args_payload.get("provider", "")).strip())
            score = self._clamp_float(confidence, minimum=0.0, maximum=1.0, default=0.0) * 0.9
            score += (1.0 / float(max(1, int(priority)))) * 0.32

            if action_name == "external_connector_preflight":
                score += 0.28
            elif action_name == "external_connector_status":
                score += 0.24
            elif action_name == "oauth_token_maintain":
                score += 0.2
            elif action_name == "oauth_token_refresh":
                score += 0.16

            if clean_contract_code in runtime_reliability_codes:
                score += 0.12
                if action_name in {"external_connector_status", "external_connector_preflight"}:
                    score += 0.1 + (blocked_ratio * 0.14)
                if action_name in {"oauth_token_maintain", "oauth_token_refresh"}:
                    score += min(0.16, (retry_after_s / 120.0) * 0.1 + (cooldown_pressure * 0.12) + (outage_pressure * 0.12))
            if volatility_mode in {"surging", "elevated"}:
                if action_name in {"external_connector_status", "external_connector_preflight"}:
                    score += 0.08 + (volatility_index * 0.1)
                elif action_name in {"oauth_token_maintain", "oauth_token_refresh"}:
                    score += 0.05 + (volatility_index * 0.08)
            elif volatility_mode == "calm":
                if action_name == "external_connector_preflight":
                    score -= 0.03
            if at_risk_ratio >= 0.4 and action_name in {"external_connector_status", "oauth_token_maintain"}:
                score += min(0.12, at_risk_ratio * 0.18)

            if provider:
                if allowed_set and provider not in allowed_set:
                    score -= 0.44
                if provider in blocked_set:
                    if action_name in {"oauth_token_maintain", "oauth_token_refresh", "external_connector_status"}:
                        score += 0.08
                    else:
                        score -= 0.32
                provider_risk = self._clamp_float(risk_by_provider.get(provider, 0.0), minimum=0.0, maximum=1.0, default=0.0)
                if action_name in {"external_connector_status", "external_connector_preflight"}:
                    score += provider_risk * 0.16
                elif action_name in {"oauth_token_maintain", "oauth_token_refresh"}:
                    score += provider_risk * 0.11
                score += self._clamp_float(memory_provider_success.get(provider, 0.0), minimum=-1.0, maximum=1.0, default=0.0) * 0.24
                score += self._clamp_float(cluster_provider_score.get(provider, 0.0), minimum=-1.0, maximum=1.0, default=0.0) * 0.2
            else:
                if action_name in {"external_connector_status", "external_connector_preflight"}:
                    score += 0.04
                if allowed_set and len(allowed_set) == 1:
                    only_provider = next(iter(allowed_set))
                    args_payload["provider"] = only_provider
                    provider = only_provider
                    score += 0.08

            if action_name in {"oauth_token_maintain", "oauth_token_refresh"}:
                score += auth_pressure * 0.22
            if action_name in {"external_connector_status", "external_connector_preflight"}:
                score += (cooldown_pressure * 0.12) + (outage_pressure * 0.14)

            ranked.append((score, int(priority), float(confidence), action_name, args_payload))

        if not ranked:
            return rows
        dedup: Dict[str, tuple[float, int, float, str, Dict[str, Any]]] = {}
        for row in ranked:
            score, priority, confidence, action_name, args_payload = row
            signature = f"{action_name}|{json.dumps(args_payload, ensure_ascii=True, sort_keys=True, separators=(',', ':'))}"
            previous = dedup.get(signature)
            if previous is None or score > previous[0]:
                dedup[signature] = row
        ordered = sorted(
            dedup.values(),
            key=lambda row: (-float(row[0]), int(row[1]), -float(row[2]), str(row[3])),
        )
        return [
            (
                str(row[3]).strip().lower(),
                dict(row[4]) if isinstance(row[4], dict) else {},
                int(row[1]),
                self._clamp_float(row[2], minimum=0.0, maximum=1.0, default=0.0),
            )
            for row in ordered
        ]

    def _infer_external_field_value(self, *, action: str, field: str, text: str, args: Dict[str, Any]) -> Any:
        clean_field = str(field or "").strip().lower()
        clean_action = str(action or "").strip().lower()
        if clean_field == "provider":
            current = self._normalize_provider(str(args.get("provider", "")).strip())
            return current or "auto"
        if clean_field == "message_id":
            return self._extract_message_id(text)
        if clean_field == "event_id":
            return self._extract_event_id(text)
        if clean_field == "document_id":
            return self._extract_document_id(text)
        if clean_field == "task_id":
            return self._extract_task_id(text)
        if clean_field == "to":
            emails = self._extract_email_addresses(text)
            return emails
        if clean_field == "subject":
            return self._extract_email_subject(text)
        if clean_field == "body":
            return self._extract_email_body(text)
        if clean_field == "title":
            if clean_action.startswith("external_calendar_"):
                return self._extract_calendar_title(text)
            if clean_action.startswith("external_doc_"):
                return self._extract_document_title(text)
            if clean_action.startswith("external_task_"):
                return self._extract_task_title(text)
            return self._extract_email_subject(text)
        if clean_field == "content":
            content = self._extract_optional_content(text) or self._extract_content(text)
            return content
        if clean_field == "notes":
            return self._extract_optional_content(text)
        if clean_field == "status":
            return self._extract_task_status(text)
        if clean_field in {"start", "end"}:
            start_value, end_value = self._extract_datetime_window(text)
            if clean_field == "start":
                return start_value
            return end_value
        if clean_field == "attendees":
            return self._extract_email_addresses(text)
        return ""

    def _external_identifier_recovery_step(self, *, action: str, repaired_args: Dict[str, Any]) -> Optional[PlanStep]:
        clean_action = str(action or "").strip().lower()
        if clean_action == "external_email_read" and not self._payload_has_value(repaired_args.get("message_id")):
            return self._step(
                "external_email_list",
                args={
                    "provider": self._normalize_provider(str(repaired_args.get("provider", "auto")).strip()) or "auto",
                    "max_results": 20,
                },
                verify={"expect_status": "success"},
            )
        if clean_action in {"external_doc_read", "external_doc_update"} and not self._payload_has_value(repaired_args.get("document_id")):
            return self._step(
                "external_doc_list",
                args={
                    "provider": self._normalize_provider(str(repaired_args.get("provider", "auto")).strip()) or "auto",
                    "max_results": 20,
                },
                verify={"expect_status": "success"},
            )
        if clean_action == "external_task_update" and not self._payload_has_value(repaired_args.get("task_id")):
            return self._step(
                "external_task_list",
                args={
                    "provider": self._normalize_provider(str(repaired_args.get("provider", "auto")).strip()) or "auto",
                    "max_results": 25,
                    "include_completed": True,
                },
                verify={"expect_status": "success"},
            )
        if clean_action == "external_calendar_update_event" and not self._payload_has_value(repaired_args.get("event_id")):
            return self._step(
                "external_calendar_list_events",
                args={
                    "provider": self._normalize_provider(str(repaired_args.get("provider", "auto")).strip()) or "auto",
                    "max_results": 20,
                },
                verify={"expect_status": "success"},
            )
        return None

    @staticmethod
    def _payload_has_value(value: Any) -> bool:
        if isinstance(value, str):
            return bool(value.strip())
        if isinstance(value, (list, tuple, set)):
            return any(Planner._payload_has_value(item) for item in value)
        if isinstance(value, dict):
            return any(Planner._payload_has_value(item) for item in value.values())
        return value is not None

    def _step(
        self,
        action: str,
        *,
        args: Dict[str, Any] | None = None,
        depends_on: List[str] | None = None,
        verify: Dict[str, Any] | None = None,
        can_retry: bool = True,
        max_retries: int = 2,
        timeout_s: int = 30,
    ) -> PlanStep:
        step_args = args or {}
        verify_template = self._verification_template(action, step_args)
        verify_rules = self._merge_verify_rules(verify_template, verify or {})
        return PlanStep(
            step_id=self._step_id(),
            action=action,
            args=step_args,
            depends_on=[str(item).strip() for item in (depends_on or []) if str(item).strip()],
            verify=verify_rules,
            can_retry=can_retry,
            max_retries=max_retries,
            timeout_s=timeout_s,
        )

    @staticmethod
    def _step_id() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def _extract_app_name(text: str) -> str:
        match = re.search(r"(?:open|launch|start app|run app)\s+(.+)$", text, flags=re.IGNORECASE)
        if not match:
            return "notepad"
        value = match.group(1).strip().strip(".")
        value = re.sub(r"^(the|app)\s+", "", value, flags=re.IGNORECASE)
        return value or "notepad"

    @staticmethod
    def _extract_media_query(text: str) -> str:
        match = re.search(r"(?:play|search)\s+(.+)$", text, flags=re.IGNORECASE)
        if match:
            value = match.group(1).strip().strip(".")
            if value:
                return value
        return text.strip() or "music"

    @staticmethod
    def _extract_url(text: str) -> str:
        match = re.search(r"https?://[^\s]+", text, flags=re.IGNORECASE)
        if not match:
            return ""
        return match.group(0).rstrip(".,)")

    @staticmethod
    def _extract_domain_like(text: str) -> str:
        match = re.search(r"\b[a-zA-Z0-9-]+\.[a-zA-Z]{2,}(?:/[^\s]*)?\b", text)
        if not match:
            return ""
        return match.group(0).rstrip(".,)")

    @staticmethod
    def _extract_session_id(text: str) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted and len(quoted) >= 8:
            return quoted.strip()
        patterns = (
            r"(?:session id|session)\s*[:=]?\s*([a-zA-Z0-9-]{8,})",
            r"\b([a-fA-F0-9]{8}-[a-fA-F0-9-]{27,})\b",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = str(match.group(1)).strip().strip(".,)")
                if value:
                    return value
        return ""

    @staticmethod
    def _extract_message_id(text: str) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted and len(quoted) >= 6:
            return quoted.strip()
        match = re.search(r"(?:message id|email id|mail id)\s*[:=]?\s*([A-Za-z0-9._\-]+)", text, flags=re.IGNORECASE)
        if match:
            return str(match.group(1)).strip().strip(".,)")
        return ""

    @staticmethod
    def _extract_event_id(text: str) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted and len(quoted) >= 4:
            return quoted.strip()
        match = re.search(r"(?:event id|calendar id|meeting id)\s*[:=]?\s*([A-Za-z0-9._\-]+)", text, flags=re.IGNORECASE)
        if match:
            return str(match.group(1)).strip().strip(".,)")
        return ""

    @staticmethod
    def _extract_document_id(text: str) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted and len(quoted) >= 4:
            return quoted.strip()
        match = re.search(r"(?:document id|doc id|file id)\s*[:=]?\s*([A-Za-z0-9._\-]+)", text, flags=re.IGNORECASE)
        if match:
            return str(match.group(1)).strip().strip(".,)")
        return ""

    @staticmethod
    def _extract_task_id(text: str) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted and len(quoted) >= 3 and " " not in quoted:
            return quoted.strip()
        match = re.search(r"(?:task id|todo id|to do id)\s*[:=]?\s*([A-Za-z0-9._\-]+)", text, flags=re.IGNORECASE)
        if match:
            return str(match.group(1)).strip().strip(".,)")
        return ""

    @staticmethod
    def _extract_task_title(text: str) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted:
            return quoted[:180]
        match = re.search(r"(?:create task|add task|new task|todo add|to do add)\s+(.+)$", text, flags=re.IGNORECASE)
        if match:
            value = match.group(1).strip().strip(".")
            if value:
                return value[:180]
        return ""

    @staticmethod
    def _extract_task_status(text: str) -> str:
        status_match = re.search(r"status\s*[:=]\s*([A-Za-z_ -]+)", text, flags=re.IGNORECASE)
        if status_match:
            value = status_match.group(1).strip().lower().replace(" ", "_")
            if value:
                return value
        lowered = str(text or "").lower()
        if any(token in lowered for token in ("complete task", "mark task done", "task done", "finish task", "close task")):
            return "completed"
        if any(token in lowered for token in ("reopen task", "mark task open", "task pending", "task not done")):
            return "not_started"
        return ""

    @staticmethod
    def _extract_window_title(text: str) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted:
            return quoted
        match = re.search(r"(?:focus window|switch to window|bring window)\s+(.+)$", text, flags=re.IGNORECASE)
        if not match:
            return ""
        return match.group(1).strip().strip(".")

    def _extract_explicit_desktop_target_context(
        self,
        *,
        text: str,
        arguments: Optional[Dict[str, Any]] = None,
    ) -> tuple[str, str]:
        args = arguments if isinstance(arguments, dict) else {}
        normalized_text = str(text or "")
        lowered_text = normalized_text.lower()

        app_name = str(args.get("app_name") or args.get("app") or "").strip()
        if not app_name:
            open_and_action = re.search(
                r"\b(?:open|launch|start)\b\s+(?P<app>.+?)\s+and\s+(?P<verb>type|click|press|hotkey|navigate|go to|search|focus search box|open search box|focus find box|open find box|run command|command|quick open|open file|focus address bar|address bar|open new tab|new tab|switch(?: to)?(?: the)?\s+(?:next|previous|prev|last|first|final|\d+(?:st|nd|rd|th)?|tab\s+\d+)\s+tab|switch(?: to)?\s+.+?\s+tab|go to\s+.+?\s+tab|focus\s+.+?\s+tab|next tab|previous tab|last tab|close tab|reopen tab|restore tab|open history|history|open downloads|downloads|open devtools|developer tools|devtools|open bookmarks|bookmarks|go back|back|go forward|forward|open chat|new chat|open conversation|jump to conversation|switch conversation|send message|message|reply all|reply to all|reply|forward email|new email|compose email|draft email|new event|new meeting|schedule meeting|open calendar|calendar view|open mail|mail view|open inbox|open contacts|contacts view|open tasks|tasks view|focus folder pane|focus message list|focus reading pane|focus preview pane|focus sidebar|open sidebar|focus main content|content area|main pane|document area|focus toolbar|command bar|menu bar|focus form|open form|focus\s+.+?\s+(?:field|input|text box|textbox|edit box)|set\s+.+?\s+to\s+.+?|fill\s+.+?\s+with\s+.+?|open\s+.+?\s+(?:dropdown|combo box)|select\s+.+?\s+in\s+.+?\s+(?:dropdown|combo box)|focus\s+.+?\s+(?:checkbox|check box)|check\s+.+?\s+(?:checkbox|check box)|uncheck\s+.+?\s+(?:checkbox|check box)|select\s+.+?\s+(?:radio button|radio option)|focus\s+.+?\s+(?:slider|spinner|stepper|value control|number input|numeric field)|(?:increase|decrease|raise|lower)\s+.+?\s+(?:slider|spinner|stepper|value(?: control)?|number input|numeric field)|(?:turn on|turn off|enable|disable|switch on|switch off)\s+.+?(?:\s+(?:switch|toggle))?|toggle\s+.+?\s+(?:switch|toggle)|open context menu|context menu|shortcut menu|right click menu|dismiss dialog|close dialog|cancel dialog|dismiss popup|close popup|confirm dialog|accept dialog|ok dialog|press ok|new document|save document|save file|open print dialog|print dialog|print|start presentation|start slideshow|slideshow|play\s*/\s*pause|play pause|toggle playback|toggle media|pause|resume|continue playback|next track|next song|skip track|skip song|previous track|prev track|last track|stop playback|stop media|stop music|new folder|create folder|focus folder tree|focus navigation pane|focus file list|focus items view|rename(?:\s+the)?\s+(?:selected\s+)?(?:file|folder|item|selection)|open properties|show properties|properties dialog|open preview pane|preview pane|open details pane|details pane|refresh|refresh view|reload|go up|parent folder|focus explorer|open explorer|workspace search|search workspace|find in files|find and replace|replace|go to symbol|symbol search|rename symbol|toggle terminal|open terminal|format document|format file|format code|zoom in|zoom out|reset zoom|actual size|normal size|run terminal command|run shell command|execute terminal command|execute shell command|run|execute)\b",
                normalized_text,
                flags=re.IGNORECASE,
            )
            if open_and_action:
                candidate_app = str(open_and_action.group("app") or "").strip().strip(".")
                if self._is_viable_desktop_target_candidate(candidate_app):
                    app_name = candidate_app
        if not app_name:
            context_patterns = (
                r"\b(?:expand|open|select|choose|focus)\s+.+?\s+in\s+(?:the\s+)?(?:tree|navigation tree|tree view)\s+in\s+(.+)$",
                r"\b(?:select|choose|click|focus)\s+.+?\s+in\s+(?:the\s+)?(?:list|results list|list view|list surface|list pane)\s+in\s+(.+)$",
                r"\b(?:select|choose|click|focus)\s+.+?(?:\s+row)?\s+in\s+(?:the\s+)?(?:table|grid|data grid)\s+in\s+(.+)$",
                r"\b(?:expand|open|select|choose|focus)\s+.+?\s+in\s+(device manager|event viewer|registry editor|regedit)\b$",
                r"\b(?:select|choose|click|focus)\s+.+?(?:\s+row)?\s+in\s+(task manager|resource monitor)\b$",
                r"\b(?:open|show|go to|switch to|select|focus)\s+.+?\s+in\s+(?:the\s+)?(?:sidebar|side panel)\s+in\s+(.+)$",
                r"\b(?:click|press|select|choose|invoke|trigger|run)\s+.+?\s+in\s+(?:the\s+)?(?:toolbar|command bar|menu bar)\s+in\s+(.+)$",
                r"\b(?:focus|open)\s+(?:the\s+)?(?:form|form surface)\s+in\s+(.+)$",
                r"\b(?:apply|save|submit|commit)\s+(?:the\s+)?(?:settings|changes|form|dialog|options|properties)(?:\s+(?:page|step))?\s+in\s+(.+)$",
                r"\b(?:continue|work|move|run|go)\s+(?:through|across)\s+(?:the\s+)?(?:form|dialog|settings|options|properties)\s+in\s+(.+)$",
                r"\b(?:apply|save|submit|finish|complete)\s+(?:the\s+)?(?:form|dialog|settings|options|properties)\s+(?:flow|all the way)\s+in\s+(.+)$",
                r"\b(?:focus|open|select)\s+.+?\s+(?:field|input|text box|textbox|edit box)\s+in\s+(.+)$",
                r"\b(?:set|fill|enter)\s+.+?\s+(?:to|with)\s+.+?\s+in\s+(.+)$",
                r"\b(?:open|show|focus)\s+.+?\s+(?:dropdown|combo box)\s+in\s+(.+)$",
                r"\b(?:select|choose)\s+.+?\s+in\s+.+?\s+(?:dropdown|combo box)\s+in\s+(.+)$",
                r"\b(?:focus|check|uncheck)\s+.+?\s+(?:checkbox|check box)\s+in\s+(.+)$",
                r"\b(?:select|choose|pick)\s+.+?\s+(?:radio button|radio option)\s+in\s+(.+)$",
                r"\b(?:focus|open|select)\s+.+?\s+(?:slider|spinner|stepper|value control|number input|numeric field)\s+in\s+(.+)$",
                r"\b(?:increase|decrease|raise|lower)\s+.+?(?:\s+(?:slider|spinner|stepper|value(?: control)?|number input|numeric field))?(?:\s+by\s+.+?)?\s+in\s+(.+)$",
                r"\b(?:turn on|turn off|enable|disable|switch on|switch off)\s+.+?(?:\s+(?:switch|toggle))?\s+in\s+(.+)$",
                r"\b(?:toggle)\s+.+?\s+(?:switch|toggle)\s+in\s+(.+)$",
                r"\b(?:click|press|select|choose|invoke|open)\s+.+?\s+in\s+(?:the\s+)?(?:context menu|shortcut menu|right click menu)\s+in\s+(.+)$",
                r"\b(?:press|click|select|choose|confirm|accept)\s+.+?(?:\s+button)?\s+in\s+(?:the\s+)?(?:dialog|popup|modal)\s+in\s+(.+)$",
                r"\b(?:open|show|go to|switch to|select|focus)\s+.+?\s+in\s+(settings|task manager|control panel|event viewer|device manager)\b$",
                r"\b(?:click|press)\s+.+?\s+in\s+(.+)$",
                r"\b(?:focus search box|open search box|focus find box|open find box)\s+in\s+(.+)$",
                r"\b(?:search(?: for)?|find|focus search box|open search box|focus find box|open find box)\s+.+?\s+in\s+(.+)$",
                r"\b(?:navigate(?: to)?|go to|browse to)\s+.+?\s+in\s+(.+)$",
                r"\b(?:run command|execute command|open command palette|command palette)\s+.+?\s+in\s+(.+)$",
                r"\b(?:quick open|open file|switch to file|go to file)\s+.+?\s+in\s+(.+)$",
                r"\b(?:open history|history|open downloads|downloads|open devtools|developer tools|devtools|open bookmarks|bookmarks|open tab search|show tab search|focus tab search|go back|back|go forward|forward|focus address bar|address bar|focus search box|open search box|focus find box|open find box|new folder|create folder|focus folder tree|focus navigation pane|focus file list|focus items view|focus sidebar|open sidebar|focus main content|content area|main pane|document area|focus toolbar|command bar|menu bar|open context menu|context menu|shortcut menu|right click menu|dismiss dialog|close dialog|cancel dialog|dismiss popup|close popup|confirm dialog|accept dialog|ok dialog|press ok|open properties|show properties|properties dialog|open preview pane|preview pane|open details pane|details pane|refresh|refresh view|reload|go up|parent folder)\s+in\s+(.+)$",
                r"\b(?:switch(?: to)?(?: the)?\s+(?:next|previous|prev|last|first|final|\d+(?:st|nd|rd|th)?\s+tab|tab\s+\d+)|next tab|previous tab|prev tab|last tab)\s+in\s+(.+)$",
                r"\b(?:switch(?: to)?|go to|focus|open)\s+.+?\s+tab\s+in\s+(.+)$",
                r"\b(?:new chat|start new chat|new conversation|start conversation)\s+in\s+(.+)$",
                r"\b(?:open chat with|new chat with|open conversation with|jump to conversation|switch conversation|send message(?: to)?|message(?: to)?|reply(?: to)?)\s+.+?\s+in\s+(.+?)(?:\s+(?:saying|with message|message:|text:)\b.*)?$",
                r"\b(?:new email|compose email|draft email|new mail|compose mail|reply all|reply to all|reply|forward email|new calendar event|create calendar event|new event|create event|new meeting|schedule meeting|create meeting|open calendar|calendar view|open mail|mail view|open inbox|open people|people view|open contacts|contacts view|open tasks|open to do|open todo|tasks view|to do view|todo view|focus folder pane|focus message list|focus reading pane|focus preview pane)\s+in\s+(.+)$",
                r"\b(?:new document|save document|save file|save workbook|save presentation|open print dialog|print dialog|print|start presentation|start slideshow|slideshow)\s+in\s+(.+)$",
                r"\b(?:play\s*/\s*pause|play pause|toggle playback|toggle media|pause(?: playback| music| media| song)?|resume(?: playback| music| media| song)?|continue playback|continue music|next track|next song|skip track|skip song|previous track|prev track|last track|previous song|stop(?: playback| music| media| song)?)\s+in\s+(.+)$",
                r"\b(?:rename(?:\s+the)?\s+(?:selected\s+)?(?:file|folder|item|selection)(?:\s+to)?)\s+.+?\s+in\s+(.+)$",
                r"\b(?:focus explorer|open explorer|workspace search(?: for)?|search workspace(?: for)?|find in files|find and replace|replace|go to symbol|symbol search|rename symbol(?: to)?)\s+.+?\s+in\s+(.+)$",
                r"\b(?:focus explorer|open explorer|toggle terminal|open terminal|format document|format file|format code)\s+in\s+(.+)$",
                r"\b(?:zoom in|zoom out|reset zoom|actual size|normal size)\s+in\s+(.+)$",
                r"\b(?:run terminal command|execute terminal command|run shell command|execute shell command|run|execute)\s+.+?\s+in\s+(.+)$",
                r"\b(?:open new tab|new tab|close tab|reopen tab|restore tab|restore closed tab)\s+in\s+(.+)$",
            )
            for pattern in context_patterns:
                context_match = re.search(pattern, normalized_text, flags=re.IGNORECASE)
                if not context_match:
                    continue
                candidate_app = str(context_match.group(1) or "").strip().strip(".")
                if self._is_viable_desktop_target_candidate(candidate_app):
                    app_name = candidate_app
                    break

        window_title = str(args.get("window_title") or "").strip()
        if not window_title and "window" in lowered_text:
            window_title = self._extract_window_title(normalized_text)

        return (app_name, window_title)

    def _is_viable_desktop_target_candidate(self, value: str) -> bool:
        candidate = str(value or "").strip().strip(".")
        if not candidate:
            return False
        if re.search(r"['\"]", candidate):
            return False
        if re.search(r"\b\d+\s+(?:second|seconds|minute|minutes|hour|hours|day|days)\b", candidate, flags=re.IGNORECASE):
            return False
        if self._looks_like_action_clause(candidate.lower()):
            return False
        return self._is_probable_desktop_app_name(candidate)

    @staticmethod
    def _extract_desktop_context_from_step(step: PlanStep) -> Dict[str, str]:
        if not isinstance(step.args, dict):
            return {}

        app_name = ""
        window_title = ""
        if step.action == "open_app":
            app_name = str(step.args.get("app_name") or "").strip()
        elif step.action == "desktop_interact":
            app_name = str(step.args.get("app_name") or "").strip()
            window_title = str(step.args.get("window_title") or "").strip()

        context: Dict[str, str] = {}
        if app_name:
            context["app_name"] = app_name
        if window_title:
            context["window_title"] = window_title
        return context

    @staticmethod
    def _steps_use_desktop_context(steps: List[PlanStep], desktop_context: Dict[str, Any]) -> bool:
        context_app_name = str(desktop_context.get("app_name") or "").strip().lower()
        context_window_title = str(desktop_context.get("window_title") or "").strip().lower()
        if not context_app_name and not context_window_title:
            return False

        for step in steps:
            if step.action != "desktop_interact" or not isinstance(step.args, dict):
                continue
            step_app_name = str(step.args.get("app_name") or "").strip().lower()
            step_window_title = str(step.args.get("window_title") or "").strip().lower()
            if context_app_name and step_app_name == context_app_name:
                return True
            if context_window_title and step_window_title == context_window_title:
                return True
        return False

    @staticmethod
    def _looks_like_desktop_followup_clause(lowered: str, arguments: Optional[Dict[str, Any]] = None) -> bool:
        if isinstance(arguments, dict) and str(arguments.get("action") or "").strip():
            return True

        followup_markers = (
            "click ",
            "press ",
            "type ",
            "hotkey",
            "shortcut",
            "navigate",
            "go to ",
            "browse to",
            "search ",
            "find ",
            "search box",
            "find box",
            "run command",
            "execute command",
            "command palette",
            "quick open",
            "open file",
            "switch to file",
            "go to file",
            "address bar",
            "bookmarks",
            "history",
            "downloads",
            "devtools",
            "developer tools",
            "go back",
            "go forward",
            "tab search",
            "search tabs",
            "search open tabs",
            "open chat",
            "new chat",
            "start new chat",
            "new conversation",
            "start conversation",
            "conversation",
            "message",
            "reply",
            "new email",
            "compose email",
            "draft email",
            "new mail",
            "compose mail",
            "reply all",
            "reply to all",
            "forward email",
            "new calendar event",
            "new event",
            "new meeting",
            "schedule meeting",
            "focus folder pane",
            "focus message list",
            "focus reading pane",
            "focus preview pane",
            "expand ",
            "focus tree",
            "navigation tree",
            "tree view",
            "in tree",
            "focus list",
            "results list",
            "list surface",
            "list pane",
            "in list",
            "focus table",
            "data grid",
            "in table",
            "row in",
            "focus sidebar",
            "open sidebar",
            "in sidebar",
            "in side panel",
            "focus main content",
            "content area",
            "main pane",
            "document area",
            "focus toolbar",
            "command bar",
            "menu bar",
            "focus form",
            "form surface",
            "input field",
            "text box",
            "edit box",
            "set ",
            "fill ",
            "dropdown",
            "combo box",
            "checkbox",
            "check box",
            "radio button",
            "radio option",
            "tab page",
            "property tab",
            "settings tab",
            "slider",
            "spinner",
            "stepper",
            "value control",
            "number input",
            "numeric field",
            "increase ",
            "decrease ",
            "raise ",
            "lower ",
            "toggle switch",
            "turn on ",
            "turn off ",
            "enable ",
            "disable ",
            "switch on ",
            "switch off ",
            "in toolbar",
            "in command bar",
            "in menu bar",
            "context menu",
            "shortcut menu",
            "right click menu",
            "dismiss dialog",
            "close dialog",
            "cancel dialog",
            "dismiss popup",
            "close popup",
            "confirm dialog",
            "accept dialog",
            "ok dialog",
            "press ok",
            "apply settings",
            "save settings",
            "save changes",
            "apply changes",
            "complete settings page",
            "complete settings flow",
            "settings flow",
            "complete form page",
            "complete form flow",
            "dialog flow",
            "continue installer",
            "continue installation",
            "continue setup",
            "continue through installer",
            "continue through installation",
            "continue through setup",
            "advance installer",
            "advance setup",
            "complete installer page",
            "complete setup page",
            "installer flow",
            "setup flow",
            "wizard flow",
            "to the end",
            "all the way",
            "next step",
            "previous step",
            "prior step",
            "finish installer",
            "finish wizard",
            "complete setup",
            "in dialog",
            "button in dialog",
            "new document",
            "save document",
            "save file",
            "save workbook",
            "save presentation",
            "print dialog",
            "print",
            "start presentation",
            "slideshow",
            "slide show",
            "calendar view",
            "open calendar",
            "people view",
            "contacts view",
            "open people",
            "open contacts",
            "tasks view",
            "open tasks",
            "open to do",
            "open todo",
            "focus folder tree",
            "focus navigation pane",
            "focus file list",
            "focus items view",
            "mail view",
            "open mail",
            "open inbox",
            "play pause",
            "play/pause",
            "toggle playback",
            "toggle media",
            "pause ",
            "resume ",
            "continue playback",
            "continue music",
            "next track",
            "next song",
            "skip track",
            "skip song",
            "previous track",
            "prev track",
            "last track",
            "previous song",
            "stop playback",
            "stop media",
            "stop music",
            "new folder",
            "create folder",
            "rename selected",
            "open properties",
            "show properties",
            "properties dialog",
            "preview pane",
            "details pane",
            "refresh",
            "reload",
            "go up",
            "parent folder",
            "focus explorer",
            "workspace search",
            "search workspace",
            "find in files",
            "find and replace",
            "replace ",
            "go to symbol",
            "symbol search",
            "rename symbol",
            "rename to",
            "toggle terminal",
            "open terminal",
            "format document",
            "format file",
            "format code",
            "run terminal command",
            "run shell command",
            "execute terminal command",
            "execute shell command",
            "new tab",
            "next tab",
            "previous tab",
            "switch to tab",
            "switch tab",
            "close tab",
            "reopen tab",
            "restore tab",
            "zoom in",
            "zoom out",
            "reset zoom",
            "actual size",
            "normal size",
        )
        if any(marker in lowered for marker in followup_markers):
            return True
        if re.search(r"\b(?:set|fill|enter)\s+.+?\s+(?:to|with)\s+.+$", lowered):
            return True
        return bool(re.search(r"\b(?:switch(?: to)?|go to|focus|open)\s+.+?\s+tab\b", lowered))

    @staticmethod
    def _extract_notification_message(text: str) -> str:
        match = re.search(r"(?:notify|notification|remind me)\s*(?:to)?\s*(.+)$", text, flags=re.IGNORECASE)
        if not match:
            return text.strip() or "Notification from JARVIS"
        value = match.group(1).strip().strip(".")
        return value or "Notification from JARVIS"

    @staticmethod
    def _extract_process_name(text: str) -> str:
        match = re.search(r"(?:kill process|terminate process|end process)\s+(.+)$", text, flags=re.IGNORECASE)
        if not match:
            return ""
        value = match.group(1).strip().strip(".")
        return value.replace(".exe", "").strip()

    @staticmethod
    def _extract_keyword(text: str) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted:
            return quoted
        match = re.search(r"(?:search text|find text|contains text|grep)\s+(.+)$", text, flags=re.IGNORECASE)
        if not match:
            return ""
        return match.group(1).strip().strip(".")

    @staticmethod
    def _extract_desktop_search_query(text: str) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted:
            return quoted
        patterns = (
            r"(?:search(?: for)?|find)\s+(.+?)(?:\s+in\s+.+)?$",
            r"(?:look for)\s+(.+?)(?:\s+in\s+.+)?$",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = match.group(1).strip().strip(".")
                if value:
                    return value
        return ""

    @staticmethod
    def _extract_desktop_command_text(text: str) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted:
            return quoted
        patterns = (
            r"(?:run command|execute command)\s+(.+?)(?:\s+in\s+.+)?$",
            r"(?:command palette)\s+(.+?)(?:\s+in\s+.+)?$",
            r"(?:open command palette)\s+(.+?)(?:\s+in\s+.+)?$",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = match.group(1).strip().strip(".")
                if value:
                    return value
        return ""

    @staticmethod
    def _extract_desktop_quick_open_query(text: str) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted:
            return quoted
        patterns = (
            r"(?:quick open|switch to file|go to file)\s+(.+?)(?:\s+in\s+.+)?$",
            r"(?:open file)\s+(.+?)(?:\s+in\s+.+)?$",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = match.group(1).strip().strip(".")
                if value:
                    return value
        return ""

    @staticmethod
    def _extract_desktop_tab_target(text: str) -> str:
        lowered = " ".join(str(text or "").strip().lower().split())
        if not lowered:
            return ""
        alias_map = {
            "next tab": "next",
            "previous tab": "previous",
            "prev tab": "previous",
            "last tab": "last",
            "switch to next tab": "next",
            "switch to previous tab": "previous",
            "switch to prev tab": "previous",
            "switch to last tab": "last",
            "switch tab": "",
            "first tab": "1",
        }
        for phrase, target in alias_map.items():
            if phrase and phrase in lowered and target:
                return target
        if re.search(r"\b(?:next|forward|following|right)\s+tab\b", lowered):
            return "next"
        if re.search(r"\b(?:previous|prev|prior|back|left)\s+tab\b", lowered):
            return "previous"
        if re.search(r"\b(?:last|final|end)\s+tab\b", lowered):
            return "last"
        if re.search(r"\b(?:first|1st|one)\s+tab\b", lowered):
            return "1"
        tab_match = re.search(r"\b(?:switch(?: to)?|go to|focus)\s+(?:the\s+)?tab\s+([1-9])\b", lowered)
        if tab_match:
            return str(tab_match.group(1))
        ordinal_match = re.search(r"\b(?:switch(?: to)?|go to|focus)\s+(?:the\s+)?([1-9])(?:st|nd|rd|th)?\s+tab\b", lowered)
        if ordinal_match:
            return str(ordinal_match.group(1))
        standalone_match = re.search(r"\btab\s+([1-9])\b", lowered)
        if standalone_match:
            return str(standalone_match.group(1))
        return ""

    @staticmethod
    def _is_named_tab_query(value: str) -> bool:
        clean = " ".join(str(value or "").strip().lower().split())
        if not clean:
            return False
        if clean in {"next", "previous", "prev", "last", "first", "final", "back", "forward", "new", "current"}:
            return False
        if re.fullmatch(r"(?:tab\s+)?[1-9](?:st|nd|rd|th)?", clean):
            return False
        if re.fullmatch(r"(?:first|second|third|fourth|fifth|sixth|seventh|eighth|ninth)\s+tab", clean):
            return False
        return True

    @staticmethod
    def _extract_desktop_tab_search_query(text: str) -> str:
        lowered = " ".join(str(text or "").strip().lower().split())
        if not any(
            marker in lowered
            for marker in (
                "search tabs for",
                "search open tabs for",
                "find tab ",
                "find tabs ",
                "find open tab ",
                "find open tabs ",
                "switch to ",
                "go to ",
                "focus ",
                "open ",
            )
        ) or "tab" not in lowered:
            return ""
        quoted = Planner._extract_quoted(text)
        if quoted and Planner._is_named_tab_query(quoted):
            return quoted
        patterns = (
            r"(?:search(?:\s+open)?\s+tabs?\s+for)\s+(.+?)(?:\s+in\s+.+)?$",
            r"(?:find(?:\s+open)?\s+tabs?)\s+(.+?)(?:\s+in\s+.+)?$",
            r"(?:switch(?:\s+to)?|go to|focus|open)\s+(?:the\s+)?(.+?)\s+tab(?:\s+in\s+.+)?$",
            r"(?:switch(?:\s+to)?|go to|focus)\s+(?:the\s+)?tab\s+(.+?)(?:\s+in\s+.+)?$",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = str(match.group(1) or "").strip().strip(".").strip("\"'")
                if value and Planner._is_named_tab_query(value):
                    return value
        return ""

    @staticmethod
    def _extract_desktop_tab_page_query(text: str, *, app_name: str = "") -> str:
        del app_name
        patterns = [
            r"(?:open|show|switch to|select|focus)\s+(.+?)\s+tab(?:\s+in\s+.+)?$",
            r"(?:open|show|switch to|select|focus)\s+(?:the\s+)?tab\s+(.+?)(?:\s+in\s+.+)?$",
        ]
        blocked_values = {
            "tab",
            "next",
            "previous",
            "prev",
            "last",
            "first",
            "new",
            "current",
        }
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            value = Planner._clean_desktop_control_query(match.group(1))
            if value and value.lower() not in blocked_values and Planner._is_named_tab_query(value):
                return value
        return ""

    @staticmethod
    def _extract_desktop_conversation_query(text: str) -> str:
        patterns = (
            r"(?:open chat with|new chat with|open conversation with|jump to conversation|switch conversation|switch to chat|switch to conversation|chat with|conversation with|dm with)\s+(.+?)(?:\s+in\s+.+)?$",
            r"(?:send message to|message to|reply to)\s+(.+?)(?:\s+(?:in\s+.+|saying|with message|message:|text:)\b|$)",
            r"(?:send message|message|reply)\s+(.+?)(?:\s+(?:in\s+.+|saying|with message|message:|text:)\b|$)",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = str(match.group(1) or "").strip().strip(".").strip("\"'")
                if value:
                    return value
        return ""

    @staticmethod
    def _extract_desktop_message_text(text: str) -> str:
        patterns = (
            r"(?:saying|with message|reply with|message:|text:)\s+(.+)$",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = str(match.group(1) or "").strip().strip(".").strip("\"'")
                if value:
                    return value
        quoted_values = [item for item in re.findall(r"['\"]([^'\"]+)['\"]", text) if str(item).strip()]
        if quoted_values:
            return str(quoted_values[-1]).strip()
        return ""

    @staticmethod
    def _clean_desktop_control_query(value: str) -> str:
        clean = str(value or "").strip().strip(".").strip("\"'")
        clean = re.sub(r"^(?:the|a|an)\s+", "", clean, flags=re.IGNORECASE)
        clean = re.sub(r"\s+button$", "", clean, flags=re.IGNORECASE)
        return Planner._strip_desktop_followup_suffix(clean).strip()

    @staticmethod
    def _strip_desktop_followup_suffix(value: str) -> str:
        clean = str(value or "").strip()
        if not clean:
            return ""
        clean = re.sub(
            r"\s+(?:and|then|after that)\s+(?:apply|save|submit|commit|complete|finish)\s+(?:the\s+)?(?:settings|changes|form|dialog|options|properties)(?:\s+(?:page|flow|all the way|to the end))?$",
            "",
            clean,
            flags=re.IGNORECASE,
        )
        clean = re.sub(
            r"\s+(?:and|then|after that)\s+(?:apply|save)\s+changes$",
            "",
            clean,
            flags=re.IGNORECASE,
        )
        return clean.strip()

    @staticmethod
    def _extract_desktop_field_query(text: str, *, app_name: str = "") -> str:
        patterns = [
            r"(?:focus|open|select)\s+(.+?)\s+(?:field|input|text box|textbox|edit box)(?:\s+in\s+.+)?$",
            r"(?:set|fill)\s+(.+?)\s+(?:field|input|text box|textbox|edit box)\s+(?:to|with)\s+.+?(?:\s+in\s+.+)?$",
            r"(?:enter|type)\s+.+?\s+in\s+(.+?)\s+(?:field|input|text box|textbox|edit box)(?:\s+in\s+.+)?$",
        ]
        if app_name and Planner._is_probable_form_app_name(app_name):
            patterns.append(r"(?:set|fill)\s+(.+?)\s+(?:to|with)\s+.+?(?:\s+in\s+.+)?$")
        blocked_values = {
            "field",
            "input",
            "text box",
            "textbox",
            "edit box",
            "dropdown",
            "combo box",
            "checkbox",
            "check box",
            "switch",
        }
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            value = Planner._clean_desktop_control_query(match.group(1))
            if value and value.lower() not in blocked_values:
                return value
        return ""

    @staticmethod
    def _extract_desktop_field_value(text: str, *, app_name: str = "") -> str:
        quoted_values = [str(item).strip() for item in re.findall(r"['\"]([^'\"]+)['\"]", text) if str(item).strip()]
        if len(quoted_values) >= 2:
            return quoted_values[-1]
        patterns = (
            r"(?:set|fill)\s+.+?(?:\s+(?:field|input|text box|textbox|edit box))?\s+(?:to|with)\s+(.+?)(?:\s+in\s+.+)?$",
            r"(?:enter|type)\s+(.+?)\s+in\s+.+?\s+(?:field|input|text box|textbox|edit box)(?:\s+in\s+.+)?$",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            value = str(match.group(1) or "").strip().strip(".").strip("\"'")
            if app_name:
                suffix = f" in {app_name.strip()}".lower()
                lowered_value = value.lower()
                if lowered_value.endswith(suffix):
                    value = value[: -len(suffix)].strip().strip(".")
            value = Planner._strip_desktop_followup_suffix(value)
            if value:
                return value
        if quoted_values:
            return quoted_values[-1]
        return ""

    @staticmethod
    def _extract_desktop_dropdown_query(text: str, *, app_name: str = "") -> str:
        patterns = [
            r"(?:open|show|focus)\s+(.+?)\s+(?:dropdown|combo box)(?:\s+in\s+.+)?$",
            r"(?:select|choose)\s+.+?\s+in\s+(.+?)\s+(?:dropdown|combo box)(?:\s+in\s+.+)?$",
        ]
        if app_name and Planner._is_probable_form_app_name(app_name):
            patterns.append(r"(?:open|show|focus)\s+(.+?)(?:\s+in\s+.+)?$")
        blocked_values = {
            "dropdown",
            "combo box",
            "field",
            "input",
            "checkbox",
        }
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            value = Planner._clean_desktop_control_query(match.group(1))
            if value and value.lower() not in blocked_values:
                return value
        return ""

    @staticmethod
    def _extract_desktop_dropdown_option(text: str, *, app_name: str = "") -> str:
        del app_name
        patterns = (
            r"(?:select|choose)\s+(.+?)\s+in\s+.+?\s+(?:dropdown|combo box)(?:\s+in\s+.+)?$",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            value = Planner._strip_desktop_followup_suffix(str(match.group(1) or "").strip().strip(".").strip("\"'"))
            if value:
                return value
        quoted_values = [str(item).strip() for item in re.findall(r"['\"]([^'\"]+)['\"]", text) if str(item).strip()]
        if quoted_values:
            return quoted_values[0]
        return ""

    @staticmethod
    def _extract_desktop_checkbox_query(text: str) -> str:
        patterns = (
            r"(?:focus|check|uncheck)\s+(.+?)\s+(?:checkbox|check box)(?:\s+in\s+.+)?$",
        )
        blocked_values = {"checkbox", "check box", "switch", "toggle"}
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            value = Planner._clean_desktop_control_query(match.group(1))
            if value and value.lower() not in blocked_values:
                return value
        return ""

    @staticmethod
    def _extract_desktop_radio_option_query(text: str) -> str:
        patterns = (
            r"(?:select|choose|pick)\s+(.+?)\s+(?:radio button|radio option)(?:\s+in\s+.+)?$",
        )
        blocked_values = {"radio", "radio button", "radio option", "option"}
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            value = Planner._clean_desktop_control_query(match.group(1))
            if value and value.lower() not in blocked_values:
                return value
        return ""

    @staticmethod
    def _extract_desktop_toggle_query(text: str) -> str:
        patterns = (
            r"(?:toggle)\s+(.+?)\s+(?:switch|toggle)(?:\s+in\s+.+)?$",
            r"(?:turn on|turn off|enable|disable)\s+(.+?)(?:\s+(?:switch|toggle))?(?:\s+in\s+.+)?$",
        )
        blocked_values = {"switch", "toggle"}
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            value = Planner._clean_desktop_control_query(match.group(1))
            if value and value.lower() not in blocked_values:
                return value
        return ""

    @staticmethod
    def _extract_desktop_value_control_query(text: str, *, app_name: str = "") -> str:
        patterns = [
            r"(?:focus|open|select)\s+(.+?)\s+(?:slider|spinner|stepper|value control|number input|numeric field)(?:\s+in\s+.+)?$",
            r"(?:increase|decrease|raise|lower)\s+(.+?)\s+(?:slider|spinner|stepper|value(?: control)?|number input|numeric field)(?:\s+by\s+.+)?(?:\s+in\s+.+)?$",
            r"(?:set|adjust|move)\s+(.+?)\s+(?:slider|spinner|stepper|value(?: control)?|number input|numeric field)(?:\s+to\s+.+)?(?:\s+in\s+.+)?$",
        ]
        if app_name and Planner._is_probable_form_app_name(app_name):
            patterns.append(
                r"(?:increase|decrease|raise|lower)\s+(.+?)(?:\s+by\s+.+)?(?:\s+in\s+.+)?$"
            )
        blocked_values = {
            "slider",
            "spinner",
            "stepper",
            "value",
            "value control",
            "number input",
            "numeric field",
        }
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            value = Planner._clean_desktop_control_query(match.group(1))
            if value and value.lower() not in blocked_values:
                return value
        return ""

    @staticmethod
    def _extract_desktop_value_control_target(text: str, *, app_name: str = "") -> str:
        del app_name
        patterns = (
            r"(?:set|adjust|move)\s+.+?(?:\s+(?:slider|spinner|stepper|value(?: control)?|number input|numeric field))?\s+to\s+(.+?)(?:\s+in\s+.+)?$",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            value = Planner._strip_desktop_followup_suffix(str(match.group(1) or "").strip().strip(".").strip("\"'"))
            if value:
                return value
        quoted_values = [str(item).strip() for item in re.findall(r"['\"]([^'\"]+)['\"]", text) if str(item).strip()]
        if quoted_values:
            return quoted_values[-1]
        return ""

    @staticmethod
    def _extract_desktop_adjust_amount(text: str) -> int:
        match = re.search(r"\bby\s+(\d+)\b", text, flags=re.IGNORECASE)
        if not match:
            match = re.search(r"\b(\d+)\s+times\b", text, flags=re.IGNORECASE)
        if match:
            try:
                return max(1, min(int(match.group(1)), 20))
            except Exception:
                return 1
        word_map = {
            "one": 1,
            "two": 2,
            "three": 3,
            "four": 4,
            "five": 5,
            "six": 6,
            "seven": 7,
            "eight": 8,
            "nine": 9,
            "ten": 10,
        }
        word_match = re.search(
            r"\bby\s+(one|two|three|four|five|six|seven|eight|nine|ten)\b",
            text,
            flags=re.IGNORECASE,
        )
        if word_match:
            return word_map.get(str(word_match.group(1) or "").strip().lower(), 1)
        return 1

    @staticmethod
    def _extract_desktop_sidebar_item_query(text: str, *, app_name: str = "") -> str:
        patterns = [
            r"(?:open|show|go to|switch to|select|focus)\s+(.+?)\s+in\s+(?:the\s+)?(?:sidebar|side panel)(?:\s+in\s+.+)?$",
        ]
        if app_name and Planner._is_probable_sidebar_navigation_app_name(app_name):
            patterns.append(r"(?:open|show|go to|switch to|select|focus)\s+(.+?)(?:\s+in\s+.+)?$")
        blocked_values = {
            "sidebar",
            "side panel",
            "main content",
            "content area",
            "main pane",
            "document area",
            "toolbar",
            "command bar",
            "menu bar",
            "context menu",
            "shortcut menu",
            "right click menu",
            "dialog",
            "popup",
            "modal",
        }
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            value = Planner._clean_desktop_control_query(match.group(1))
            if value and value.lower() not in blocked_values:
                return value
        return ""

    @staticmethod
    def _extract_desktop_toolbar_action_query(text: str) -> str:
        patterns = (
            r"(?:click|press|select|choose|invoke|trigger|run)\s+(.+?)\s+in\s+(?:the\s+)?(?:toolbar|command bar|menu bar)(?:\s+in\s+.+)?$",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = Planner._clean_desktop_control_query(match.group(1))
                if value:
                    return value
        return ""

    @staticmethod
    def _extract_desktop_context_menu_item_query(text: str) -> str:
        patterns = (
            r"(?:click|press|select|choose|invoke|open)\s+(.+?)\s+in\s+(?:the\s+)?(?:context menu|shortcut menu|right click menu)(?:\s+in\s+.+)?$",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = Planner._clean_desktop_control_query(match.group(1))
                if value:
                    return value
        return ""

    @staticmethod
    def _extract_desktop_dialog_button_query(text: str) -> str:
        patterns = (
            r"(?:press|click|select|choose|confirm|accept)\s+(.+?)(?:\s+button)?\s+in\s+(?:the\s+)?(?:dialog|popup|modal)(?:\s+in\s+.+)?$",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = Planner._clean_desktop_control_query(match.group(1))
                if value:
                    return value
        return ""

    @staticmethod
    def _extract_desktop_wizard_app_name(text: str) -> str:
        if re.search(
            r"\b(?:open|launch|start)\s+(?:the\s+)?(?:setup wizard|installation wizard|install wizard|installer|setup|wizard)\b\s+(?:and|then)\b",
            text,
            flags=re.IGNORECASE,
        ):
            return ""
        match = re.search(r"\b(setup wizard|installation wizard|install wizard|installer|setup|wizard)\b", text, flags=re.IGNORECASE)
        if not match:
            return ""
        return str(match.group(1) or "").strip().lower()

    @staticmethod
    def _extract_desktop_tree_item_query(text: str, *, app_name: str = "") -> str:
        patterns = [
            r"(?:expand|open|select|choose|focus)\s+(.+?)\s+in\s+(?:the\s+)?(?:tree|navigation tree|tree view)(?:\s+in\s+.+)?$",
        ]
        if app_name and Planner._is_probable_tree_navigation_app_name(app_name):
            patterns.extend(
                [
                    r"(?:expand|open|select|choose|focus)\s+(.+?)(?:\s+in\s+.+)?$",
                ]
            )
        blocked_values = {
            "tree",
            "navigation tree",
            "tree view",
            "list",
            "table",
            "grid",
            "sidebar",
        }
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            value = Planner._clean_desktop_control_query(match.group(1))
            if value and value.lower() not in blocked_values:
                return value
        return ""

    @staticmethod
    def _extract_desktop_list_item_query(text: str) -> str:
        patterns = (
            r"(?:select|choose|click|focus)\s+(.+?)\s+in\s+(?:the\s+)?(?:list|results list|list view|list surface|list pane)(?:\s+in\s+.+)?$",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = Planner._clean_desktop_control_query(match.group(1))
                if value:
                    return value
        return ""

    @staticmethod
    def _extract_desktop_table_row_query(text: str, *, app_name: str = "") -> str:
        patterns = [
            r"(?:select|choose|click|focus)\s+(.+?)(?:\s+row)?\s+in\s+(?:the\s+)?(?:table|grid|data grid)(?:\s+in\s+.+)?$",
            r"(?:select|choose|click|focus)\s+(.+?)\s+row(?:\s+in\s+.+)?$",
        ]
        if app_name and Planner._is_probable_table_surface_app_name(app_name):
            patterns.append(r"(?:select|choose|click|focus)\s+(.+?)(?:\s+in\s+.+)?$")
        blocked_values = {
            "table",
            "grid",
            "data grid",
            "row",
        }
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            value = Planner._clean_desktop_control_query(match.group(1))
            if value and value.lower() not in blocked_values:
                return value
        return ""

    @staticmethod
    def _extract_desktop_workspace_search_query(text: str) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted:
            return quoted
        patterns = (
            r"(?:search workspace for|workspace search for|find in files)\s+(.+?)(?:\s+in\s+.+)?$",
            r"(?:search workspace|workspace search)\s+(.+?)(?:\s+in\s+.+)?$",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = match.group(1).strip().strip(".")
                if value:
                    return value
        return ""

    @staticmethod
    def _extract_desktop_replace_terms(text: str, *, app_name: str = "") -> tuple[str, str]:
        quoted_values = [str(item).strip() for item in re.findall(r"['\"]([^'\"]+)['\"]", text) if str(item).strip()]
        if len(quoted_values) >= 2:
            return (quoted_values[0], quoted_values[1])

        patterns = (
            r"(?:find and replace|replace)\s+(.+?)\s+with\s+(.+)$",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            query_value = str(match.group(1) or "").strip().strip(".").strip("\"'")
            replacement_value = str(match.group(2) or "").strip().strip(".").strip("\"'")
            if app_name:
                suffix = f" in {app_name.strip()}".lower()
                lowered_replacement = replacement_value.lower()
                if lowered_replacement.endswith(suffix):
                    replacement_value = replacement_value[: -len(suffix)].strip().strip(".")
            if query_value and replacement_value:
                return (query_value, replacement_value)
        return ("", "")

    @staticmethod
    def _extract_desktop_selection_rename_text(text: str) -> str:
        quoted_values = [str(item).strip() for item in re.findall(r"['\"]([^'\"]+)['\"]", text) if str(item).strip()]
        if quoted_values:
            return quoted_values[-1]
        patterns = (
            r"(?:rename(?:\s+the)?\s+(?:selected\s+)?(?:file|folder|item|selection)\s+to)\s+(.+?)(?:\s+in\s+.+)?$",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = str(match.group(1) or "").strip().strip(".").strip("\"'")
                if value:
                    return value
        return ""

    @staticmethod
    def _extract_desktop_symbol_query(text: str) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted:
            return quoted
        patterns = (
            r"(?:go to symbol|find symbol|open symbol)\s+(.+?)(?:\s+in\s+.+)?$",
            r"(?:symbol search)\s+(.+?)(?:\s+in\s+.+)?$",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = match.group(1).strip().strip(".")
                if value:
                    return value
        return ""

    @staticmethod
    def _extract_desktop_rename_text(text: str) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted:
            return quoted
        patterns = (
            r"(?:rename symbol to|rename to)\s+(.+?)(?:\s+in\s+.+)?$",
            r"(?:rename symbol)\s+(.+?)(?:\s+in\s+.+)?$",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = match.group(1).strip().strip(".")
                if value:
                    return value
        return ""

    @staticmethod
    def _extract_desktop_terminal_command_text(text: str, *, permissive: bool = False) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted:
            return quoted
        patterns = [
            r"(?:run terminal command|execute terminal command|run shell command|execute shell command)\s+(.+?)(?:\s+in\s+.+)?$",
            r"(?:run in terminal|execute in terminal)\s+(.+?)(?:\s+in\s+.+)?$",
        ]
        if permissive:
            patterns.extend(
                [
                    r"(?:open|launch|start)\s+.+?\s+and\s+(?:run|execute)\s+(.+)$",
                    r"(?:run|execute)\s+(.+?)(?:\s+in\s+.+)?$",
                ]
            )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = match.group(1).strip().strip(".")
                if value:
                    return value
        return ""

    @staticmethod
    def _extract_phrase(text: str) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted:
            return quoted
        match = re.search(
            r"(?:is text visible|find text on screen|screen contains text|click text|click on text|wait for text|find text targets|find ui element|locate ui element|click ui element|invoke ui element|activate ui element|find button|click button|list ui elements)\s+(.+)$",
            text,
            flags=re.IGNORECASE,
        )
        if not match:
            return ""
        return match.group(1).strip().strip(".")

    @staticmethod
    def _extract_file_pattern(text: str) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted and any(token in quoted for token in ("*", "?", ".")):
            return quoted
        match = re.search(r"(?:find file|search files|locate file)\s+(.+?)(?:\s+in\s+.+)?$", text, flags=re.IGNORECASE)
        if not match:
            return ""
        candidate = match.group(1).strip().strip(".")
        if any(token in candidate for token in ("*", "?", ".")):
            return candidate
        return ""

    @staticmethod
    def _extract_content(text: str) -> str:
        match = re.search(r"(?:content|text)\s*[:=]\s*(.+)$", text, flags=re.IGNORECASE)
        if match:
            value = match.group(1).strip()
            if value:
                return value
        return "Generated by JARVIS."

    @staticmethod
    def _extract_optional_content(text: str) -> str:
        match = re.search(r"(?:content|text)\s*[:=]\s*(.+)$", text, flags=re.IGNORECASE)
        if not match:
            return ""
        value = match.group(1).strip()
        return value

    @staticmethod
    def _extract_clipboard_text(text: str) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted:
            return quoted
        match = re.search(
            r"(?:copy to clipboard|set clipboard|clipboard write|clipboard copy|type text|type this|type)\s+(.+)$",
            text,
            flags=re.IGNORECASE,
        )
        if not match:
            return ""
        value = match.group(1).strip().strip(".")
        return value

    @staticmethod
    def _extract_hotkey_keys(text: str) -> List[str]:
        quoted = Planner._extract_quoted(text)
        source = quoted or text
        match = re.search(r"(?:hotkey|shortcut|press key)\s+(.+)$", source, flags=re.IGNORECASE)
        value = match.group(1) if match else source
        keys = [part.strip().lower() for part in re.split(r"[+, ]+", value) if part.strip()]
        return keys[:5]

    @staticmethod
    def _is_probable_desktop_app_name(value: str) -> bool:
        clean = str(value or "").strip().strip(".")
        if not clean:
            return False
        lowered = clean.lower()
        if any(token in clean for token in ("\\", "/", ":")):
            return False
        blocked = {
            "desktop",
            "documents",
            "downloads",
            "folder",
            "directory",
            "filesystem",
            "file system",
            "workspace",
            "project",
            "repo",
            "repository",
            "web",
            "website",
            "internet",
            "screen",
            "window",
            "tab",
        }
        return lowered not in blocked

    @staticmethod
    def _is_probable_terminal_app_name(value: str) -> bool:
        lowered = " ".join(str(value or "").strip().lower().split())
        return any(
            token in lowered
            for token in (
                "powershell",
                "pwsh",
                "windows terminal",
                "terminal",
                "command prompt",
                "cmd",
                "warp",
                "tabby",
                "hyper",
            )
        )

    @staticmethod
    def _is_probable_browser_app_name(value: str) -> bool:
        lowered = " ".join(str(value or "").strip().lower().split())
        return any(
            token in lowered
            for token in (
                "chrome",
                "google chrome",
                "edge",
                "microsoft edge",
                "brave",
                "firefox",
                "opera",
                "vivaldi",
                "browser",
            )
        )

    @staticmethod
    def _is_probable_file_manager_app_name(value: str) -> bool:
        lowered = " ".join(str(value or "").strip().lower().split())
        return any(
            token in lowered
            for token in (
                "file explorer",
                "windows explorer",
                "explorer",
            )
        )

    @staticmethod
    def _is_probable_chat_app_name(value: str) -> bool:
        lowered = " ".join(str(value or "").strip().lower().split())
        return any(
            token in lowered
            for token in (
                "slack",
                "discord",
                "telegram",
                "whatsapp",
                "teams",
                "signal",
                "messenger",
            )
        )

    @staticmethod
    def _is_probable_office_app_name(value: str) -> bool:
        lowered = " ".join(str(value or "").strip().lower().split())
        return any(
            token in lowered
            for token in (
                "word",
                "excel",
                "powerpoint",
                "onenote",
                "outlook",
                "office",
                "mail",
                "calendar",
                "notion",
            )
        )

    @staticmethod
    def _is_probable_mail_app_name(value: str) -> bool:
        lowered = " ".join(str(value or "").strip().lower().split())
        return any(
            token in lowered
            for token in (
                "outlook",
                "mail",
                "proton mail",
                "thunderbird",
            )
        )

    @staticmethod
    def _is_probable_media_app_name(value: str) -> bool:
        lowered = " ".join(str(value or "").strip().lower().split())
        return any(
            token in lowered
            for token in (
                "spotify",
                "vlc",
                "obs",
                "media player",
                "musicbee",
                "itunes",
                "winamp",
                "potplayer",
                "kodi",
                "foobar",
                "plex",
            )
        )

    @staticmethod
    def _is_probable_sidebar_navigation_app_name(value: str) -> bool:
        lowered = " ".join(str(value or "").strip().lower().split())
        return any(
            token in lowered
            for token in (
                "settings",
                "task manager",
                "control panel",
                "event viewer",
                "device manager",
            )
        )

    @staticmethod
    def _is_probable_tree_navigation_app_name(value: str) -> bool:
        lowered = " ".join(str(value or "").strip().lower().split())
        return any(
            token in lowered
            for token in (
                "device manager",
                "event viewer",
                "registry editor",
                "regedit",
            )
        )

    @staticmethod
    def _is_probable_table_surface_app_name(value: str) -> bool:
        lowered = " ".join(str(value or "").strip().lower().split())
        return any(
            token in lowered
            for token in (
                "task manager",
                "resource monitor",
                "performance monitor",
            )
        )

    @staticmethod
    def _is_probable_form_app_name(value: str) -> bool:
        lowered = " ".join(str(value or "").strip().lower().split())
        return any(
            token in lowered
            for token in (
                "settings",
                "control panel",
                "installer",
                "setup",
                "wizard",
                "preferences",
                "options",
                "properties",
                "account",
                "sign in",
                "login",
            )
        )

    @staticmethod
    def _is_probable_editor_app_name(value: str) -> bool:
        lowered = " ".join(str(value or "").strip().lower().split())
        return any(
            token in lowered
            for token in (
                "vscode",
                "visual studio code",
                "visual studio",
                "pycharm",
                "intellij",
                "cursor",
                "zed",
                "notepad++",
                "sublime",
                "android studio",
                "webstorm",
                "rider",
            )
        )

    @staticmethod
    def _extract_email_addresses(text: str) -> List[str]:
        matches = re.findall(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Za-z]{2,}\b", text)
        deduped: List[str] = []
        for item in matches:
            clean = item.strip()
            if clean and clean not in deduped:
                deduped.append(clean)
        return deduped[:20]

    @staticmethod
    def _extract_email_subject(text: str) -> str:
        subject_match = re.search(r"subject\s*[:=]\s*(.+?)(?:\s+body\s*[:=]|$)", text, flags=re.IGNORECASE)
        if subject_match:
            value = subject_match.group(1).strip().strip(".")
            if value:
                return value
        quoted = Planner._extract_quoted(text)
        if quoted:
            return quoted[:180]
        return "Message from JARVIS"

    @staticmethod
    def _extract_email_body(text: str) -> str:
        body_match = re.search(r"body\s*[:=]\s*(.+)$", text, flags=re.IGNORECASE)
        if body_match:
            value = body_match.group(1).strip()
            if value:
                return value
        return Planner._extract_content(text)

    @staticmethod
    def _extract_calendar_title(text: str) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted:
            return quoted[:180]
        match = re.search(r"(?:create calendar event|schedule meeting|add to calendar|calendar event)\s+(.+)$", text, flags=re.IGNORECASE)
        if not match:
            return "JARVIS Event"
        value = match.group(1).strip().strip(".")
        if " at " in value.lower():
            value = re.split(r"\s+at\s+", value, maxsplit=1, flags=re.IGNORECASE)[0]
        return value[:180] or "JARVIS Event"

    @staticmethod
    def _extract_document_title(text: str) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted:
            return quoted[:180]
        match = re.search(r"(?:create document|new document|create doc|write doc)\s+(.+)$", text, flags=re.IGNORECASE)
        if match:
            value = match.group(1).strip().strip(".")
            if value:
                return value[:180]
        return "JARVIS Document"

    @staticmethod
    def _extract_datetime_window(text: str) -> tuple[str, str]:
        matches = re.findall(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2})?(?:Z|[+-]\d{2}:\d{2})?", text)
        if len(matches) >= 2:
            return (matches[0], matches[1])
        if len(matches) == 1:
            start_raw = matches[0]
            try:
                start_dt = datetime.fromisoformat(start_raw.replace("Z", "+00:00"))
                if start_dt.tzinfo is None:
                    start_dt = start_dt.replace(tzinfo=timezone.utc)
                end_dt = start_dt + timedelta(minutes=30)
                return (start_dt.isoformat(), end_dt.isoformat())
            except Exception:
                return (start_raw, "")
        return ("", "")

    @staticmethod
    def _extract_coordinates(text: str) -> tuple[int | None, int | None]:
        match = re.search(r"\b(?:x\s*[:=]?\s*)?(-?\d{1,5})\s*[, ]\s*(?:y\s*[:=]?\s*)?(-?\d{1,5})\b", text, flags=re.IGNORECASE)
        if not match:
            return (None, None)
        try:
            return (int(match.group(1)), int(match.group(2)))
        except Exception:
            return (None, None)

    @staticmethod
    def _extract_script_name(text: str) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted:
            return Path(quoted).name
        match = re.search(r"(?:run trusted script|execute trusted script)\s+(.+)$", text, flags=re.IGNORECASE)
        if not match:
            return ""
        return Path(match.group(1).strip().strip(".")).name

    @staticmethod
    def _extract_path(text: str) -> str:
        quoted = Planner._extract_quoted(text)
        if quoted and ("\\" in quoted or "/" in quoted or "." in Path(quoted).name):
            return quoted

        win_match = re.search(r"([A-Za-z]:\\[^\n\r\"']+)", text)
        if win_match:
            return win_match.group(1).strip().rstrip(".,)")

        rel_match = re.search(r"\b([\w./\\-]+\.[A-Za-z0-9]{1,8})\b", text)
        if rel_match:
            return rel_match.group(1).strip()
        return ""

    @staticmethod
    def _extract_two_paths(text: str) -> tuple[str, str]:
        quoted = re.findall(r'"([^"]+)"|\'([^\']+)\'', text)
        values = [item[0] or item[1] for item in quoted if (item[0] or item[1])]
        if len(values) >= 2:
            return (values[0], values[1])

        paths = re.findall(r"([A-Za-z]:\\[^\n\r\"']+)", text)
        if len(paths) >= 2:
            return (paths[0].strip().rstrip(".,)"), paths[1].strip().rstrip(".,)"))
        return ("", "")

    @staticmethod
    def _extract_timezone(text: str) -> str:
        match = re.search(r"(UTC|GMT|[A-Za-z_]+/[A-Za-z_]+)", text)
        if not match:
            return "UTC"
        return match.group(1)

    @staticmethod
    def _extract_quoted(text: str) -> str:
        match = re.search(r'"([^"]+)"', text)
        if match:
            return match.group(1).strip()
        match = re.search(r"'([^']+)'", text)
        if match:
            return match.group(1).strip()
        return ""

