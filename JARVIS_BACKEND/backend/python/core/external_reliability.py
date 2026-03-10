from __future__ import annotations

import hashlib
import json
import math
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List


class ExternalReliabilityOrchestrator:
    """
    Adaptive reliability controller for external connector actions.

    Features:
    - provider-aware cooldown windows after repeated/transient failures
    - action contract preflight checks for high-value connector operations
    - retry profile hints derived from provider health trends
    - persisted state for cross-run stability learning
    """

    _MANAGED_PREFIXES = ("external_", "oauth_token_")

    _ACTION_CONTRACTS: Dict[str, Dict[str, Any]] = {
        "external_email_send": {"required_any": [["to"]]},
        "external_email_read": {"required_any": [["message_id"]]},
        "external_calendar_update_event": {
            "required_all": ["event_id"],
            "required_any": [["title", "description", "location", "start", "end", "attendees"]],
        },
        "external_doc_update": {
            "required_all": ["document_id"],
            "required_any": [["title", "content"]],
        },
        "external_task_update": {
            "required_all": ["task_id"],
            "required_any": [["title", "notes", "status", "due", "rank", "list_id"]],
        },
        "oauth_token_refresh": {"required_all": ["provider"]},
        "oauth_token_revoke": {"required_all": ["provider"]},
        "oauth_token_maintain": {"required_any": [["provider"], ["account_id"]]},
    }

    _ACTION_OPERATION_CLASS: Dict[str, str] = {
        "external_connector_status": "healthcheck",
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

    _MISSION_OUTAGE_PROFILE_ADJUSTMENTS: Dict[str, Dict[str, float]] = {
        "defensive": {
            "bias_gain": 0.22,
            "trip_delta": -0.028,
            "recover_delta": -0.02,
            "route_block_delta": -0.05,
            "preflight_block_delta": -0.048,
        },
        "cautious": {
            "bias_gain": 0.11,
            "trip_delta": -0.014,
            "recover_delta": -0.011,
            "route_block_delta": -0.026,
            "preflight_block_delta": -0.022,
        },
        "balanced": {
            "bias_gain": 0.0,
            "trip_delta": 0.0,
            "recover_delta": 0.0,
            "route_block_delta": 0.0,
            "preflight_block_delta": 0.0,
        },
        "throughput": {
            "bias_gain": -0.1,
            "trip_delta": 0.018,
            "recover_delta": 0.014,
            "route_block_delta": 0.026,
            "preflight_block_delta": 0.024,
        },
    }

    _ACTION_PROVIDER_RULES: Dict[str, Dict[str, Any]] = {
        "external_email_send": {"allow": ["google", "graph", "smtp"], "prefer": ["google", "graph", "smtp"]},
        "external_email_list": {"allow": ["google", "graph"], "prefer": ["google", "graph"]},
        "external_email_read": {"allow": ["google", "graph"], "prefer": ["google", "graph"]},
        "external_calendar_create_event": {"allow": ["google", "graph"], "prefer": ["google", "graph"]},
        "external_calendar_list_events": {"allow": ["google", "graph"], "prefer": ["google", "graph"]},
        "external_calendar_update_event": {"allow": ["google", "graph"], "prefer": ["google", "graph"]},
        "external_doc_create": {"allow": ["google", "graph"], "prefer": ["google", "graph"]},
        "external_doc_list": {"allow": ["google", "graph"], "prefer": ["google", "graph"]},
        "external_doc_read": {"allow": ["google", "graph"], "prefer": ["google", "graph"]},
        "external_doc_update": {"allow": ["google", "graph"], "prefer": ["google", "graph"]},
        "external_task_list": {"allow": ["google", "graph"], "prefer": ["google", "graph"]},
        "external_task_create": {"allow": ["google", "graph"], "prefer": ["google", "graph"]},
        "external_task_update": {"allow": ["google", "graph"], "prefer": ["google", "graph"]},
        "oauth_token_list": {"allow": ["google", "graph"], "prefer": ["google", "graph"]},
        "oauth_token_upsert": {"allow": ["google", "graph"], "prefer": ["google", "graph"]},
        "oauth_token_refresh": {"allow": ["google", "graph"], "prefer": ["google", "graph"]},
        "oauth_token_maintain": {"allow": ["google", "graph"], "prefer": ["google", "graph"]},
        "oauth_token_revoke": {"allow": ["google", "graph"], "prefer": ["google", "graph"]},
    }

    _PROVIDER_CAPABILITY_CONTRACTS: Dict[str, Dict[str, Any]] = {
        "google": {
            "capabilities": ["email", "calendar", "document", "task", "auth", "external"],
            "operation_classes": ["healthcheck", "read", "write", "mutate", "auth", "maintenance"],
        },
        "graph": {
            "capabilities": ["email", "calendar", "document", "task", "auth", "external"],
            "operation_classes": ["healthcheck", "read", "write", "mutate", "auth", "maintenance"],
        },
        "smtp": {
            "capabilities": ["email", "external"],
            "operation_classes": ["healthcheck", "write"],
        },
    }

    _ACTION_FIELD_SCHEMAS: Dict[str, Dict[str, Dict[str, Any]]] = {
        "external_email_send": {
            "to": {"type": "list", "required": True, "min_items": 1, "max_items": 80, "item_type": "string", "item_min_len": 3, "item_max_len": 320},
            "subject": {"type": "string", "min_len": 1, "max_len": 300},
            "body": {"type": "string", "min_len": 1, "max_len": 200000},
            "provider": {"type": "string", "allowed": ["auto", "google", "graph", "smtp"]},
            "thread_id": {"type": "string", "max_len": 160},
        },
        "external_email_list": {
            "provider": {"type": "string", "allowed": ["auto", "google", "graph"]},
            "max_results": {"type": "int", "min": 1, "max": 200},
            "query": {"type": "string", "max_len": 400},
        },
        "external_email_read": {
            "provider": {"type": "string", "allowed": ["auto", "google", "graph"]},
            "message_id": {"type": "string", "required": True, "min_len": 3, "max_len": 260},
        },
        "external_calendar_create_event": {
            "provider": {"type": "string", "allowed": ["auto", "google", "graph"]},
            "title": {"type": "string", "required": True, "min_len": 1, "max_len": 220},
            "start": {"type": "string", "iso_datetime": True},
            "end": {"type": "string", "iso_datetime": True},
            "timezone": {"type": "string", "max_len": 80},
            "attendees": {"type": "list", "max_items": 200, "item_type": "string", "item_min_len": 3, "item_max_len": 320},
        },
        "external_calendar_list_events": {
            "provider": {"type": "string", "allowed": ["auto", "google", "graph"]},
            "max_results": {"type": "int", "min": 1, "max": 200},
        },
        "external_calendar_update_event": {
            "provider": {"type": "string", "allowed": ["auto", "google", "graph"]},
            "event_id": {"type": "string", "required": True, "min_len": 2, "max_len": 260},
            "title": {"type": "string", "max_len": 220},
            "description": {"type": "string", "max_len": 6000},
            "location": {"type": "string", "max_len": 300},
            "start": {"type": "string", "iso_datetime": True},
            "end": {"type": "string", "iso_datetime": True},
            "attendees": {"type": "list", "max_items": 200, "item_type": "string", "item_min_len": 3, "item_max_len": 320},
        },
        "external_doc_create": {
            "provider": {"type": "string", "allowed": ["auto", "google", "graph"]},
            "title": {"type": "string", "required": True, "min_len": 1, "max_len": 220},
            "content": {"type": "string", "max_len": 300000},
        },
        "external_doc_list": {
            "provider": {"type": "string", "allowed": ["auto", "google", "graph"]},
            "max_results": {"type": "int", "min": 1, "max": 300},
            "query": {"type": "string", "max_len": 300},
        },
        "external_doc_read": {
            "provider": {"type": "string", "allowed": ["auto", "google", "graph"]},
            "document_id": {"type": "string", "required": True, "min_len": 2, "max_len": 260},
        },
        "external_doc_update": {
            "provider": {"type": "string", "allowed": ["auto", "google", "graph"]},
            "document_id": {"type": "string", "required": True, "min_len": 2, "max_len": 260},
            "title": {"type": "string", "max_len": 220},
            "content": {"type": "string", "max_len": 300000},
        },
        "external_task_list": {
            "provider": {"type": "string", "allowed": ["auto", "google", "graph"]},
            "max_results": {"type": "int", "min": 1, "max": 300},
            "include_completed": {"type": "bool"},
            "query": {"type": "string", "max_len": 300},
        },
        "external_task_create": {
            "provider": {"type": "string", "allowed": ["auto", "google", "graph"]},
            "title": {"type": "string", "required": True, "min_len": 1, "max_len": 320},
            "notes": {"type": "string", "max_len": 15000},
            "due": {"type": "string", "iso_datetime": True},
            "status": {"type": "string", "allowed": ["needsAction", "notStarted", "completed", "inProgress"]},
        },
        "external_task_update": {
            "provider": {"type": "string", "allowed": ["auto", "google", "graph"]},
            "task_id": {"type": "string", "required": True, "min_len": 1, "max_len": 260},
            "title": {"type": "string", "max_len": 320},
            "notes": {"type": "string", "max_len": 15000},
            "due": {"type": "string", "iso_datetime": True},
            "status": {"type": "string", "allowed": ["needsAction", "notStarted", "completed", "inProgress"]},
            "rank": {"type": "string", "max_len": 80},
            "list_id": {"type": "string", "max_len": 200},
        },
        "oauth_token_upsert": {
            "provider": {"type": "string", "required": True, "allowed": ["google", "graph"]},
            "access_token": {"type": "string", "required": True, "min_len": 10, "max_len": 8000},
            "refresh_token": {"type": "string", "max_len": 8000},
            "expires_at": {"type": "string", "iso_datetime": True},
            "scope": {"type": "string", "max_len": 4000},
            "account_id": {"type": "string", "max_len": 320},
        },
        "oauth_token_refresh": {
            "provider": {"type": "string", "required": True, "allowed": ["google", "graph"]},
            "account_id": {"type": "string", "max_len": 320},
        },
        "oauth_token_revoke": {
            "provider": {"type": "string", "required": True, "allowed": ["google", "graph"]},
            "account_id": {"type": "string", "max_len": 320},
        },
        "oauth_token_maintain": {
            "provider": {"type": "string", "allowed": ["google", "graph"]},
            "account_id": {"type": "string", "max_len": 320},
            "window_s": {"type": "int", "min": 30, "max": 172800},
            "limit": {"type": "int", "min": 1, "max": 5000},
        },
    }

    _ACTION_SCOPE_REQUIREMENTS: Dict[str, Dict[str, Dict[str, List[str]]]] = {
        "external_email_send": {
            "google": {"any_of": ["gmail.send", "mail.google.com", "gmail.modify"]},
            "graph": {"any_of": ["mail.send"]},
            "smtp": {"any_of": []},
        },
        "external_email_list": {
            "google": {"any_of": ["gmail.readonly", "gmail.modify", "mail.google.com"]},
            "graph": {"any_of": ["mail.read", "mail.readwrite"]},
        },
        "external_email_read": {
            "google": {"any_of": ["gmail.readonly", "gmail.modify", "mail.google.com"]},
            "graph": {"any_of": ["mail.read", "mail.readwrite"]},
        },
        "external_calendar_create_event": {
            "google": {"any_of": ["calendar.events", "calendar"]},
            "graph": {"any_of": ["calendars.readwrite"]},
        },
        "external_calendar_list_events": {
            "google": {"any_of": ["calendar.readonly", "calendar.events.readonly", "calendar"]},
            "graph": {"any_of": ["calendars.read", "calendars.readwrite"]},
        },
        "external_calendar_update_event": {
            "google": {"any_of": ["calendar.events", "calendar"]},
            "graph": {"any_of": ["calendars.readwrite"]},
        },
        "external_doc_create": {
            "google": {"any_of": ["documents", "drive.file", "drive"]},
            "graph": {"any_of": ["files.readwrite", "files.readwrite.all"]},
        },
        "external_doc_list": {
            "google": {"any_of": ["drive.readonly", "drive.metadata.readonly", "drive"]},
            "graph": {"any_of": ["files.read", "files.read.all", "files.readwrite"]},
        },
        "external_doc_read": {
            "google": {"any_of": ["documents.readonly", "documents", "drive.readonly", "drive"]},
            "graph": {"any_of": ["files.read", "files.read.all", "files.readwrite"]},
        },
        "external_doc_update": {
            "google": {"any_of": ["documents", "drive.file", "drive"]},
            "graph": {"any_of": ["files.readwrite", "files.readwrite.all"]},
        },
        "external_task_list": {
            "google": {"any_of": ["tasks.readonly", "tasks"]},
            "graph": {"any_of": ["tasks.read", "tasks.readwrite"]},
        },
        "external_task_create": {
            "google": {"any_of": ["tasks"]},
            "graph": {"any_of": ["tasks.readwrite"]},
        },
        "external_task_update": {
            "google": {"any_of": ["tasks"]},
            "graph": {"any_of": ["tasks.readwrite"]},
        },
        "oauth_token_refresh": {
            "google": {"any_of": ["openid", "offline_access"]},
            "graph": {"any_of": ["offline_access"]},
        },
        "oauth_token_maintain": {
            "google": {"any_of": ["offline_access"]},
            "graph": {"any_of": ["offline_access"]},
        },
    }

    _OPERATION_MIN_TOKEN_TTL_S: Dict[str, int] = {
        "healthcheck": 30,
        "read": 90,
        "write": 180,
        "mutate": 240,
        "auth": 300,
        "maintenance": 120,
        "default": 90,
    }

    _OPERATION_COOLDOWN_FACTOR: Dict[str, float] = {
        "healthcheck": 0.78,
        "read": 0.92,
        "write": 1.08,
        "mutate": 1.16,
        "auth": 1.24,
        "maintenance": 0.98,
        "default": 1.0,
    }

    _OPERATION_RETRY_FACTOR: Dict[str, float] = {
        "healthcheck": 0.75,
        "read": 0.9,
        "write": 1.12,
        "mutate": 1.2,
        "auth": 1.22,
        "maintenance": 1.0,
        "default": 1.0,
    }

    _CATEGORY_SIGNALS: Dict[str, float] = {
        "auth": 0.98,
        "rate_limited": 0.9,
        "timeout": 0.82,
        "transient": 0.74,
        "non_retryable": 0.94,
        "unknown": 0.66,
    }

    _PROVIDER_COOLDOWN_BASE: Dict[str, Dict[str, int]] = {
        "google": {"auth": 120, "rate_limited": 75, "timeout": 30, "transient": 24, "unknown": 18},
        "graph": {"auth": 150, "rate_limited": 90, "timeout": 36, "transient": 28, "unknown": 20},
        "smtp": {"auth": 60, "rate_limited": 28, "timeout": 20, "transient": 16, "unknown": 12},
        "default": {"auth": 90, "rate_limited": 60, "timeout": 28, "transient": 22, "unknown": 16},
    }

    @staticmethod
    def _mission_outage_policy_default_state() -> Dict[str, Any]:
        return {
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
            "profile_history": [],
            "capability_bias": {},
            "updated_at": "",
            "last_reason": "",
        }

    def __init__(
        self,
        *,
        store_path: str = "data/external_reliability.json",
        max_providers: int = 1200,
    ) -> None:
        self.store_path = Path(store_path)
        self.max_providers = max(60, min(int(max_providers), 20000))
        self.cooldown_enabled = self._env_flag("JARVIS_EXTERNAL_COOLDOWN_ENABLED", default=True)
        self.autotune_enabled = self._env_flag("JARVIS_EXTERNAL_RETRY_AUTOTUNE_ENABLED", default=True)
        self.max_cooldown_s = self._coerce_int(
            os.getenv("JARVIS_EXTERNAL_MAX_COOLDOWN_S", "900"),
            minimum=20,
            maximum=7200,
            default=900,
        )
        self.min_samples_for_hint = self._coerce_int(
            os.getenv("JARVIS_EXTERNAL_MIN_SAMPLES_FOR_HINT", "4"),
            minimum=1,
            maximum=500,
            default=4,
        )
        self.failure_ema_decay = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_FAILURE_EMA_DECAY", "0.88"),
            minimum=0.55,
            maximum=0.995,
            default=0.88,
        )
        self.action_failure_ema_decay = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_ACTION_FAILURE_EMA_DECAY", "0.84"),
            minimum=0.55,
            maximum=0.995,
            default=0.84,
        )
        self.provider_routing_enabled = self._env_flag("JARVIS_EXTERNAL_PROVIDER_ROUTING_ENABLED", default=True)
        self.recent_success_window_s = self._coerce_int(
            os.getenv("JARVIS_EXTERNAL_RECENT_SUCCESS_WINDOW_S", "240"),
            minimum=30,
            maximum=7200,
            default=240,
        )
        self.preflight_contract_strict = self._env_flag("JARVIS_EXTERNAL_CONTRACT_STRICT", default=True)
        self.preflight_provider_contract_strict = self._env_flag(
            "JARVIS_EXTERNAL_PROVIDER_CONTRACT_STRICT",
            default=True,
        )
        self.preflight_provider_capability_contract_strict = self._env_flag(
            "JARVIS_EXTERNAL_PROVIDER_CAPABILITY_CONTRACT_STRICT",
            default=True,
        )
        self.preflight_provider_capability_runtime_enabled = self._env_flag(
            "JARVIS_EXTERNAL_PROVIDER_CAPABILITY_RUNTIME_ENABLED",
            default=True,
        )
        self.route_preference_boost = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_ROUTE_PREFERENCE_BOOST", "0.08"),
            minimum=0.0,
            maximum=0.4,
            default=0.08,
        )
        self.route_operation_penalty_weight = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_ROUTE_OPERATION_PENALTY_WEIGHT", "0.22"),
            minimum=0.0,
            maximum=1.0,
            default=0.22,
        )
        self.route_entropy_enabled = self._env_flag(
            "JARVIS_EXTERNAL_ROUTE_ENTROPY_ENABLED",
            default=True,
        )
        self.route_entropy_temperature = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_ROUTE_ENTROPY_TEMPERATURE", "0.72"),
            minimum=0.15,
            maximum=2.5,
            default=0.72,
        )
        self.route_entropy_explore_base_probability = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_ROUTE_ENTROPY_EXPLORE_BASE_P", "0.08"),
            minimum=0.0,
            maximum=0.8,
            default=0.08,
        )
        self.route_entropy_explore_max_probability = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_ROUTE_ENTROPY_EXPLORE_MAX_P", "0.34"),
            minimum=0.01,
            maximum=0.95,
            default=0.34,
        )
        self.route_entropy_score_gap_threshold = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_ROUTE_ENTROPY_SCORE_GAP_THRESHOLD", "0.16"),
            minimum=0.01,
            maximum=0.5,
            default=0.16,
        )
        self.route_entropy_min_candidates = self._coerce_int(
            os.getenv("JARVIS_EXTERNAL_ROUTE_ENTROPY_MIN_CANDIDATES", "2"),
            minimum=2,
            maximum=12,
            default=2,
        )
        self.max_action_stats_per_provider = self._coerce_int(
            os.getenv("JARVIS_EXTERNAL_MAX_ACTION_STATS_PER_PROVIDER", "120"),
            minimum=8,
            maximum=1000,
            default=120,
        )
        self.max_operation_stats_per_provider = self._coerce_int(
            os.getenv("JARVIS_EXTERNAL_MAX_OPERATION_STATS_PER_PROVIDER", "24"),
            minimum=6,
            maximum=200,
            default=24,
        )
        self.failure_trend_decay = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_FAILURE_TREND_DECAY", "0.72"),
            minimum=0.35,
            maximum=0.99,
            default=0.72,
        )
        self.cooldown_trend_weight = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_COOLDOWN_TREND_WEIGHT", "0.32"),
            minimum=0.0,
            maximum=1.2,
            default=0.32,
        )
        self.cooldown_recovery_discount = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_COOLDOWN_RECOVERY_DISCOUNT", "0.22"),
            minimum=0.0,
            maximum=0.8,
            default=0.22,
        )
        self.cooldown_adaptive_enabled = self._env_flag("JARVIS_EXTERNAL_COOLDOWN_ADAPTIVE_ENABLED", default=True)
        self.cooldown_bias_decay = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_COOLDOWN_BIAS_DECAY", "0.78"),
            minimum=0.3,
            maximum=0.99,
            default=0.78,
        )
        self.cooldown_bias_failure_gain = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_COOLDOWN_BIAS_FAILURE_GAIN", "0.55"),
            minimum=0.05,
            maximum=2.0,
            default=0.55,
        )
        self.cooldown_bias_success_relief = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_COOLDOWN_BIAS_SUCCESS_RELIEF", "0.28"),
            minimum=0.01,
            maximum=1.2,
            default=0.28,
        )
        self.cooldown_bias_min = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_COOLDOWN_BIAS_MIN", "0.55"),
            minimum=0.2,
            maximum=1.0,
            default=0.55,
        )
        self.cooldown_bias_max = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_COOLDOWN_BIAS_MAX", "2.6"),
            minimum=1.0,
            maximum=5.0,
            default=2.6,
        )
        self.auth_precheck_enabled = self._env_flag("JARVIS_EXTERNAL_AUTH_PRECHECK_ENABLED", default=True)
        self.auth_precheck_fail_closed = self._env_flag("JARVIS_EXTERNAL_AUTH_PRECHECK_FAIL_CLOSED", default=False)
        self.auth_precheck_scope_strict = self._env_flag("JARVIS_EXTERNAL_AUTH_PRECHECK_SCOPE_STRICT", default=True)
        self.auth_precheck_expiry_strict = self._env_flag("JARVIS_EXTERNAL_AUTH_PRECHECK_EXPIRY_STRICT", default=False)
        self.auth_precheck_warn_ttl_s = self._coerce_int(
            os.getenv("JARVIS_EXTERNAL_AUTH_PRECHECK_WARN_TTL_S", "180"),
            minimum=30,
            maximum=86400,
            default=180,
        )
        self.outage_filter_enabled = self._env_flag("JARVIS_EXTERNAL_OUTAGE_FILTER_ENABLED", default=True)
        self.outage_ema_decay = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_OUTAGE_EMA_DECAY", "0.84"),
            minimum=0.45,
            maximum=0.99,
            default=0.84,
        )
        self.outage_trip_threshold = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_OUTAGE_TRIP_THRESHOLD", "0.62"),
            minimum=0.15,
            maximum=0.98,
            default=0.62,
        )
        self.outage_recover_threshold = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_OUTAGE_RECOVER_THRESHOLD", "0.36"),
            minimum=0.05,
            maximum=0.9,
            default=0.36,
        )
        self.outage_fail_streak_threshold = self._coerce_int(
            os.getenv("JARVIS_EXTERNAL_OUTAGE_FAIL_STREAK_THRESHOLD", "3"),
            minimum=1,
            maximum=30,
            default=3,
        )
        self.outage_route_penalty_weight = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_OUTAGE_ROUTE_PENALTY_WEIGHT", "0.22"),
            minimum=0.0,
            maximum=1.0,
            default=0.22,
        )
        self.outage_route_hard_block_threshold = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_OUTAGE_ROUTE_HARD_BLOCK_THRESHOLD", "0.86"),
            minimum=0.2,
            maximum=1.0,
            default=0.86,
        )
        self.outage_preflight_block_enabled = self._env_flag(
            "JARVIS_EXTERNAL_OUTAGE_PREFLIGHT_BLOCK_ENABLED",
            default=False,
        )
        self.outage_preflight_block_threshold = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_OUTAGE_PREFLIGHT_BLOCK_THRESHOLD", "0.92"),
            minimum=0.2,
            maximum=1.0,
            default=0.92,
        )
        self.outage_policy_adaptive_enabled = self._env_flag(
            "JARVIS_EXTERNAL_OUTAGE_POLICY_ADAPTIVE_ENABLED",
            default=True,
        )
        self.outage_policy_bias_decay = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_OUTAGE_POLICY_BIAS_DECAY", "0.82"),
            minimum=0.3,
            maximum=0.99,
            default=0.82,
        )
        self.outage_policy_bias_pressure_gain = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_OUTAGE_POLICY_BIAS_PRESSURE_GAIN", "0.28"),
            minimum=0.01,
            maximum=1.2,
            default=0.28,
        )
        self.outage_policy_bias_success_relief = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_OUTAGE_POLICY_BIAS_SUCCESS_RELIEF", "0.18"),
            minimum=0.01,
            maximum=1.0,
            default=0.18,
        )
        self.outage_policy_bias_min = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_OUTAGE_POLICY_BIAS_MIN", "-0.45"),
            minimum=-1.0,
            maximum=0.0,
            default=-0.45,
        )
        self.outage_policy_bias_max = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_OUTAGE_POLICY_BIAS_MAX", "0.6"),
            minimum=0.0,
            maximum=1.4,
            default=0.6,
        )
        self.mission_outage_autotune_enabled = self._env_flag(
            "JARVIS_EXTERNAL_MISSION_OUTAGE_AUTOTUNE_ENABLED",
            default=True,
        )
        self.mission_outage_bias_decay = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_MISSION_OUTAGE_BIAS_DECAY", "0.8"),
            minimum=0.35,
            maximum=0.99,
            default=0.8,
        )
        self.mission_outage_bias_gain = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_MISSION_OUTAGE_BIAS_GAIN", "0.34"),
            minimum=0.05,
            maximum=1.4,
            default=0.34,
        )
        self.mission_outage_quality_relief = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_MISSION_OUTAGE_QUALITY_RELIEF", "0.18"),
            minimum=0.01,
            maximum=1.2,
            default=0.18,
        )
        self.mission_outage_bias_min = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_MISSION_OUTAGE_BIAS_MIN", "-0.55"),
            minimum=-1.2,
            maximum=0.0,
            default=-0.55,
        )
        self.mission_outage_bias_max = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_MISSION_OUTAGE_BIAS_MAX", "0.85"),
            minimum=0.0,
            maximum=1.6,
            default=0.85,
        )
        self.mission_outage_profile_autotune_enabled = self._env_flag(
            "JARVIS_EXTERNAL_MISSION_OUTAGE_PROFILE_AUTOTUNE_ENABLED",
            default=True,
        )
        self.mission_outage_profile_hysteresis = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_MISSION_OUTAGE_PROFILE_HYSTERESIS", "0.09"),
            minimum=0.01,
            maximum=0.5,
            default=0.09,
        )
        self.mission_outage_profile_decay = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_MISSION_OUTAGE_PROFILE_DECAY", "0.78"),
            minimum=0.3,
            maximum=0.99,
            default=0.78,
        )
        self.mission_outage_profile_stability_decay = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_MISSION_OUTAGE_PROFILE_STABILITY_DECAY", "0.82"),
            minimum=0.35,
            maximum=0.99,
            default=0.82,
        )
        self.mission_outage_profile_history_limit = self._coerce_int(
            os.getenv("JARVIS_EXTERNAL_MISSION_OUTAGE_PROFILE_HISTORY_LIMIT", "60"),
            minimum=8,
            maximum=600,
            default=60,
        )
        self.mission_analysis_history_limit = self._coerce_int(
            os.getenv("JARVIS_EXTERNAL_MISSION_ANALYSIS_HISTORY_LIMIT", "240"),
            minimum=24,
            maximum=4000,
            default=240,
        )
        self.mission_analysis_record_min_interval_s = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_MISSION_ANALYSIS_MIN_INTERVAL_S", "45"),
            minimum=5.0,
            maximum=3600.0,
            default=45.0,
        )
        self.mission_analysis_record_change_threshold = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_MISSION_ANALYSIS_CHANGE_THRESHOLD", "0.05"),
            minimum=0.0,
            maximum=1.0,
            default=0.05,
        )
        self.mission_provider_policy_autotune_enabled = self._env_flag(
            "JARVIS_EXTERNAL_MISSION_PROVIDER_POLICY_AUTOTUNE_ENABLED",
            default=True,
        )
        self.mission_provider_policy_max_providers = self._coerce_int(
            os.getenv("JARVIS_EXTERNAL_MISSION_PROVIDER_POLICY_MAX_PROVIDERS", "80"),
            minimum=8,
            maximum=1000,
            default=80,
        )
        self.mission_outage_capability_bias_decay = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_MISSION_OUTAGE_CAPABILITY_BIAS_DECAY", "0.84"),
            minimum=0.35,
            maximum=0.995,
            default=0.84,
        )
        self.mission_outage_capability_bias_gain = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_MISSION_OUTAGE_CAPABILITY_BIAS_GAIN", "0.24"),
            minimum=0.02,
            maximum=1.0,
            default=0.24,
        )
        self.mission_outage_capability_limit = self._coerce_int(
            os.getenv("JARVIS_EXTERNAL_MISSION_OUTAGE_CAPABILITY_LIMIT", "10"),
            minimum=4,
            maximum=24,
            default=10,
        )
        self.profile_performance_enabled = self._env_flag(
            "JARVIS_EXTERNAL_PROFILE_PERFORMANCE_ENABLED",
            default=True,
        )
        self.profile_performance_decay = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_PROFILE_PERFORMANCE_DECAY", "0.82"),
            minimum=0.35,
            maximum=0.99,
            default=0.82,
        )
        self.profile_performance_min_samples = self._coerce_int(
            os.getenv("JARVIS_EXTERNAL_PROFILE_PERFORMANCE_MIN_SAMPLES", "3"),
            minimum=1,
            maximum=30,
            default=3,
        )
        self.profile_performance_bonus_weight = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_PROFILE_PERFORMANCE_BONUS_WEIGHT", "0.11"),
            minimum=0.01,
            maximum=0.35,
            default=0.11,
        )
        self.profile_performance_penalty_weight = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_PROFILE_PERFORMANCE_PENALTY_WEIGHT", "0.14"),
            minimum=0.01,
            maximum=0.4,
            default=0.14,
        )

        self._lock = RLock()
        self._provider_states: Dict[str, Dict[str, Any]] = {}
        self._mission_outage_policy: Dict[str, Any] = self._mission_outage_policy_default_state()
        self._mission_analysis_history: List[Dict[str, Any]] = []
        self._updates_since_save = 0
        self._last_save_monotonic = 0.0
        self._mission_policy_runtime_defaults = self._mission_policy_runtime_config_snapshot()
        self._load()

    def _mission_policy_runtime_config_snapshot(self) -> Dict[str, Any]:
        return {
            "mission_outage_autotune_enabled": bool(self.mission_outage_autotune_enabled),
            "mission_outage_profile_autotune_enabled": bool(self.mission_outage_profile_autotune_enabled),
            "mission_provider_policy_autotune_enabled": bool(self.mission_provider_policy_autotune_enabled),
            "mission_outage_bias_gain": round(float(self.mission_outage_bias_gain), 6),
            "mission_outage_bias_decay": round(float(self.mission_outage_bias_decay), 6),
            "mission_outage_quality_relief": round(float(self.mission_outage_quality_relief), 6),
            "mission_outage_profile_decay": round(float(self.mission_outage_profile_decay), 6),
            "mission_outage_profile_stability_decay": round(float(self.mission_outage_profile_stability_decay), 6),
            "mission_outage_profile_hysteresis": round(float(self.mission_outage_profile_hysteresis), 6),
            "mission_outage_capability_bias_gain": round(float(self.mission_outage_capability_bias_gain), 6),
            "mission_outage_capability_bias_decay": round(float(self.mission_outage_capability_bias_decay), 6),
            "mission_outage_capability_limit": int(self.mission_outage_capability_limit),
            "provider_policy_max_providers": int(self.mission_provider_policy_max_providers),
            "outage_trip_threshold": round(float(self.outage_trip_threshold), 6),
            "outage_recover_threshold": round(float(self.outage_recover_threshold), 6),
            "outage_route_hard_block_threshold": round(float(self.outage_route_hard_block_threshold), 6),
            "outage_preflight_block_threshold": round(float(self.outage_preflight_block_threshold), 6),
        }

    def _apply_mission_policy_runtime_config(self, raw: Any, *, reset: bool = False) -> Dict[str, Any]:
        defaults_raw = getattr(self, "_mission_policy_runtime_defaults", {})
        defaults = defaults_raw if isinstance(defaults_raw, dict) else {}
        payload = raw if isinstance(raw, dict) else {}
        changed: Dict[str, Any] = {}

        def _assign_bool(attr: str, key: str) -> None:
            source_has_value = reset or (key in payload)
            if not source_has_value:
                return
            if reset:
                value = bool(defaults.get(key, getattr(self, attr)))
            else:
                value = self._coerce_bool(payload.get(key), default=bool(getattr(self, attr)))
            if bool(getattr(self, attr)) != bool(value):
                setattr(self, attr, bool(value))
                changed[key] = bool(value)

        def _assign_float(attr: str, key: str, minimum: float, maximum: float) -> None:
            source_has_value = reset or (key in payload)
            if not source_has_value:
                return
            if reset:
                base_value = defaults.get(key, getattr(self, attr))
            else:
                base_value = payload.get(key, getattr(self, attr))
            value = self._coerce_float(base_value, minimum=minimum, maximum=maximum, default=float(getattr(self, attr)))
            if abs(float(getattr(self, attr)) - float(value)) >= 0.000001:
                setattr(self, attr, float(value))
                changed[key] = round(float(value), 6)

        def _assign_int(attr: str, key: str, minimum: int, maximum: int) -> None:
            source_has_value = reset or (key in payload)
            if not source_has_value:
                return
            if reset:
                base_value = defaults.get(key, getattr(self, attr))
            else:
                base_value = payload.get(key, getattr(self, attr))
            value = self._coerce_int(base_value, minimum=minimum, maximum=maximum, default=int(getattr(self, attr)))
            if int(getattr(self, attr)) != int(value):
                setattr(self, attr, int(value))
                changed[key] = int(value)

        _assign_bool("mission_outage_autotune_enabled", "mission_outage_autotune_enabled")
        _assign_bool("mission_outage_profile_autotune_enabled", "mission_outage_profile_autotune_enabled")
        _assign_bool("mission_provider_policy_autotune_enabled", "mission_provider_policy_autotune_enabled")
        _assign_float("mission_outage_bias_gain", "mission_outage_bias_gain", 0.12, 1.95)
        _assign_float("mission_outage_bias_decay", "mission_outage_bias_decay", 0.42, 0.98)
        _assign_float("mission_outage_quality_relief", "mission_outage_quality_relief", 0.04, 1.2)
        _assign_float("mission_outage_profile_decay", "mission_outage_profile_decay", 0.42, 0.98)
        _assign_float("mission_outage_profile_stability_decay", "mission_outage_profile_stability_decay", 0.42, 0.98)
        _assign_float("mission_outage_profile_hysteresis", "mission_outage_profile_hysteresis", 0.01, 0.5)
        _assign_float("mission_outage_capability_bias_gain", "mission_outage_capability_bias_gain", 0.05, 1.0)
        _assign_float("mission_outage_capability_bias_decay", "mission_outage_capability_bias_decay", 0.3, 0.99)
        _assign_int("mission_outage_capability_limit", "mission_outage_capability_limit", 1, 100)
        _assign_int("mission_provider_policy_max_providers", "provider_policy_max_providers", 1, 2000)
        _assign_float("outage_trip_threshold", "outage_trip_threshold", 0.15, 0.98)
        _assign_float("outage_route_hard_block_threshold", "outage_route_hard_block_threshold", 0.2, 1.0)
        _assign_float("outage_preflight_block_threshold", "outage_preflight_block_threshold", 0.2, 1.0)

        recover_default = float(defaults.get("outage_recover_threshold", getattr(self, "outage_recover_threshold", 0.36)))
        recover_candidate = recover_default if reset else payload.get("outage_recover_threshold", getattr(self, "outage_recover_threshold", recover_default))
        recover_threshold = self._coerce_float(
            recover_candidate,
            minimum=0.05,
            maximum=max(0.05, float(self.outage_trip_threshold) - 0.03),
            default=float(getattr(self, "outage_recover_threshold", recover_default)),
        )
        if abs(float(getattr(self, "outage_recover_threshold")) - float(recover_threshold)) >= 0.000001:
            self.outage_recover_threshold = float(recover_threshold)
            changed["outage_recover_threshold"] = round(float(recover_threshold), 6)

        if self.outage_preflight_block_threshold < self.outage_route_hard_block_threshold:
            self.outage_preflight_block_threshold = self._coerce_float(
                self.outage_route_hard_block_threshold + 0.01,
                minimum=0.2,
                maximum=1.0,
                default=self.outage_route_hard_block_threshold,
            )
            changed["outage_preflight_block_threshold"] = round(float(self.outage_preflight_block_threshold), 6)
        if self.outage_recover_threshold > max(0.05, self.outage_trip_threshold - 0.03):
            self.outage_recover_threshold = self._coerce_float(
                self.outage_trip_threshold - 0.03,
                minimum=0.05,
                maximum=max(0.05, self.outage_trip_threshold - 0.03),
                default=self.outage_recover_threshold,
            )
            changed["outage_recover_threshold"] = round(float(self.outage_recover_threshold), 6)
        return changed

    def is_managed_action(self, action: str) -> bool:
        clean_action = str(action or "").strip().lower()
        if not clean_action:
            return False
        return clean_action.startswith(self._MANAGED_PREFIXES)

    def preflight(
        self,
        *,
        action: str,
        args: Dict[str, Any] | None = None,
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        clean_action = str(action or "").strip().lower()
        payload = args if isinstance(args, dict) else {}
        runtime_meta = dict(metadata) if isinstance(metadata, dict) else {}
        runtime_meta.setdefault("__external_action", clean_action)
        runtime_meta.setdefault("__external_operation_class", self._operation_class(clean_action))
        runtime_meta.setdefault(
            "__external_capability",
            self._normalize_mission_capability(self._action_domain(clean_action)),
        )

        if not self.is_managed_action(clean_action):
            return {"status": "skip"}

        contract = self._check_contract(clean_action, payload)
        if contract:
            contract_diagnostic = self._build_contract_diagnostic(
                action=clean_action,
                payload=payload,
                message=contract,
            )
            return self._finalize_preflight_payload(
                action=clean_action,
                metadata=runtime_meta,
                providers=[],
                provider_health=[],
                explicit_provider="",
                payload={
                "status": "error",
                "message": contract,
                "failure_category": "non_retryable",
                "action": clean_action,
                "contract_diagnostic": contract_diagnostic,
                "remediation_hints": contract_diagnostic.get("remediation_hints", []),
                "remediation_contract": contract_diagnostic.get("remediation_contract", {}),
                },
            )

        explicit_provider = self._normalize_provider(str(payload.get("provider", "")).strip())
        providers = self._provider_candidates(clean_action, payload)
        contract_negotiation = self._negotiate_provider_contract(
            action=clean_action,
            payload=payload,
            providers=providers,
            explicit_provider=explicit_provider,
        )
        if str(contract_negotiation.get("status", "")).strip().lower() == "error":
            provider_diag = self._build_provider_contract_diagnostic(
                action=clean_action,
                negotiation=contract_negotiation,
            )
            return self._finalize_preflight_payload(
                action=clean_action,
                metadata=runtime_meta,
                providers=providers if isinstance(providers, list) else [],
                provider_health=[],
                explicit_provider=explicit_provider,
                payload={
                "status": "error",
                "message": str(contract_negotiation.get("message", "")).strip()
                or f"Provider contract failed for action '{clean_action}'.",
                "failure_category": "non_retryable",
                "action": clean_action,
                "contract_negotiation": contract_negotiation,
                "contract_diagnostic": provider_diag,
                "remediation_hints": provider_diag.get("remediation_hints", []),
                "remediation_contract": provider_diag.get("remediation_contract", {}),
                },
            )
        providers = contract_negotiation.get("providers", [])
        if not isinstance(providers, list):
            providers = []
        providers = [self._normalize_provider(str(item)) for item in providers if self._normalize_provider(str(item))]
        capability_negotiation = self._negotiate_provider_capability_contract(
            action=clean_action,
            payload=payload,
            providers=providers,
            explicit_provider=explicit_provider,
            metadata=runtime_meta,
        )
        if str(capability_negotiation.get("status", "")).strip().lower() == "error":
            capability_diag = self._build_provider_contract_diagnostic(
                action=clean_action,
                negotiation=capability_negotiation,
            )
            return self._finalize_preflight_payload(
                action=clean_action,
                metadata=runtime_meta,
                providers=providers if isinstance(providers, list) else [],
                provider_health=[],
                explicit_provider=explicit_provider,
                payload={
                    "status": "error",
                    "message": str(capability_negotiation.get("message", "")).strip()
                    or f"Provider capability contract failed for action '{clean_action}'.",
                    "failure_category": "non_retryable",
                    "action": clean_action,
                    "contract_negotiation": contract_negotiation,
                    "capability_negotiation": capability_negotiation,
                    "contract_diagnostic": capability_diag,
                    "remediation_hints": capability_diag.get("remediation_hints", []),
                    "remediation_contract": capability_diag.get("remediation_contract", {}),
                },
            )
        providers = capability_negotiation.get("providers", providers)
        if not isinstance(providers, list):
            providers = []
        providers = [self._normalize_provider(str(item)) for item in providers if self._normalize_provider(str(item))]
        if isinstance(contract_negotiation, dict):
            dropped_base = contract_negotiation.get("dropped_providers", [])
            dropped_rows = dropped_base if isinstance(dropped_base, list) else []
            capability_dropped = capability_negotiation.get("dropped_providers", [])
            if isinstance(capability_dropped, list) and capability_dropped:
                contract_negotiation["dropped_providers"] = self._merge_dropped_provider_rows(
                    dropped_rows,
                    [dict(row) for row in capability_dropped if isinstance(row, dict)],
                )
            contract_negotiation["capability_negotiation"] = capability_negotiation
        auth_preflight = self._auth_preflight_contract(
            action=clean_action,
            payload=payload,
            providers=providers,
            explicit_provider=explicit_provider,
            metadata=runtime_meta,
        )
        if str(auth_preflight.get("status", "")).strip().lower() == "error":
            auth_diag = self._build_auth_contract_diagnostic(
                action=clean_action,
                auth_preflight=auth_preflight,
            )
            return self._finalize_preflight_payload(
                action=clean_action,
                metadata=runtime_meta,
                providers=providers if isinstance(providers, list) else [],
                provider_health=[],
                explicit_provider=explicit_provider,
                payload={
                "status": "blocked",
                "message": str(auth_preflight.get("message", "")).strip()
                or f"External auth contract failed for action '{clean_action}'.",
                "failure_category": "auth",
                "action": clean_action,
                "contract_negotiation": contract_negotiation,
                "auth_preflight": auth_preflight,
                "contract_diagnostic": auth_diag,
                "remediation_hints": auth_diag.get("remediation_hints", []),
                "remediation_contract": auth_diag.get("remediation_contract", {}),
                },
            )
        providers = auth_preflight.get("providers", providers)
        if not isinstance(providers, list):
            providers = []
        providers = [self._normalize_provider(str(item)) for item in providers if self._normalize_provider(str(item))]
        if isinstance(contract_negotiation, dict):
            existing_dropped = contract_negotiation.get("dropped_providers", [])
            existing_rows = existing_dropped if isinstance(existing_dropped, list) else []
            auth_dropped = auth_preflight.get("dropped_providers", [])
            if isinstance(auth_dropped, list) and auth_dropped:
                contract_negotiation["dropped_providers"] = self._merge_dropped_provider_rows(
                    existing_rows,
                    [dict(row) for row in auth_dropped if isinstance(row, dict)],
                )
            contract_negotiation["auth_preflight"] = auth_preflight
        if not providers:
            provider_diag = self._build_provider_contract_diagnostic(
                action=clean_action,
                negotiation=contract_negotiation,
            )
            return self._finalize_preflight_payload(
                action=clean_action,
                metadata=runtime_meta,
                providers=[],
                provider_health=[],
                explicit_provider=explicit_provider,
                payload={
                "status": "error",
                "message": f"No compatible providers available for action '{clean_action}'.",
                "failure_category": "non_retryable",
                "action": clean_action,
                "contract_negotiation": contract_negotiation,
                "auth_preflight": auth_preflight,
                "contract_diagnostic": provider_diag,
                "remediation_hints": provider_diag.get("remediation_hints", []),
                "remediation_contract": provider_diag.get("remediation_contract", {}),
                },
            )

        blocked_candidates: List[Dict[str, Any]] = []
        now_ts = time.time()
        provider_health: List[Dict[str, Any]] = []
        with self._lock:
            for provider in providers:
                state = self._provider_states.get(provider, {})
                health_row = self._provider_health_row(
                    provider=provider,
                    action=clean_action,
                    state=state,
                    now_ts=now_ts,
                    metadata=runtime_meta,
                )
                if health_row:
                    provider_health.append(health_row)
                cooldown_until = self._to_timestamp(state.get("cooldown_until", ""))
                if cooldown_until <= now_ts:
                    continue
                retry_after_s = max(0.0, cooldown_until - now_ts)
                blocked_candidates.append(
                    {
                        "provider": provider,
                        "retry_after_s": round(retry_after_s, 3),
                        "failure_ema": self._coerce_float(state.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        "last_category": str(state.get("last_category", "")).strip().lower(),
                    }
                )
        outage_blocked: List[Dict[str, Any]] = []
        for row in provider_health:
            if not isinstance(row, dict):
                continue
            outage_active = bool(row.get("outage_active", False))
            outage_pressure = self._coerce_float(
                row.get("outage_pressure", row.get("outage_ema", 0.0)),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            if not outage_active:
                continue
            block_threshold = self._coerce_float(
                row.get("preflight_block_threshold", self.outage_preflight_block_threshold),
                minimum=0.2,
                maximum=1.0,
                default=self.outage_preflight_block_threshold,
            )
            if outage_pressure < block_threshold:
                continue
            outage_blocked.append(
                {
                    "provider": str(row.get("provider", "")).strip().lower(),
                    "outage_pressure": round(outage_pressure, 6),
                    "preflight_block_threshold": round(block_threshold, 6),
                }
            )
        outage_override = self._coerce_bool(runtime_meta.get("external_outage_override", False), default=False)
        if (
            self.outage_filter_enabled
            and self.outage_preflight_block_enabled
            and not outage_override
            and providers
            and len(outage_blocked) >= len(providers)
        ):
            block_message = (
                f"All external providers are in outage regime for action '{clean_action}'. "
                "Retry later or override with metadata.external_outage_override=true."
            )
            runtime_diag = self._build_runtime_block_contract_diagnostic(
                action=clean_action,
                reason="outage",
                providers=providers,
                blocked_candidates=blocked_candidates,
                outage_blocked=outage_blocked,
                auth_preflight=auth_preflight,
                message=block_message,
            )
            return self._finalize_preflight_payload(
                action=clean_action,
                metadata=runtime_meta,
                providers=providers,
                provider_health=provider_health,
                explicit_provider=explicit_provider,
                payload={
                "status": "blocked",
                "message": block_message,
                "action": clean_action,
                "providers": outage_blocked,
                "failure_category": "transient",
                "contract_negotiation": contract_negotiation,
                "auth_preflight": auth_preflight,
                "contract_diagnostic": runtime_diag,
                "remediation_hints": runtime_diag.get("remediation_hints", []),
                "remediation_plan": runtime_diag.get("remediation_plan", []),
                "remediation_contract": runtime_diag.get("remediation_contract", {}),
                },
            )

        if self._coerce_bool(runtime_meta.get("external_cooldown_override", False), default=False):
            route = self._build_provider_route(
                action=clean_action,
                payload=payload,
                providers=providers,
                blocked_candidates=blocked_candidates,
                provider_health=provider_health,
                explicit_provider=explicit_provider,
                override=True,
                contract_negotiation=contract_negotiation,
            )
            out = {
                "status": "ok",
                "action": clean_action,
                "provider_candidates": providers,
                "provider_routing": route,
                "cooldown_override": True,
                "contract_negotiation": contract_negotiation,
                "auth_preflight": auth_preflight,
            }
            if isinstance(route.get("args_patch"), dict) and route.get("args_patch"):
                out["args_patch"] = dict(route.get("args_patch", {}))
            return out

        if self.cooldown_enabled and providers and len(blocked_candidates) >= len(providers):
            earliest = min(float(item.get("retry_after_s", 0.0) or 0.0) for item in blocked_candidates)
            block_message = (
                f"External provider cooldown active for action '{clean_action}'. "
                f"Retry in {max(0.0, earliest):.1f}s or override with metadata.external_cooldown_override=true."
            )
            runtime_diag = self._build_runtime_block_contract_diagnostic(
                action=clean_action,
                reason="cooldown",
                providers=providers,
                blocked_candidates=blocked_candidates,
                outage_blocked=outage_blocked,
                auth_preflight=auth_preflight,
                message=block_message,
            )
            return self._finalize_preflight_payload(
                action=clean_action,
                metadata=runtime_meta,
                providers=providers,
                provider_health=provider_health,
                explicit_provider=explicit_provider,
                payload={
                "status": "blocked",
                "message": block_message,
                "action": clean_action,
                "retry_after_s": round(max(0.0, earliest), 3),
                "providers": blocked_candidates,
                "failure_category": "transient",
                "contract_negotiation": contract_negotiation,
                "auth_preflight": auth_preflight,
                "contract_diagnostic": runtime_diag,
                "remediation_hints": runtime_diag.get("remediation_hints", []),
                "remediation_plan": runtime_diag.get("remediation_plan", []),
                "remediation_contract": runtime_diag.get("remediation_contract", {}),
                },
            )

        hint = self.retry_hint(action=clean_action, args=payload, metadata=runtime_meta)
        route = self._build_provider_route(
            action=clean_action,
            payload=payload,
            providers=providers,
            blocked_candidates=blocked_candidates,
            provider_health=provider_health,
            explicit_provider=explicit_provider,
            override=False,
            contract_negotiation=contract_negotiation,
        )
        args_patch = route.get("args_patch") if isinstance(route, dict) else {}
        return {
            "status": "ok",
            "action": clean_action,
            "provider_candidates": providers,
            "provider_routing": route,
            "args_patch": dict(args_patch) if isinstance(args_patch, dict) else {},
            "retry_hint": hint.get("retry_hint", {}) if isinstance(hint, dict) else {},
            "retry_contract": hint.get("retry_contract", {}) if isinstance(hint, dict) else {},
            "contract_negotiation": contract_negotiation,
            "auth_preflight": auth_preflight,
        }

    def _finalize_preflight_payload(
        self,
        *,
        action: str,
        payload: Dict[str, Any],
        metadata: Dict[str, Any],
        providers: List[str] | None = None,
        provider_health: List[Dict[str, Any]] | None = None,
        explicit_provider: str = "",
    ) -> Dict[str, Any]:
        base = dict(payload) if isinstance(payload, dict) else {}
        tuned = self._autotune_preflight_remediation_contract(
            action=action,
            payload=base,
            metadata=metadata if isinstance(metadata, dict) else {},
            providers=[str(item).strip().lower() for item in (providers or []) if str(item).strip()],
            provider_health=[dict(row) for row in (provider_health or []) if isinstance(row, dict)],
            explicit_provider=explicit_provider,
        )
        return tuned if isinstance(tuned, dict) else base

    def _autotune_preflight_remediation_contract(
        self,
        *,
        action: str,
        payload: Dict[str, Any],
        metadata: Dict[str, Any],
        providers: List[str],
        provider_health: List[Dict[str, Any]],
        explicit_provider: str,
    ) -> Dict[str, Any]:
        out = dict(payload) if isinstance(payload, dict) else {}
        diagnostic_raw = out.get("contract_diagnostic", {})
        diagnostic = diagnostic_raw if isinstance(diagnostic_raw, dict) else {}
        contract_raw = out.get("remediation_contract")
        contract = contract_raw if isinstance(contract_raw, dict) else {}
        if not contract and isinstance(diagnostic.get("remediation_contract"), dict):
            contract = dict(diagnostic.get("remediation_contract", {}))
        if not contract:
            return out

        execution_raw = contract.get("execution_contract", {})
        execution = execution_raw if isinstance(execution_raw, dict) else {}
        verification_raw = execution.get("verification", {})
        verification = verification_raw if isinstance(verification_raw, dict) else {}
        stop_conditions_raw = execution.get("stop_conditions", [])
        stop_conditions = (
            [str(item).strip().lower() for item in stop_conditions_raw if str(item).strip()]
            if isinstance(stop_conditions_raw, list)
            else []
        )
        stop_condition_set = set(stop_conditions)

        clean_action = str(action or "").strip().lower()
        operation_class = self._operation_class(clean_action)
        clean_explicit_provider = self._normalize_provider(str(explicit_provider or "").strip())

        health_rows = [dict(row) for row in provider_health if isinstance(row, dict)]
        if not health_rows and providers:
            runtime_meta = metadata if isinstance(metadata, dict) else {}
            now_ts = time.time()
            with self._lock:
                for provider in providers[:8]:
                    clean_provider = self._normalize_provider(str(provider))
                    if not clean_provider:
                        continue
                    state = self._provider_states.get(clean_provider, {})
                    health = self._provider_health_row(
                        provider=clean_provider,
                        action=clean_action,
                        state=state if isinstance(state, dict) else {},
                        now_ts=now_ts,
                        metadata=runtime_meta,
                    )
                    if isinstance(health, dict) and health:
                        health_rows.append(health)

        provider_pressure = 0.0
        cooldown_ratio = 0.0
        outage_ratio = 0.0
        if health_rows:
            total = float(len(health_rows))
            cooldown_hits = 0
            outage_hits = 0
            for row in health_rows:
                failure_ema = self._coerce_float(
                    row.get("failure_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                health_score = self._coerce_float(
                    row.get("health_score", 0.5),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.5,
                )
                outage_pressure = self._coerce_float(
                    row.get("outage_pressure", row.get("outage_ema", 0.0)),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                if bool(row.get("cooldown_active", False)):
                    cooldown_hits += 1
                if bool(row.get("outage_active", False)):
                    outage_hits += 1
                provider_pressure = max(
                    provider_pressure,
                    failure_ema,
                    max(0.0, 1.0 - health_score),
                    outage_pressure,
                )
            cooldown_ratio = max(0.0, min(1.0, float(cooldown_hits) / total))
            outage_ratio = max(0.0, min(1.0, float(outage_hits) / total))
        auth_preflight_payload = out.get("auth_preflight", {})
        auth_preflight_contract = auth_preflight_payload if isinstance(auth_preflight_payload, dict) else {}
        blocked_candidates_rows: List[Dict[str, Any]] = []
        outage_blocked_rows: List[Dict[str, Any]] = []
        providers_payload = out.get("providers", [])
        provider_payload_rows = [dict(row) for row in providers_payload if isinstance(row, dict)] if isinstance(providers_payload, list) else []
        for row in provider_payload_rows:
            provider = self._normalize_provider(str(row.get("provider", "")))
            if not provider:
                continue
            if "retry_after_s" in row:
                blocked_candidates_rows.append(
                    {
                        "provider": provider,
                        "retry_after_s": self._coerce_float(
                            row.get("retry_after_s", 0.0),
                            minimum=0.0,
                            maximum=float(self.max_cooldown_s),
                            default=0.0,
                        ),
                        "failure_ema": self._coerce_float(row.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        "last_category": str(row.get("last_category", "")).strip().lower(),
                    }
                )
            if "outage_pressure" in row:
                outage_blocked_rows.append(
                    {
                        "provider": provider,
                        "outage_pressure": self._coerce_float(
                            row.get("outage_pressure", 0.0),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        "preflight_block_threshold": self._coerce_float(
                            row.get("preflight_block_threshold", self.outage_preflight_block_threshold),
                            minimum=0.2,
                            maximum=1.0,
                            default=self.outage_preflight_block_threshold,
                        ),
                    }
                )
        diagnostic_cooldown_rows = diagnostic.get("cooldown_providers", [])
        if isinstance(diagnostic_cooldown_rows, list):
            for row in diagnostic_cooldown_rows:
                if not isinstance(row, dict):
                    continue
                provider = self._normalize_provider(str(row.get("provider", "")))
                if not provider:
                    continue
                blocked_candidates_rows.append(
                    {
                        "provider": provider,
                        "retry_after_s": self._coerce_float(
                            row.get("retry_after_s", 0.0),
                            minimum=0.0,
                            maximum=float(self.max_cooldown_s),
                            default=0.0,
                        ),
                        "failure_ema": self._coerce_float(row.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        "last_category": str(row.get("last_category", "")).strip().lower(),
                    }
                )
        diagnostic_outage_rows = diagnostic.get("outage_providers", [])
        if isinstance(diagnostic_outage_rows, list):
            for row in diagnostic_outage_rows:
                if not isinstance(row, dict):
                    continue
                provider = self._normalize_provider(str(row.get("provider", "")))
                if not provider:
                    continue
                outage_blocked_rows.append(
                    {
                        "provider": provider,
                        "outage_pressure": self._coerce_float(
                            row.get("outage_pressure", 0.0),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        "preflight_block_threshold": self._coerce_float(
                            row.get("preflight_block_threshold", self.outage_preflight_block_threshold),
                            minimum=0.2,
                            maximum=1.0,
                            default=self.outage_preflight_block_threshold,
                        ),
                    }
                )
        orchestration_rows = self._runtime_provider_orchestration_rows(
            providers=providers,
            blocked_candidates=blocked_candidates_rows,
            outage_blocked=outage_blocked_rows,
            auth_preflight=auth_preflight_contract,
        )
        orchestration_primary_provider = (
            self._normalize_provider(str(orchestration_rows[0].get("provider", "")))
            if orchestration_rows
            else ""
        )
        orchestration_fallback_provider = ""
        for row in orchestration_rows[1:]:
            provider = self._normalize_provider(str(row.get("provider", "")))
            if provider and provider != orchestration_primary_provider:
                orchestration_fallback_provider = provider
                break
        orchestration_pressure = self._coerce_float(
            1.0
            - (
                self._coerce_float(
                    orchestration_rows[0].get("score", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                if orchestration_rows
                else max(provider_pressure, cooldown_ratio * 0.72, outage_ratio * 0.84)
            ),
            minimum=0.0,
            maximum=1.0,
            default=max(provider_pressure, cooldown_ratio * 0.72, outage_ratio * 0.84),
        )
        orchestration_mode = "stable"
        if orchestration_pressure >= 0.72:
            orchestration_mode = "severe"
        elif orchestration_pressure >= 0.46:
            orchestration_mode = "elevated"
        orchestration_retry_schedule: List[Dict[str, Any]] = []
        for row in orchestration_rows[:4]:
            if not isinstance(row, dict):
                continue
            provider = self._normalize_provider(str(row.get("provider", "")))
            if not provider:
                continue
            retry_after_s = self._coerce_float(
                row.get("retry_after_s", 0.0),
                minimum=0.0,
                maximum=float(self.max_cooldown_s),
                default=0.0,
            )
            outage_pressure_row = self._coerce_float(
                row.get("outage_pressure", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            orchestration_retry_schedule.append(
                {
                    "provider": provider,
                    "delay_s": round(retry_after_s + (outage_pressure_row * 90.0), 3),
                    "score": round(
                        self._coerce_float(row.get("score", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "auth_status": str(row.get("auth_status", "")).strip().lower(),
                }
            )
        orchestration_contract = {
            "mode": orchestration_mode,
            "pressure": round(orchestration_pressure, 6),
            "primary_provider": orchestration_primary_provider,
            "fallback_provider": orchestration_fallback_provider,
            "provider_rows": orchestration_rows[:8],
            "retry_schedule": orchestration_retry_schedule[:4],
            "blocked_count": len(blocked_candidates_rows),
            "outage_count": len(outage_blocked_rows),
        }
        orchestration_remediation_hints: List[Dict[str, Any]] = []
        if orchestration_fallback_provider and orchestration_pressure >= 0.48:
            orchestration_remediation_hints.append(
                {
                    "id": "orchestration_switch_fallback",
                    "priority": 2,
                    "confidence": 0.78 if orchestration_pressure >= 0.72 else 0.7,
                    "summary": (
                        f"Switch retry lane to fallback provider '{orchestration_fallback_provider}' "
                        "until primary provider pressure recovers."
                    ),
                    "args_patch": {"provider": orchestration_fallback_provider},
                    "provider": orchestration_primary_provider or "auto",
                    "fallback_provider": orchestration_fallback_provider,
                }
            )
        if orchestration_retry_schedule:
            orchestration_remediation_hints.append(
                {
                    "id": "orchestration_staggered_retry",
                    "priority": 3,
                    "confidence": 0.74 if orchestration_mode != "stable" else 0.66,
                    "summary": "Use staged retry schedule derived from provider cooldown/outage/auth pressure.",
                    "retry_schedule": orchestration_retry_schedule[:4],
                    "remediation": {
                        "type": "staggered_provider_retry",
                        "schedule": orchestration_retry_schedule[:4],
                    },
                }
            )
        if orchestration_pressure >= 0.72:
            stop_condition_set.add("orchestration_pressure_high")
        elif orchestration_pressure >= 0.46:
            stop_condition_set.add("orchestration_pressure_elevated")

        mission_policy = self._mission_outage_policy if isinstance(self._mission_outage_policy, dict) else {}
        mission_mode = str(mission_policy.get("mode", "")).strip().lower()
        mission_profile = str(mission_policy.get("profile", "")).strip().lower()
        mission_bias = self._coerce_float(
            mission_policy.get("bias", 0.0),
            minimum=-1.0,
            maximum=1.0,
            default=0.0,
        )
        mission_pressure = self._coerce_float(
            mission_policy.get("pressure_ema", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )

        severity_score = self._coerce_float(
            diagnostic.get("severity_score", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        blocking_class = str(
            diagnostic.get("blocking_class", contract.get("blocking_class", ""))
        ).strip().lower()
        estimated_recovery_s = self._coerce_int(
            diagnostic.get("estimated_recovery_s", contract.get("estimated_recovery_s", 0)),
            minimum=0,
            maximum=86_400,
            default=0,
        )

        mode = str(execution.get("mode", contract.get("automation_tier", "automated"))).strip().lower() or "automated"
        if mode not in {"manual", "assisted", "automated"}:
            mode = "automated"
        max_retry_attempts = self._coerce_int(
            execution.get("max_retry_attempts", 2 if mode == "automated" else 1),
            minimum=1,
            maximum=8,
            default=2 if mode == "automated" else 1,
        )
        allow_provider_reroute = self._coerce_bool(
            verification.get("allow_provider_reroute", True),
            default=True,
        )

        contract_risk = max(
            severity_score,
            provider_pressure,
            self._coerce_float(
                (mission_pressure * 0.76) + (max(0.0, mission_bias) * 0.24),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            ),
        )
        if blocking_class == "auth":
            contract_risk = max(contract_risk, 0.66)
        elif blocking_class == "provider":
            contract_risk = max(contract_risk, 0.54)
        if estimated_recovery_s >= 1800:
            contract_risk = max(contract_risk, 0.76)
        elif estimated_recovery_s >= 900:
            contract_risk = max(contract_risk, 0.64)
        if mission_mode in {"severe", "degraded"}:
            contract_risk = max(contract_risk, 0.62)

        if (
            contract_risk >= 0.82
            or mission_mode == "severe"
            or (blocking_class == "auth" and estimated_recovery_s >= 900)
        ):
            mode = "manual" if blocking_class == "auth" else "assisted"
            max_retry_attempts = 1
        elif contract_risk >= 0.58 or mission_mode in {"worsening", "degraded"}:
            if mode != "manual":
                mode = "assisted"
            max_retry_attempts = min(max_retry_attempts, 2)
        elif contract_risk <= 0.24 and mission_mode in {"stable", "improving"} and mode == "assisted":
            mode = "automated"
            max_retry_attempts = max(max_retry_attempts, 2)

        provider_reroute_locked = bool(clean_explicit_provider and clean_explicit_provider != "auto")
        if provider_reroute_locked or blocking_class == "auth":
            allow_provider_reroute = False
            stop_condition_set.add("provider_reroute_locked")

        checkpoint_mode = "off"
        if mode == "manual" or contract_risk >= 0.72 or mission_mode in {"severe", "degraded"}:
            checkpoint_mode = "strict"
            stop_condition_set.add("checkpoint_failure")
        elif mode == "assisted" or contract_risk >= 0.42:
            checkpoint_mode = "standard"
        if mode == "manual":
            stop_condition_set.add("manual_escalation")
        if contract_risk >= 0.78:
            stop_condition_set.add("risk_budget_exceeded")

        autotune = {
            "risk_score": round(self._coerce_float(contract_risk, minimum=0.0, maximum=1.0, default=0.0), 6),
            "mission_mode": mission_mode or "stable",
            "mission_profile": mission_profile or "balanced",
            "mission_bias": round(mission_bias, 6),
            "provider_pressure": round(provider_pressure, 6),
            "cooldown_ratio": round(cooldown_ratio, 6),
            "outage_ratio": round(outage_ratio, 6),
            "operation_class": operation_class,
            "explicit_provider": clean_explicit_provider,
            "provider_count": len(health_rows) if health_rows else len(providers),
            "orchestration_mode": orchestration_mode,
            "orchestration_pressure": round(orchestration_pressure, 6),
            "orchestration_primary_provider": orchestration_primary_provider,
            "orchestration_fallback_provider": orchestration_fallback_provider,
        }

        verification["allow_provider_reroute"] = bool(allow_provider_reroute)
        verification["checkpoint_mode"] = checkpoint_mode
        execution["mode"] = mode
        execution["max_retry_attempts"] = int(max_retry_attempts)
        execution["verification"] = verification
        execution["stop_conditions"] = sorted(stop_condition_set)[:12]
        contract["automation_tier"] = mode
        contract["estimated_recovery_s"] = int(max(estimated_recovery_s, execution.get("estimated_recovery_s", 0) or 0))
        contract["execution_contract"] = execution
        contract["autotune"] = autotune
        contract["orchestration_contract"] = orchestration_contract
        existing_hints_raw = out.get("remediation_hints", [])
        existing_hints = [dict(row) for row in existing_hints_raw if isinstance(row, dict)] if isinstance(existing_hints_raw, list) else []
        if orchestration_remediation_hints:
            existing_hint_ids = {
                str(row.get("id", "")).strip().lower()
                for row in existing_hints
                if isinstance(row, dict)
            }
            for row in orchestration_remediation_hints:
                hint_id = str(row.get("id", "")).strip().lower()
                if hint_id and hint_id in existing_hint_ids:
                    continue
                existing_hints.append(dict(row))
        if existing_hints:
            out["remediation_hints"] = existing_hints[:24]
        out["preflight_orchestration"] = orchestration_contract

        if isinstance(diagnostic, dict):
            diagnostic["automation_tier"] = mode
            diagnostic["blocking_class"] = blocking_class or str(contract.get("blocking_class", "")).strip().lower()
            diagnostic["estimated_recovery_s"] = int(contract.get("estimated_recovery_s", 0) or 0)
            diagnostic["remediation_contract"] = contract
            diagnostic["remediation_contract_autotune"] = autotune
            diagnostic["orchestration_contract"] = orchestration_contract
            out["contract_diagnostic"] = diagnostic
        out["remediation_contract"] = contract
        return out

    def retry_hint(
        self,
        *,
        action: str,
        args: Dict[str, Any] | None = None,
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        clean_action = str(action or "").strip().lower()
        if not self.is_managed_action(clean_action) or not self.autotune_enabled:
            return {"status": "skip"}

        payload = args if isinstance(args, dict) else {}
        providers = self._provider_candidates(clean_action, payload)
        if not providers:
            return {"status": "skip"}

        route = self.route_provider(action=clean_action, args=payload, metadata=metadata)
        best_provider = str(route.get("selected_provider", "")).strip().lower() if isinstance(route, dict) else ""
        best_state: Dict[str, Any] = {}
        with self._lock:
            ordered_providers = [best_provider] if best_provider else []
            for provider in providers:
                if provider not in ordered_providers:
                    ordered_providers.append(provider)
            for provider in ordered_providers:
                state = self._provider_states.get(provider)
                if not isinstance(state, dict):
                    continue
                if not best_state and provider == best_provider:
                    best_state = dict(state)
                    best_provider = provider
                    continue
                state_action = self._action_state_for(state=state, action=clean_action)
                state_ema = self._coerce_float(state_action.get("failure_ema", state.get("failure_ema", 0.0)), minimum=0.0, maximum=1.0, default=0.0)
                best_action = self._action_state_for(state=best_state, action=clean_action) if best_state else {}
                best_ema = self._coerce_float(best_action.get("failure_ema", best_state.get("failure_ema", 0.0) if best_state else 0.0), minimum=0.0, maximum=1.0, default=0.0)
                state_failures = self._coerce_int(state_action.get("consecutive_failures", state.get("consecutive_failures", 0)), minimum=0, maximum=100000, default=0)
                best_failures = self._coerce_int(best_action.get("consecutive_failures", best_state.get("consecutive_failures", 0) if best_state else 0), minimum=0, maximum=100000, default=0)
                if not best_state or (state_ema, state_failures) > (best_ema, best_failures):
                    best_state = dict(state)
                    best_provider = provider

        if not best_state:
            return {"status": "skip"}

        best_action = self._action_state_for(state=best_state, action=clean_action)
        action_samples = self._coerce_int(best_action.get("samples", 0), minimum=0, maximum=10_000_000, default=0)
        provider_samples = self._coerce_int(best_state.get("samples", 0), minimum=0, maximum=10_000_000, default=0)
        samples = max(action_samples, provider_samples // 2)
        if samples < self.min_samples_for_hint:
            return {"status": "skip"}

        provider_failure_ema = self._coerce_float(best_state.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        provider_failure_trend_ema = self._coerce_float(
            best_state.get("failure_trend_ema", 0.0),
            minimum=-1.0,
            maximum=1.0,
            default=0.0,
        )
        provider_consecutive = self._coerce_int(best_state.get("consecutive_failures", 0), minimum=0, maximum=100000, default=0)
        provider_outage_ema = self._coerce_float(
            best_state.get("outage_ema", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        provider_outage_streak = self._coerce_int(
            best_state.get("outage_streak", 0),
            minimum=0,
            maximum=100000,
            default=0,
        )
        provider_outage_active = self._coerce_bool(best_state.get("outage_active", False), default=False)
        provider_cooldown_bias = self._coerce_float(
            best_state.get("cooldown_bias", 1.0),
            minimum=self.cooldown_bias_min,
            maximum=self.cooldown_bias_max,
            default=1.0,
        )
        failure_ema = self._coerce_float(best_action.get("failure_ema", provider_failure_ema), minimum=0.0, maximum=1.0, default=0.0)
        failure_trend_ema = self._coerce_float(
            best_action.get("failure_trend_ema", provider_failure_trend_ema),
            minimum=-1.0,
            maximum=1.0,
            default=provider_failure_trend_ema,
        )
        consecutive_failures = self._coerce_int(best_action.get("consecutive_failures", provider_consecutive), minimum=0, maximum=100000, default=0)
        severity = max(
            (failure_ema * 0.72) + (provider_failure_ema * 0.28),
            min(1.0, (float(consecutive_failures) * 0.68 + float(provider_consecutive) * 0.32) / 6.0),
        )
        if failure_trend_ema > 0.0:
            severity = min(1.0, severity + (failure_trend_ema * 0.2))
        elif failure_trend_ema < 0.0:
            severity = max(0.0, severity - (abs(failure_trend_ema) * 0.08))
        outage_pressure = min(1.0, max(provider_outage_ema, min(1.0, float(provider_outage_streak) / 7.0)))
        if outage_pressure > 0.0:
            severity = min(1.0, severity + (outage_pressure * (0.12 if provider_outage_active else 0.06)))

        operation_class = self._operation_class(clean_action)
        operation_bias_rows = best_state.get("operation_cooldown_bias", {})
        operation_bias_map = operation_bias_rows if isinstance(operation_bias_rows, dict) else {}
        operation_cooldown_bias = self._coerce_float(
            operation_bias_map.get(operation_class, 1.0),
            minimum=self.cooldown_bias_min,
            maximum=self.cooldown_bias_max,
            default=1.0,
        )
        cooldown_bias_pressure = max(
            0.0,
            min(1.0, ((provider_cooldown_bias * operation_cooldown_bias) - 1.0) / 1.6),
        )
        if cooldown_bias_pressure > 0.0:
            severity = min(1.0, severity + (cooldown_bias_pressure * 0.18))
        if severity < 0.18:
            return {"status": "skip"}

        last_category = str(best_action.get("last_category", best_state.get("last_category", "unknown"))).strip().lower() or "unknown"
        category_factor = {
            "auth": 1.45,
            "rate_limited": 1.35,
            "timeout": 1.15,
            "transient": 1.0,
            "unknown": 1.0,
            "non_retryable": 0.65,
        }.get(last_category, 1.0)
        operation_factor = self._coerce_float(
            self._OPERATION_RETRY_FACTOR.get(operation_class, self._OPERATION_RETRY_FACTOR["default"]),
            minimum=0.45,
            maximum=2.0,
            default=1.0,
        )
        selected_health = self._coerce_float(
            route.get("selected_health_score", 0.0) if isinstance(route, dict) else 0.0,
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        selected_effective = self._coerce_float(
            route.get("selected_effective_score", selected_health) if isinstance(route, dict) else selected_health,
            minimum=0.0,
            maximum=1.0,
            default=selected_health,
        )
        health_pressure = max(0.0, min(1.0, 0.72 - selected_health))
        effective_pressure = max(0.0, min(1.0, 0.66 - selected_effective))
        base_delay = (0.5 + (severity * 1.65)) * category_factor * operation_factor * (1.0 + (health_pressure * 0.65))
        base_delay *= (1.0 + (effective_pressure * 0.4))
        base_delay *= (1.0 + (outage_pressure * (0.45 if provider_outage_active else 0.18)))
        base_delay *= 1.0 + (cooldown_bias_pressure * 0.3)
        max_delay = max(2.0, min(24.0, base_delay * (2.0 + severity + (0.22 * category_factor))))
        multiplier = 1.45 + (severity * 0.62) + ((category_factor - 1.0) * 0.22)
        multiplier += outage_pressure * (0.22 if provider_outage_active else 0.1)
        multiplier += cooldown_bias_pressure * 0.14
        jitter = 0.08 + (severity * 0.24) + (health_pressure * 0.18) + (effective_pressure * 0.08)
        jitter += outage_pressure * (0.14 if provider_outage_active else 0.06)
        jitter += cooldown_bias_pressure * 0.1
        retry_hint = {
            "base_delay_s": round(base_delay, 3),
            "max_delay_s": round(max_delay, 3),
            "multiplier": round(multiplier, 3),
            "jitter_s": round(jitter, 3),
        }
        retry_contract = self._build_retry_contract(
            action=clean_action,
            provider=best_provider,
            operation_class=operation_class,
            category=last_category,
            severity=severity,
            route=route if isinstance(route, dict) else {},
            retry_hint=retry_hint,
            state=best_state,
            action_state=best_action,
            health_pressure=health_pressure,
            effective_pressure=effective_pressure,
            outage_pressure=outage_pressure,
            outage_active=provider_outage_active,
            cooldown_bias_pressure=cooldown_bias_pressure,
        )
        return {
            "status": "success",
            "provider": best_provider,
            "severity": round(severity, 4),
            "last_category": last_category,
            "operation_class": operation_class,
            "health_pressure": round(health_pressure, 4),
            "effective_pressure": round(effective_pressure, 4),
            "failure_trend_ema": round(failure_trend_ema, 4),
            "outage_pressure": round(outage_pressure, 4),
            "outage_active": bool(provider_outage_active),
            "cooldown_bias": round(provider_cooldown_bias, 4),
            "operation_cooldown_bias": round(operation_cooldown_bias, 4),
            "cooldown_bias_pressure": round(cooldown_bias_pressure, 4),
            "action": clean_action,
            "retry_hint": retry_hint,
            "retry_contract": retry_contract,
        }

    def record_outcome(
        self,
        *,
        action: str,
        args: Dict[str, Any] | None,
        status: str,
        error: str = "",
        output: Dict[str, Any] | None = None,
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        clean_action = str(action or "").strip().lower()
        if not self.is_managed_action(clean_action):
            return {"status": "skip"}

        payload = args if isinstance(args, dict) else {}
        out = output if isinstance(output, dict) else {}
        runtime_meta = metadata if isinstance(metadata, dict) else {}
        normalized_status = str(status or "").strip().lower() or "unknown"
        message = str(error or "").strip()
        if not message:
            message = str(out.get("message") or out.get("error") or "").strip()
        category = self._classify_failure_category(message)
        providers = self._providers_from_result(clean_action, payload, out)
        if not providers:
            providers = self._provider_candidates(clean_action, payload)
        if not providers:
            providers = ["default"]

        duration_ms = self._coerce_int(
            runtime_meta.get("__result_duration_ms", out.get("duration_ms", 0)),
            minimum=0,
            maximum=3_600_000,
            default=0,
        )
        operation_class = self._operation_class(clean_action)
        result_attempt = self._coerce_int(
            runtime_meta.get("__result_attempt", 1),
            minimum=1,
            maximum=200,
            default=1,
        )
        route_strategy = str(runtime_meta.get("__external_route_strategy", "")).strip().lower()
        confirm_mode = str(runtime_meta.get("__confirm_policy_mode", "")).strip().lower()
        confirm_satisfied = self._coerce_bool(
            runtime_meta.get("__confirm_policy_satisfied", True),
            default=True,
        )
        retry_contract_mode = str(runtime_meta.get("__external_retry_contract_mode", "")).strip().lower()
        retry_contract_risk = self._coerce_float(
            runtime_meta.get("__external_retry_contract_risk", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        retry_contract_max_attempts = self._coerce_int(
            runtime_meta.get("__external_retry_contract_max_attempts", 0),
            minimum=0,
            maximum=16,
            default=0,
        )
        retry_contract_cooldown_s = self._coerce_float(
            runtime_meta.get("__external_retry_contract_cooldown_s", 0.0),
            minimum=0.0,
            maximum=float(self.max_cooldown_s),
            default=0.0,
        )
        cooldown_events: List[Dict[str, Any]] = []
        outage_events: List[Dict[str, Any]] = []
        now_iso = datetime.now(timezone.utc).isoformat()
        now_ts = time.time()
        with self._lock:
            for provider in providers:
                state = dict(self._provider_states.get(provider, {}))
                samples = self._coerce_int(state.get("samples", 0), minimum=0, maximum=10_000_000, default=0) + 1
                successes = self._coerce_int(state.get("successes", 0), minimum=0, maximum=10_000_000, default=0)
                failures = self._coerce_int(state.get("failures", 0), minimum=0, maximum=10_000_000, default=0)
                consecutive_failures = self._coerce_int(
                    state.get("consecutive_failures", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                )
                failure_ema = self._coerce_float(state.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
                previous_failure_ema = failure_ema
                failure_trend_ema = self._coerce_float(
                    state.get("failure_trend_ema", 0.0),
                    minimum=-1.0,
                    maximum=1.0,
                    default=0.0,
                )
                availability_ema = self._coerce_float(
                    state.get("availability_ema", 0.55),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.55,
                )
                latency_ema_ms = self._coerce_float(
                    state.get("latency_ema_ms", 0.0),
                    minimum=0.0,
                    maximum=3_600_000.0,
                    default=0.0,
                )
                outage_ema = self._coerce_float(
                    state.get("outage_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                outage_streak = self._coerce_int(
                    state.get("outage_streak", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                )
                outage_active = self._coerce_bool(state.get("outage_active", False), default=False)
                outage_since_at = str(state.get("outage_since_at", "")).strip()
                outage_policy_bias = self._coerce_float(
                    state.get("outage_policy_bias", 0.0),
                    minimum=self.outage_policy_bias_min,
                    maximum=self.outage_policy_bias_max,
                    default=0.0,
                )
                cooldown_bias = self._coerce_float(
                    state.get("cooldown_bias", 1.0),
                    minimum=self.cooldown_bias_min,
                    maximum=self.cooldown_bias_max,
                    default=1.0,
                )
                operation_cooldown_bias_rows = state.get("operation_cooldown_bias", {})
                operation_cooldown_bias_map = (
                    operation_cooldown_bias_rows
                    if isinstance(operation_cooldown_bias_rows, dict)
                    else {}
                )
                operation_cooldown_bias = self._coerce_float(
                    operation_cooldown_bias_map.get(operation_class, 1.0),
                    minimum=self.cooldown_bias_min,
                    maximum=self.cooldown_bias_max,
                    default=1.0,
                )
                cooldown_until_ts = self._to_timestamp(state.get("cooldown_until", ""))
                was_cooling_down = cooldown_until_ts > now_ts
                was_outage_active = outage_active
                outage_policy = self._outage_policy_thresholds(state=state, metadata=runtime_meta)
                outage_trip_threshold = self._coerce_float(
                    outage_policy.get("trip_threshold", self.outage_trip_threshold),
                    minimum=0.15,
                    maximum=0.98,
                    default=self.outage_trip_threshold,
                )
                outage_recover_threshold = self._coerce_float(
                    outage_policy.get("recover_threshold", self.outage_recover_threshold),
                    minimum=0.05,
                    maximum=max(0.05, outage_trip_threshold - 0.03),
                    default=min(self.outage_recover_threshold, outage_trip_threshold - 0.03),
                )
                mission_pressure_row = self._mission_outage_pressure(runtime_meta)
                mission_pressure = self._coerce_float(
                    mission_pressure_row.get("pressure", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                mission_worsening = bool(mission_pressure_row.get("worsening", False))
                mission_improving = bool(mission_pressure_row.get("improving", False))
                mission_profile = self._normalize_mission_outage_profile(
                    str(
                        runtime_meta.get(
                            "external_route_profile",
                            self._mission_outage_policy.get("profile", "balanced"),
                        )
                    )
                )
                profile_performance_rows = state.get("profile_performance", {})
                profile_performance_map = (
                    dict(profile_performance_rows)
                    if isinstance(profile_performance_rows, dict)
                    else {}
                )
                profile_performance_row = dict(
                    profile_performance_map.get(mission_profile, {})
                )
                profile_samples = self._coerce_int(
                    profile_performance_row.get("samples", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                ) + 1
                profile_successes = self._coerce_int(
                    profile_performance_row.get("successes", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                )
                profile_failures = self._coerce_int(
                    profile_performance_row.get("failures", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                )
                profile_success_ema = self._coerce_float(
                    profile_performance_row.get("success_ema", 0.5),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.5,
                )

                if normalized_status == "success":
                    successes += 1
                    consecutive_failures = 0
                    failure_signal = 0.0
                    outage_signal = 0.0
                    outage_streak = max(0, outage_streak - 2)
                    profile_successes += 1
                    profile_signal = 1.0
                elif normalized_status in {"failed", "blocked"}:
                    failures += 1
                    consecutive_failures += 1
                    failure_signal = self._CATEGORY_SIGNALS.get(category, self._CATEGORY_SIGNALS["unknown"])
                    outage_streak += 1
                    outage_signal = {
                        "auth": 1.0,
                        "rate_limited": 0.92,
                        "timeout": 0.88,
                        "transient": 0.78,
                        "unknown": 0.62,
                        "non_retryable": 0.35,
                    }.get(category, 0.62)
                    profile_failures += 1
                    profile_signal = 0.0
                else:
                    failure_signal = 0.35
                    outage_signal = 0.25
                    outage_streak = max(0, outage_streak - 1)
                    profile_signal = 0.5
                profile_success_ema = (
                    profile_success_ema * self.profile_performance_decay
                ) + (profile_signal * (1.0 - self.profile_performance_decay))
                profile_success_ema = max(0.0, min(1.0, profile_success_ema))
                profile_success_rate = float(profile_successes) / max(
                    1.0, float(profile_samples)
                )
                profile_performance_map[mission_profile] = {
                    "samples": profile_samples,
                    "successes": profile_successes,
                    "failures": profile_failures,
                    "success_rate": round(profile_success_rate, 6),
                    "success_ema": round(profile_success_ema, 6),
                    "updated_at": now_iso,
                }
                if len(profile_performance_map) > 4:
                    ordered_profile_rows = sorted(
                        profile_performance_map.items(),
                        key=lambda item: (
                            str(item[1].get("updated_at", ""))
                            if isinstance(item[1], dict)
                            else "",
                            self._coerce_int(
                                item[1].get("samples", 0),
                                minimum=0,
                                maximum=10_000_000,
                                default=0,
                            )
                            if isinstance(item[1], dict)
                            else 0,
                            item[0],
                        ),
                        reverse=True,
                    )
                    profile_performance_map = {
                        self._normalize_mission_outage_profile(str(name)): dict(value)
                        for name, value in ordered_profile_rows[:4]
                        if isinstance(value, dict)
                    }

                failure_ema = (failure_ema * self.failure_ema_decay) + (failure_signal * (1.0 - self.failure_ema_decay))
                failure_ema = max(0.0, min(1.0, failure_ema))
                failure_trend_delta = max(-1.0, min(1.0, failure_ema - previous_failure_ema))
                failure_trend_ema = (failure_trend_ema * self.failure_trend_decay) + (
                    failure_trend_delta * (1.0 - self.failure_trend_decay)
                )
                failure_trend_ema = max(-1.0, min(1.0, failure_trend_ema))
                availability_signal = 1.0 if normalized_status == "success" else 0.0 if normalized_status in {"failed", "blocked"} else 0.5
                availability_ema = (availability_ema * 0.9) + (availability_signal * 0.1)
                availability_ema = max(0.0, min(1.0, availability_ema))
                if duration_ms > 0:
                    if latency_ema_ms <= 0:
                        latency_ema_ms = float(duration_ms)
                    else:
                        latency_ema_ms = (latency_ema_ms * 0.84) + (float(duration_ms) * 0.16)
                    latency_ema_ms = max(0.0, min(3_600_000.0, latency_ema_ms))
                outage_ema = (outage_ema * self.outage_ema_decay) + (outage_signal * (1.0 - self.outage_ema_decay))
                outage_ema = max(0.0, min(1.0, outage_ema))
                if (
                    not outage_active
                    and normalized_status in {"failed", "blocked"}
                    and outage_streak >= self.outage_fail_streak_threshold
                    and outage_ema >= outage_trip_threshold
                    and category in {"auth", "rate_limited", "timeout", "transient", "unknown"}
                ):
                    outage_active = True
                    outage_since_at = now_iso
                if outage_active and normalized_status == "success" and outage_ema <= outage_recover_threshold:
                    outage_active = False
                    outage_since_at = ""
                if outage_active != was_outage_active:
                    outage_events.append(
                        {
                            "provider": provider,
                            "outage_active": bool(outage_active),
                            "outage_ema": round(outage_ema, 6),
                            "outage_streak": int(outage_streak),
                            "category": category,
                            "trip_threshold": round(outage_trip_threshold, 6),
                            "recover_threshold": round(outage_recover_threshold, 6),
                        }
                    )
                action_stats = state.get("action_stats", {})
                action_rows = action_stats if isinstance(action_stats, dict) else {}
                action_state = dict(action_rows.get(clean_action, {}))
                action_samples = self._coerce_int(action_state.get("samples", 0), minimum=0, maximum=10_000_000, default=0) + 1
                action_failures = self._coerce_int(action_state.get("failures", 0), minimum=0, maximum=10_000_000, default=0)
                action_successes = self._coerce_int(action_state.get("successes", 0), minimum=0, maximum=10_000_000, default=0)
                action_consecutive_failures = self._coerce_int(
                    action_state.get("consecutive_failures", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                )
                action_failure_ema = self._coerce_float(
                    action_state.get("failure_ema", failure_ema),
                    minimum=0.0,
                    maximum=1.0,
                    default=failure_ema,
                )
                previous_action_failure_ema = action_failure_ema
                action_failure_trend_ema = self._coerce_float(
                    action_state.get("failure_trend_ema", failure_trend_ema),
                    minimum=-1.0,
                    maximum=1.0,
                    default=failure_trend_ema,
                )
                action_latency_ema_ms = self._coerce_float(
                    action_state.get("latency_ema_ms", latency_ema_ms),
                    minimum=0.0,
                    maximum=3_600_000.0,
                    default=latency_ema_ms,
                )
                if normalized_status == "success":
                    action_successes += 1
                    action_consecutive_failures = 0
                elif normalized_status in {"failed", "blocked"}:
                    action_failures += 1
                    action_consecutive_failures += 1
                action_failure_ema = (action_failure_ema * self.action_failure_ema_decay) + (failure_signal * (1.0 - self.action_failure_ema_decay))
                action_failure_ema = max(0.0, min(1.0, action_failure_ema))
                action_trend_delta = max(-1.0, min(1.0, action_failure_ema - previous_action_failure_ema))
                action_failure_trend_ema = (action_failure_trend_ema * self.failure_trend_decay) + (
                    action_trend_delta * (1.0 - self.failure_trend_decay)
                )
                action_failure_trend_ema = max(-1.0, min(1.0, action_failure_trend_ema))
                if duration_ms > 0:
                    if action_latency_ema_ms <= 0:
                        action_latency_ema_ms = float(duration_ms)
                    else:
                        action_latency_ema_ms = (action_latency_ema_ms * 0.82) + (float(duration_ms) * 0.18)
                    action_latency_ema_ms = max(0.0, min(3_600_000.0, action_latency_ema_ms))
                action_state = {
                    "samples": action_samples,
                    "successes": action_successes,
                    "failures": action_failures,
                    "consecutive_failures": action_consecutive_failures,
                    "failure_ema": round(action_failure_ema, 6),
                    "failure_trend_ema": round(action_failure_trend_ema, 6),
                    "latency_ema_ms": round(action_latency_ema_ms, 3),
                    "last_status": normalized_status,
                    "last_category": category,
                    "updated_at": now_iso,
                }
                action_rows[clean_action] = action_state
                if len(action_rows) > self.max_action_stats_per_provider:
                    action_items = sorted(
                        action_rows.items(),
                        key=lambda item: (
                            str(item[1].get("updated_at", "")),
                            self._coerce_int(item[1].get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                            item[0],
                        ),
                        reverse=True,
                    )
                    action_rows = {name: dict(row) for name, row in action_items[: self.max_action_stats_per_provider]}

                operation_stats = state.get("operation_stats", {})
                operation_rows = operation_stats if isinstance(operation_stats, dict) else {}
                operation_state = dict(operation_rows.get(operation_class, {}))
                operation_samples = self._coerce_int(
                    operation_state.get("samples", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                ) + 1
                operation_failures = self._coerce_int(
                    operation_state.get("failures", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                )
                operation_successes = self._coerce_int(
                    operation_state.get("successes", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                )
                operation_consecutive_failures = self._coerce_int(
                    operation_state.get("consecutive_failures", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                )
                operation_failure_ema = self._coerce_float(
                    operation_state.get("failure_ema", failure_ema),
                    minimum=0.0,
                    maximum=1.0,
                    default=failure_ema,
                )
                previous_operation_failure_ema = operation_failure_ema
                operation_failure_trend_ema = self._coerce_float(
                    operation_state.get("failure_trend_ema", action_failure_trend_ema),
                    minimum=-1.0,
                    maximum=1.0,
                    default=action_failure_trend_ema,
                )
                operation_latency_ema_ms = self._coerce_float(
                    operation_state.get("latency_ema_ms", action_latency_ema_ms),
                    minimum=0.0,
                    maximum=3_600_000.0,
                    default=action_latency_ema_ms,
                )
                if normalized_status == "success":
                    operation_successes += 1
                    operation_consecutive_failures = 0
                elif normalized_status in {"failed", "blocked"}:
                    operation_failures += 1
                    operation_consecutive_failures += 1
                operation_failure_ema = (operation_failure_ema * self.action_failure_ema_decay) + (
                    failure_signal * (1.0 - self.action_failure_ema_decay)
                )
                operation_failure_ema = max(0.0, min(1.0, operation_failure_ema))
                operation_trend_delta = max(-1.0, min(1.0, operation_failure_ema - previous_operation_failure_ema))
                operation_failure_trend_ema = (operation_failure_trend_ema * self.failure_trend_decay) + (
                    operation_trend_delta * (1.0 - self.failure_trend_decay)
                )
                operation_failure_trend_ema = max(-1.0, min(1.0, operation_failure_trend_ema))
                if duration_ms > 0:
                    if operation_latency_ema_ms <= 0:
                        operation_latency_ema_ms = float(duration_ms)
                    else:
                        operation_latency_ema_ms = (operation_latency_ema_ms * 0.81) + (float(duration_ms) * 0.19)
                    operation_latency_ema_ms = max(0.0, min(3_600_000.0, operation_latency_ema_ms))
                operation_state = {
                    "samples": operation_samples,
                    "successes": operation_successes,
                    "failures": operation_failures,
                    "consecutive_failures": operation_consecutive_failures,
                    "failure_ema": round(operation_failure_ema, 6),
                    "failure_trend_ema": round(operation_failure_trend_ema, 6),
                    "latency_ema_ms": round(operation_latency_ema_ms, 3),
                    "last_status": normalized_status,
                    "last_category": category,
                    "updated_at": now_iso,
                }
                operation_rows[operation_class] = operation_state
                if len(operation_rows) > self.max_operation_stats_per_provider:
                    operation_items = sorted(
                        operation_rows.items(),
                        key=lambda item: (
                            str(item[1].get("updated_at", "")),
                            self._coerce_int(item[1].get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                            item[0],
                        ),
                        reverse=True,
                    )
                    operation_rows = {
                        name: dict(row) for name, row in operation_items[: self.max_operation_stats_per_provider]
                    }
                if self.cooldown_adaptive_enabled:
                    bias_signal = self._coerce_float(
                        self._CATEGORY_SIGNALS.get(category, self._CATEGORY_SIGNALS["unknown"]),
                        minimum=0.0,
                        maximum=1.0,
                        default=0.0,
                    )
                    if result_attempt > 1:
                        bias_signal = min(1.0, bias_signal + min(0.26, float(result_attempt - 1) * 0.06))
                    if route_strategy in {"fallback_ranked", "override_cooldown"}:
                        bias_signal = min(1.0, bias_signal + 0.08)
                    if confirm_mode and not confirm_satisfied:
                        bias_signal = min(1.0, bias_signal + 0.1)
                    if retry_contract_mode in {"adaptive_backoff", "stabilize", "probe_then_backoff"}:
                        bias_signal = min(1.0, bias_signal + 0.06 + (retry_contract_risk * 0.14))
                    if retry_contract_max_attempts > 0 and result_attempt >= retry_contract_max_attempts:
                        bias_signal = min(1.0, bias_signal + 0.08)
                    if retry_contract_cooldown_s > 0.0:
                        cooldown_contract_pressure = min(
                            1.0,
                            retry_contract_cooldown_s / max(1.0, float(self.max_cooldown_s)),
                        )
                        bias_signal = min(1.0, bias_signal + (cooldown_contract_pressure * 0.12))

                    provider_target = 1.0
                    operation_target = 1.0
                    if normalized_status in {"failed", "blocked"}:
                        provider_target = min(
                            self.cooldown_bias_max,
                            1.0 + (bias_signal * self.cooldown_bias_failure_gain),
                        )
                        operation_target = min(
                            self.cooldown_bias_max,
                            1.0 + (bias_signal * self.cooldown_bias_failure_gain * 1.15),
                        )
                        if category in {"auth", "rate_limited"}:
                            provider_target = min(self.cooldown_bias_max, provider_target + 0.12)
                            operation_target = min(self.cooldown_bias_max, operation_target + 0.15)
                        if retry_contract_mode == "stabilize":
                            provider_target = min(self.cooldown_bias_max, provider_target + 0.14)
                            operation_target = min(self.cooldown_bias_max, operation_target + 0.18)
                        elif retry_contract_mode == "adaptive_backoff":
                            provider_target = min(self.cooldown_bias_max, provider_target + 0.08)
                            operation_target = min(self.cooldown_bias_max, operation_target + 0.1)
                    elif normalized_status == "success":
                        relief = max(0.0, min(1.0, self.cooldown_bias_success_relief))
                        if retry_contract_mode in {"stabilize", "adaptive_backoff"} and retry_contract_risk >= 0.55:
                            relief *= max(0.35, 1.0 - (retry_contract_risk * 0.5))
                        provider_target = max(self.cooldown_bias_min, 1.0 - (relief * 0.08))
                        operation_target = max(self.cooldown_bias_min, 1.0 - (relief * 0.1))
                    else:
                        provider_target = max(self.cooldown_bias_min, min(self.cooldown_bias_max, cooldown_bias))
                        operation_target = max(
                            self.cooldown_bias_min,
                            min(self.cooldown_bias_max, operation_cooldown_bias),
                        )

                    cooldown_bias = self._smooth_cooldown_bias(
                        previous=cooldown_bias,
                        target=provider_target,
                    )
                    operation_cooldown_bias = self._smooth_cooldown_bias(
                        previous=operation_cooldown_bias,
                        target=operation_target,
                    )
                    operation_cooldown_bias_map[operation_class] = round(operation_cooldown_bias, 6)
                    if len(operation_cooldown_bias_map) > self.max_operation_stats_per_provider:
                        ordered_bias = sorted(
                            operation_cooldown_bias_map.items(),
                            key=lambda item: (
                                self._coerce_float(item[1], minimum=self.cooldown_bias_min, maximum=self.cooldown_bias_max, default=1.0),
                                item[0],
                            ),
                            reverse=True,
                        )
                        operation_cooldown_bias_map = {
                            str(name).strip().lower(): round(
                                self._coerce_float(value, minimum=self.cooldown_bias_min, maximum=self.cooldown_bias_max, default=1.0),
                                6,
                            )
                            for name, value in ordered_bias[: self.max_operation_stats_per_provider]
                            if str(name).strip()
                        }
                if self.outage_policy_adaptive_enabled:
                    target_outage_bias = outage_policy_bias
                    if normalized_status in {"failed", "blocked"}:
                        outage_signal = self._coerce_float(
                            self._CATEGORY_SIGNALS.get(category, self._CATEGORY_SIGNALS["unknown"]),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        )
                        target_outage_bias += (outage_signal * 0.12)
                        target_outage_bias += mission_pressure * self.outage_policy_bias_pressure_gain
                        if mission_worsening:
                            target_outage_bias += 0.08
                        if category in {"auth", "rate_limited"}:
                            target_outage_bias += 0.06
                        if route_strategy in {"fallback_ranked", "override_cooldown"}:
                            target_outage_bias += 0.03
                    elif normalized_status == "success":
                        relief = self._coerce_float(
                            self.outage_policy_bias_success_relief,
                            minimum=0.01,
                            maximum=1.0,
                            default=0.18,
                        )
                        target_outage_bias -= relief * (1.0 + mission_pressure)
                        if mission_improving:
                            target_outage_bias -= 0.08
                    outage_policy_bias = self._smooth_outage_policy_bias(
                        previous=outage_policy_bias,
                        target=target_outage_bias,
                    )
                category_counts = state.get("category_counts", {})
                category_rows = category_counts if isinstance(category_counts, dict) else {}
                clean_category = category or "unknown"
                category_rows[clean_category] = self._coerce_int(
                    category_rows.get(clean_category, 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                ) + 1
                last_success_at = str(state.get("last_success_at", "")).strip()
                last_failure_at = str(state.get("last_failure_at", "")).strip()
                if normalized_status == "success":
                    last_success_at = now_iso
                elif normalized_status in {"failed", "blocked"}:
                    last_failure_at = now_iso

                if normalized_status == "success":
                    cooldown_until = ""
                    last_cooldown_s = 0
                else:
                    cooldown_bias_factor = max(
                        self.cooldown_bias_min,
                        min(self.cooldown_bias_max, cooldown_bias * operation_cooldown_bias),
                    )
                    cooldown_s = self._cooldown_seconds(
                        provider=provider,
                        category=category,
                        operation_class=operation_class,
                        consecutive_failures=consecutive_failures,
                        failure_ema=failure_ema,
                        trend_ema=failure_trend_ema,
                        operation_trend_ema=operation_failure_trend_ema,
                        cooldown_bias=cooldown_bias_factor,
                    )
                    if self.cooldown_enabled and cooldown_s > 0:
                        cooldown_until_ts = now_ts + float(cooldown_s)
                        cooldown_until = datetime.fromtimestamp(cooldown_until_ts, tz=timezone.utc).isoformat()
                        last_cooldown_s = cooldown_s
                        cooldown_events.append(
                            {
                                "provider": provider,
                                "cooldown_s": cooldown_s,
                                "category": category,
                                "failure_ema": round(failure_ema, 4),
                                "consecutive_failures": consecutive_failures,
                                "cooldown_bias": round(cooldown_bias_factor, 6),
                                "escalated": not was_cooling_down or cooldown_s >= int(state.get("last_cooldown_s", 0) or 0),
                            }
                        )
                    else:
                        cooldown_until = str(state.get("cooldown_until", "")).strip()
                        last_cooldown_s = self._coerce_int(state.get("last_cooldown_s", 0), minimum=0, maximum=self.max_cooldown_s, default=0)

                self._provider_states[provider] = {
                    "provider": provider,
                    "samples": samples,
                    "successes": successes,
                    "failures": failures,
                    "consecutive_failures": consecutive_failures,
                    "failure_ema": round(failure_ema, 6),
                    "failure_trend_ema": round(failure_trend_ema, 6),
                    "availability_ema": round(availability_ema, 6),
                    "latency_ema_ms": round(latency_ema_ms, 3),
                    "outage_ema": round(outage_ema, 6),
                    "outage_streak": int(outage_streak),
                    "outage_active": bool(outage_active),
                    "outage_since_at": outage_since_at,
                    "outage_policy_bias": round(outage_policy_bias, 6),
                    "cooldown_bias": round(cooldown_bias, 6),
                    "operation_cooldown_bias": {
                        str(name).strip().lower(): round(
                            self._coerce_float(value, minimum=self.cooldown_bias_min, maximum=self.cooldown_bias_max, default=1.0),
                            6,
                        )
                        for name, value in operation_cooldown_bias_map.items()
                        if str(name).strip()
                    },
                    "profile_performance": {
                        self._normalize_mission_outage_profile(str(name)): {
                            "samples": self._coerce_int(
                                value.get("samples", 0),
                                minimum=0,
                                maximum=10_000_000,
                                default=0,
                            ),
                            "successes": self._coerce_int(
                                value.get("successes", 0),
                                minimum=0,
                                maximum=10_000_000,
                                default=0,
                            ),
                            "failures": self._coerce_int(
                                value.get("failures", 0),
                                minimum=0,
                                maximum=10_000_000,
                                default=0,
                            ),
                            "success_rate": round(
                                self._coerce_float(
                                    value.get("success_rate", 0.0),
                                    minimum=0.0,
                                    maximum=1.0,
                                    default=0.0,
                                ),
                                6,
                            ),
                            "success_ema": round(
                                self._coerce_float(
                                    value.get("success_ema", 0.5),
                                    minimum=0.0,
                                    maximum=1.0,
                                    default=0.5,
                                ),
                                6,
                            ),
                            "updated_at": str(value.get("updated_at", "")).strip(),
                        }
                        for name, value in profile_performance_map.items()
                        if isinstance(value, dict)
                    },
                    "last_status": normalized_status,
                    "last_error": message,
                    "last_category": category,
                    "last_action": clean_action,
                    "cooldown_until": cooldown_until,
                    "last_cooldown_s": int(last_cooldown_s),
                    "last_success_at": last_success_at,
                    "last_failure_at": last_failure_at,
                    "category_counts": category_rows,
                    "action_stats": action_rows,
                    "operation_stats": operation_rows,
                    "updated_at": now_iso,
                }

            self._trim_locked()
            self._updates_since_save += 1
            self._maybe_save_locked(force=False)

        return {
            "status": "success",
            "action": clean_action,
            "providers": providers,
            "category": category,
            "operation_class": operation_class,
            "cooldowns": cooldown_events,
            "outage_events": outage_events,
        }

    def snapshot(self, *, provider: str = "", limit: int = 120) -> Dict[str, Any]:
        clean_provider = self._normalize_provider(provider)
        bounded = self._coerce_int(limit, minimum=1, maximum=5000, default=120)
        with self._lock:
            rows = list(self._provider_states.values())
            mission_policy = dict(self._mission_outage_policy)
        if clean_provider:
            rows = [row for row in rows if str(row.get("provider", "")).strip() == clean_provider]
        now_ts = time.time()
        enriched: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            provider = str(row.get("provider", "")).strip().lower()
            if not provider:
                continue
            payload = dict(row)
            health = self._provider_health_row(provider=provider, action="", state=row, now_ts=now_ts)
            payload["health_score"] = self._coerce_float(health.get("health_score", 0.0) if isinstance(health, dict) else 0.0, minimum=0.0, maximum=1.0, default=0.0)
            if isinstance(health, dict):
                payload["outage_policy_bias"] = self._coerce_float(
                    health.get("outage_policy_bias", row.get("outage_policy_bias", 0.0)),
                    minimum=self.outage_policy_bias_min,
                    maximum=self.outage_policy_bias_max,
                    default=0.0,
                )
                payload["outage_mission_pressure"] = self._coerce_float(
                    health.get("outage_mission_pressure", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                payload["trip_threshold"] = self._coerce_float(
                    health.get("trip_threshold", self.outage_trip_threshold),
                    minimum=0.15,
                    maximum=0.98,
                    default=self.outage_trip_threshold,
                )
                payload["recover_threshold"] = self._coerce_float(
                    health.get("recover_threshold", self.outage_recover_threshold),
                    minimum=0.05,
                    maximum=0.95,
                    default=self.outage_recover_threshold,
                )
                payload["route_block_threshold"] = self._coerce_float(
                    health.get("route_block_threshold", self.outage_route_hard_block_threshold),
                    minimum=0.2,
                    maximum=1.0,
                    default=self.outage_route_hard_block_threshold,
                )
                payload["preflight_block_threshold"] = self._coerce_float(
                    health.get("preflight_block_threshold", self.outage_preflight_block_threshold),
                    minimum=0.2,
                    maximum=1.0,
                    default=self.outage_preflight_block_threshold,
                )
                payload["mission_profile_alignment"] = self._coerce_float(
                    health.get("mission_profile_alignment", 0.0),
                    minimum=-1.0,
                    maximum=1.0,
                    default=0.0,
                )
                payload["mission_profile_samples"] = self._coerce_int(
                    health.get("mission_profile_samples", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                )
                payload["mission_profile_success_rate"] = self._coerce_float(
                    health.get("mission_profile_success_rate", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                payload["mission_profile_success_ema"] = self._coerce_float(
                    health.get("mission_profile_success_ema", 0.5),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.5,
                )
            action_rows = row.get("action_stats", {})
            top_actions: List[Dict[str, Any]] = []
            if isinstance(action_rows, dict):
                for action_name, action_state in action_rows.items():
                    if not isinstance(action_state, dict):
                        continue
                    top_actions.append(
                        {
                            "action": str(action_name or "").strip().lower(),
                            "samples": self._coerce_int(action_state.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                            "failures": self._coerce_int(action_state.get("failures", 0), minimum=0, maximum=10_000_000, default=0),
                            "failure_ema": self._coerce_float(action_state.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                            "failure_trend_ema": self._coerce_float(
                                action_state.get("failure_trend_ema", 0.0),
                                minimum=-1.0,
                                maximum=1.0,
                                default=0.0,
                            ),
                            "latency_ema_ms": self._coerce_float(
                                action_state.get("latency_ema_ms", 0.0),
                                minimum=0.0,
                                maximum=3_600_000.0,
                                default=0.0,
                            ),
                            "consecutive_failures": self._coerce_int(
                                action_state.get("consecutive_failures", 0),
                                minimum=0,
                                maximum=10_000_000,
                                default=0,
                            ),
                            "updated_at": str(action_state.get("updated_at", "")).strip(),
                        }
                    )
            top_actions.sort(
                key=lambda item: (
                    -float(item.get("failure_ema", 0.0) or 0.0),
                    -float(item.get("failure_trend_ema", 0.0) or 0.0),
                    -int(item.get("consecutive_failures", 0) or 0),
                    -int(item.get("samples", 0) or 0),
                    str(item.get("action", "")),
                )
            )
            payload["top_action_risks"] = top_actions[:8]
            operation_rows = row.get("operation_stats", {})
            top_operations: List[Dict[str, Any]] = []
            if isinstance(operation_rows, dict):
                for operation_name, operation_state in operation_rows.items():
                    if not isinstance(operation_state, dict):
                        continue
                    top_operations.append(
                        {
                            "operation": str(operation_name or "").strip().lower(),
                            "samples": self._coerce_int(
                                operation_state.get("samples", 0),
                                minimum=0,
                                maximum=10_000_000,
                                default=0,
                            ),
                            "failures": self._coerce_int(
                                operation_state.get("failures", 0),
                                minimum=0,
                                maximum=10_000_000,
                                default=0,
                            ),
                            "failure_ema": self._coerce_float(
                                operation_state.get("failure_ema", 0.0),
                                minimum=0.0,
                                maximum=1.0,
                                default=0.0,
                            ),
                            "failure_trend_ema": self._coerce_float(
                                operation_state.get("failure_trend_ema", 0.0),
                                minimum=-1.0,
                                maximum=1.0,
                                default=0.0,
                            ),
                            "latency_ema_ms": self._coerce_float(
                                operation_state.get("latency_ema_ms", 0.0),
                                minimum=0.0,
                                maximum=3_600_000.0,
                                default=0.0,
                            ),
                            "consecutive_failures": self._coerce_int(
                                operation_state.get("consecutive_failures", 0),
                                minimum=0,
                                maximum=10_000_000,
                                default=0,
                            ),
                            "updated_at": str(operation_state.get("updated_at", "")).strip(),
                        }
                    )
            top_operations.sort(
                key=lambda item: (
                    -float(item.get("failure_ema", 0.0) or 0.0),
                    -float(item.get("failure_trend_ema", 0.0) or 0.0),
                    -int(item.get("consecutive_failures", 0) or 0),
                    -int(item.get("samples", 0) or 0),
                    str(item.get("operation", "")),
                )
            )
            payload["top_operation_risks"] = top_operations[:6]
            enriched.append(payload)
        enriched.sort(
            key=lambda row: (
                float(row.get("health_score", 0.0) or 0.0),
                -self._coerce_float(row.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                -self._coerce_int(row.get("consecutive_failures", 0), minimum=0, maximum=1_000_000, default=0),
                str(row.get("provider", "")),
            )
        )
        return {
            "status": "success",
            "enabled": bool(self.cooldown_enabled),
            "provider_routing_enabled": bool(self.provider_routing_enabled),
            "contract_strict": bool(self.preflight_contract_strict),
            "provider_contract_strict": bool(self.preflight_provider_contract_strict),
            "retry_autotune_enabled": bool(self.autotune_enabled),
            "mission_outage_autotune_enabled": bool(self.mission_outage_autotune_enabled),
            "mission_outage_profile_autotune_enabled": bool(self.mission_outage_profile_autotune_enabled),
            "mission_provider_policy_autotune_enabled": bool(self.mission_provider_policy_autotune_enabled),
            "mission_outage_capability_bias_gain": round(float(self.mission_outage_capability_bias_gain), 6),
            "mission_outage_capability_bias_decay": round(float(self.mission_outage_capability_bias_decay), 6),
            "mission_outage_capability_limit": int(self.mission_outage_capability_limit),
            "mission_analysis_history_count": len(self._mission_analysis_history),
            "profile_performance_enabled": bool(self.profile_performance_enabled),
            "profile_performance_min_samples": int(self.profile_performance_min_samples),
            "profile_performance_bonus_weight": round(float(self.profile_performance_bonus_weight), 6),
            "profile_performance_penalty_weight": round(float(self.profile_performance_penalty_weight), 6),
            "mission_outage_policy": {
                "bias": round(
                    self._coerce_float(
                        mission_policy.get("bias", 0.0),
                        minimum=self.mission_outage_bias_min,
                        maximum=self.mission_outage_bias_max,
                        default=0.0,
                    ),
                    6,
                ),
                "pressure_ema": round(self._coerce_float(mission_policy.get("pressure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6),
                "risk_ema": round(self._coerce_float(mission_policy.get("risk_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6),
                "quality_ema": round(self._coerce_float(mission_policy.get("quality_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6),
                "failed_ratio_ema": round(
                    self._coerce_float(mission_policy.get("failed_ratio_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                    6,
                ),
                "blocked_ratio_ema": round(
                    self._coerce_float(mission_policy.get("blocked_ratio_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                    6,
                ),
                "mode": str(mission_policy.get("mode", "stable")).strip().lower() or "stable",
                "profile": self._normalize_mission_outage_profile(str(mission_policy.get("profile", "balanced"))),
                "profile_confidence": round(
                    self._coerce_float(mission_policy.get("profile_confidence", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                    6,
                ),
                "profile_pressure_ema": round(
                    self._coerce_float(mission_policy.get("profile_pressure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                    6,
                ),
                "profile_stability_ema": round(
                    self._coerce_float(mission_policy.get("profile_stability_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                    6,
                ),
                "profile_switch_count": self._coerce_int(
                    mission_policy.get("profile_switch_count", 0),
                    minimum=0,
                    maximum=1_000_000,
                    default=0,
                ),
                "profile_last_switch_at": str(mission_policy.get("profile_last_switch_at", "")).strip(),
                "profile_last_reason": str(mission_policy.get("profile_last_reason", "")).strip(),
                "capability_bias": self._serialize_mission_capability_bias(mission_policy.get("capability_bias", {})),
                "updated_at": str(mission_policy.get("updated_at", "")).strip(),
                "last_reason": str(mission_policy.get("last_reason", "")).strip(),
            },
            "count": min(len(enriched), bounded),
            "total": len(enriched),
            "items": [dict(row) for row in enriched[:bounded]],
        }

    def mission_policy_status(
        self,
        *,
        provider_limit: int = 16,
        history_limit: int = 24,
        history_window: int = 36,
    ) -> Dict[str, Any]:
        bounded_provider_limit = self._coerce_int(provider_limit, minimum=1, maximum=200, default=16)
        bounded_history_limit = self._coerce_int(
            history_limit,
            minimum=1,
            maximum=max(1, self.mission_outage_profile_history_limit),
            default=24,
        )
        bounded_history_window = self._coerce_int(history_window, minimum=4, maximum=1200, default=36)
        snapshot = self.snapshot(limit=max(bounded_provider_limit * 4, bounded_provider_limit))
        history_snapshot = self.mission_analysis_history(
            limit=max(bounded_history_limit * 4, bounded_history_limit),
            window=bounded_history_window,
        )
        with self._lock:
            mission_policy = dict(self._mission_outage_policy)

        profile_history_raw = mission_policy.get("profile_history", [])
        profile_history = (
            [dict(row) for row in profile_history_raw if isinstance(row, dict)]
            if isinstance(profile_history_raw, list)
            else []
        )
        if len(profile_history) > bounded_history_limit:
            profile_history = profile_history[-bounded_history_limit:]

        capability_bias = self._serialize_mission_capability_bias(mission_policy.get("capability_bias", {}))
        capability_rows: List[Dict[str, Any]] = []
        for capability, row in capability_bias.items():
            if not isinstance(row, dict):
                continue
            capability_rows.append(
                {
                    "capability": self._normalize_mission_capability(str(capability)),
                    "bias": round(
                        self._coerce_float(
                            row.get("bias", 0.0),
                            minimum=self.mission_outage_bias_min,
                            maximum=self.mission_outage_bias_max,
                            default=0.0,
                        ),
                        6,
                    ),
                    "pressure_ema": round(
                        self._coerce_float(row.get("pressure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "samples": self._coerce_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                    "weight": round(
                        self._coerce_float(row.get("weight", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "top_action": str(row.get("top_action", "")).strip().lower(),
                    "updated_at": str(row.get("updated_at", "")).strip(),
                }
            )
        capability_rows.sort(
            key=lambda row: (
                abs(float(row.get("bias", 0.0) or 0.0)),
                float(row.get("pressure_ema", 0.0) or 0.0),
                int(row.get("samples", 0) or 0),
                str(row.get("capability", "")),
            ),
            reverse=True,
        )

        profile_switches = 0
        previous_profile = ""
        unique_profiles: set[str] = set()
        unique_modes: set[str] = set()
        for row in profile_history:
            profile_name = self._normalize_mission_outage_profile(str(row.get("profile", "")))
            mode_name = str(row.get("mode", "")).strip().lower()
            if profile_name:
                unique_profiles.add(profile_name)
                if previous_profile and previous_profile != profile_name:
                    profile_switches += 1
                previous_profile = profile_name
            if mode_name:
                unique_modes.add(mode_name)

        provider_rows_raw = snapshot.get("items", []) if isinstance(snapshot, dict) else []
        provider_rows = [dict(row) for row in provider_rows_raw if isinstance(row, dict)]
        provider_bias_rows: List[Dict[str, Any]] = []
        for row in provider_rows:
            provider = self._normalize_provider(str(row.get("provider", "")).strip())
            if not provider:
                continue
            operation_bias_map = (
                row.get("operation_cooldown_bias", {})
                if isinstance(row.get("operation_cooldown_bias"), dict)
                else {}
            )
            top_operation_bias = []
            for operation_name, value in operation_bias_map.items():
                operation = str(operation_name or "").strip().lower()
                if not operation:
                    continue
                bias_value = self._coerce_float(
                    value,
                    minimum=self.cooldown_bias_min,
                    maximum=self.cooldown_bias_max,
                    default=1.0,
                )
                top_operation_bias.append(
                    {
                        "operation": operation,
                        "bias": round(bias_value, 6),
                        "deviation": round(abs(bias_value - 1.0), 6),
                    }
                )
            top_operation_bias.sort(
                key=lambda item: (
                    float(item.get("deviation", 0.0) or 0.0),
                    float(item.get("bias", 0.0) or 0.0),
                    str(item.get("operation", "")),
                ),
                reverse=True,
            )
            provider_bias_rows.append(
                {
                    "provider": provider,
                    "health_score": round(
                        self._coerce_float(row.get("health_score", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "failure_ema": round(
                        self._coerce_float(row.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "outage_ema": round(
                        self._coerce_float(row.get("outage_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "outage_policy_bias": round(
                        self._coerce_float(
                            row.get("outage_policy_bias", 0.0),
                            minimum=self.outage_policy_bias_min,
                            maximum=self.outage_policy_bias_max,
                            default=0.0,
                        ),
                        6,
                    ),
                    "cooldown_bias": round(
                        self._coerce_float(
                            row.get("cooldown_bias", 1.0),
                            minimum=self.cooldown_bias_min,
                            maximum=self.cooldown_bias_max,
                            default=1.0,
                        ),
                        6,
                    ),
                    "mission_pressure": round(
                        self._coerce_float(row.get("outage_mission_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "trip_threshold": round(
                        self._coerce_float(row.get("trip_threshold", self.outage_trip_threshold), minimum=0.15, maximum=0.98, default=self.outage_trip_threshold),
                        6,
                    ),
                    "recover_threshold": round(
                        self._coerce_float(
                            row.get("recover_threshold", self.outage_recover_threshold),
                            minimum=0.05,
                            maximum=0.95,
                            default=self.outage_recover_threshold,
                        ),
                        6,
                    ),
                    "route_block_threshold": round(
                        self._coerce_float(
                            row.get("route_block_threshold", self.outage_route_hard_block_threshold),
                            minimum=0.2,
                            maximum=1.0,
                            default=self.outage_route_hard_block_threshold,
                        ),
                        6,
                    ),
                    "preflight_block_threshold": round(
                        self._coerce_float(
                            row.get("preflight_block_threshold", self.outage_preflight_block_threshold),
                            minimum=0.2,
                            maximum=1.0,
                            default=self.outage_preflight_block_threshold,
                        ),
                        6,
                    ),
                    "mission_profile_alignment": round(
                        self._coerce_float(
                            row.get("mission_profile_alignment", 0.0),
                            minimum=-1.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        6,
                    ),
                    "mission_profile_samples": self._coerce_int(
                        row.get("mission_profile_samples", 0),
                        minimum=0,
                        maximum=10_000_000,
                        default=0,
                    ),
                    "cooldown_active": bool(row.get("cooldown_active", False)),
                    "outage_active": bool(row.get("outage_active", False)),
                    "top_operation_bias": top_operation_bias[:4],
                    "top_action_risks": [
                        dict(item)
                        for item in (
                            row.get("top_action_risks", [])
                            if isinstance(row.get("top_action_risks"), list)
                            else []
                        )[:3]
                        if isinstance(item, dict)
                    ],
                    "top_operation_risks": [
                        dict(item)
                        for item in (
                            row.get("top_operation_risks", [])
                            if isinstance(row.get("top_operation_risks"), list)
                            else []
                        )[:3]
                        if isinstance(item, dict)
                    ],
                    "updated_at": str(row.get("updated_at", "")).strip(),
                }
            )
        provider_bias_rows.sort(
            key=lambda row: (
                abs(float(row.get("outage_policy_bias", 0.0) or 0.0)),
                abs(float(row.get("cooldown_bias", 1.0) or 1.0) - 1.0),
                float(row.get("mission_pressure", 0.0) or 0.0),
                float(row.get("failure_ema", 0.0) or 0.0),
                str(row.get("provider", "")),
            ),
            reverse=True,
        )
        provider_bias_rows = provider_bias_rows[:bounded_provider_limit]

        history_diagnostics_raw = history_snapshot.get("diagnostics", {}) if isinstance(history_snapshot, dict) else {}
        history_diagnostics = history_diagnostics_raw if isinstance(history_diagnostics_raw, dict) else {}
        recent_history_rows_raw = history_snapshot.get("items", []) if isinstance(history_snapshot, dict) else []
        recent_history_rows = [
            dict(item)
            for item in recent_history_rows_raw[-bounded_history_limit:]
            if isinstance(item, dict)
        ]

        return {
            "status": "success",
            "config": {
                **self._mission_policy_runtime_config_snapshot(),
                "mission_outage_profile_history_limit": int(self.mission_outage_profile_history_limit),
                "mission_analysis_history_limit": int(self.mission_analysis_history_limit),
            },
            "policy": {
                **self._mission_outage_policy_default_state(),
                "bias": round(
                    self._coerce_float(
                        mission_policy.get("bias", 0.0),
                        minimum=self.mission_outage_bias_min,
                        maximum=self.mission_outage_bias_max,
                        default=0.0,
                    ),
                    6,
                ),
                "pressure_ema": round(self._coerce_float(mission_policy.get("pressure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6),
                "risk_ema": round(self._coerce_float(mission_policy.get("risk_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6),
                "quality_ema": round(self._coerce_float(mission_policy.get("quality_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6),
                "failed_ratio_ema": round(self._coerce_float(mission_policy.get("failed_ratio_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6),
                "blocked_ratio_ema": round(self._coerce_float(mission_policy.get("blocked_ratio_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6),
                "mode": str(mission_policy.get("mode", "stable")).strip().lower() or "stable",
                "profile": self._normalize_mission_outage_profile(str(mission_policy.get("profile", "balanced"))),
                "profile_confidence": round(
                    self._coerce_float(mission_policy.get("profile_confidence", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                    6,
                ),
                "profile_pressure_ema": round(
                    self._coerce_float(mission_policy.get("profile_pressure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                    6,
                ),
                "profile_stability_ema": round(
                    self._coerce_float(mission_policy.get("profile_stability_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                    6,
                ),
                "profile_switch_count": self._coerce_int(
                    mission_policy.get("profile_switch_count", 0),
                    minimum=0,
                    maximum=1_000_000,
                    default=0,
                ),
                "profile_last_switch_at": str(mission_policy.get("profile_last_switch_at", "")).strip(),
                "profile_last_reason": str(mission_policy.get("profile_last_reason", "")).strip(),
                "profile_history": profile_history,
                "profile_history_count": len(profile_history),
                "capability_bias": capability_bias,
                "capability_rows": capability_rows[: min(12, len(capability_rows))],
                "updated_at": str(mission_policy.get("updated_at", "")).strip(),
                "last_reason": str(mission_policy.get("last_reason", "")).strip(),
            },
            "profile_history_analysis": {
                "history_count": len(profile_history),
                "switch_count": int(profile_switches),
                "unique_profiles": sorted(unique_profiles),
                "unique_modes": sorted(unique_modes),
                "latest_profile": self._normalize_mission_outage_profile(
                    str(profile_history[-1].get("profile", mission_policy.get("profile", "balanced")))
                )
                if profile_history
                else self._normalize_mission_outage_profile(str(mission_policy.get("profile", "balanced"))),
                "latest_mode": str(profile_history[-1].get("mode", mission_policy.get("mode", "stable"))).strip().lower()
                if profile_history
                else str(mission_policy.get("mode", "stable")).strip().lower(),
            },
            "history": {
                "count": self._coerce_int(history_snapshot.get("count", 0), minimum=0, maximum=100_000, default=0),
                "total": self._coerce_int(history_snapshot.get("total", 0), minimum=0, maximum=100_000, default=0),
                "window": bounded_history_window,
                "diagnostics": {
                    "mode": str(history_diagnostics.get("mode", "")).strip().lower(),
                    "drift_score": round(
                        self._coerce_float(history_diagnostics.get("drift_score", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "switch_pressure": round(
                        self._coerce_float(history_diagnostics.get("switch_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "recent_pressure_score": round(
                        self._coerce_float(
                            history_diagnostics.get("recent_pressure_score", 0.0),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        6,
                    ),
                },
                "items": recent_history_rows,
            },
            "provider_count": len(provider_rows),
            "provider_biases": provider_bias_rows,
        }

    def reset(self, *, provider: str = "") -> Dict[str, Any]:
        clean_provider = self._normalize_provider(provider)
        removed = 0
        with self._lock:
            if clean_provider:
                if clean_provider in self._provider_states:
                    self._provider_states.pop(clean_provider, None)
                    removed = 1
            else:
                removed = len(self._provider_states)
                self._provider_states = {}
            self._maybe_save_locked(force=True)
        return {"status": "success", "provider": clean_provider, "removed": removed}

    def update_mission_policy_config(
        self,
        *,
        config: Dict[str, Any] | None = None,
        persist_now: bool = True,
    ) -> Dict[str, Any]:
        payload = dict(config) if isinstance(config, dict) else {}
        reset_requested = self._coerce_bool(payload.get("reset_config", False), default=False)
        with self._lock:
            before = self._mission_policy_runtime_config_snapshot()
            baseline_validation = self._build_mission_policy_config_validation(
                requested_payload={},
                before=before,
                current=before,
                changed={},
                reset_requested=False,
            )
            resolved_actions: List[Dict[str, Any]] = []
            effective_payload = dict(payload)
            if not reset_requested:
                effective_payload, resolved_actions = self._resolve_mission_policy_config_request(
                    payload=payload,
                    current=before,
                    validation=baseline_validation,
                )
            requested_payload = {
                str(key): value
                for key, value in effective_payload.items()
                if str(key) not in {"reset_config", "preset_id", "apply_recommended_preset", "remediation_action"}
            }
            changed = self._apply_mission_policy_runtime_config(requested_payload, reset=reset_requested)
            updated = bool(changed) or bool(reset_requested)
            if updated:
                self._updates_since_save += 1
                self._maybe_save_locked(force=bool(persist_now))
            current = self._mission_policy_runtime_config_snapshot()
            validation = self._build_mission_policy_config_validation(
                requested_payload=requested_payload,
                before=before,
                current=current,
                changed=changed,
                reset_requested=reset_requested,
            )
            validation["resolved_actions"] = resolved_actions
        return {
            "status": "success",
            "updated": bool(updated),
            "reset": bool(reset_requested),
            "changed": changed,
            "config": current,
            "validation": validation,
        }

    def _resolve_mission_policy_config_request(
        self,
        *,
        payload: Dict[str, Any],
        current: Dict[str, Any],
        validation: Dict[str, Any],
    ) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cleaned_payload = dict(payload)
        explicit_payload = {
            str(key): value
            for key, value in cleaned_payload.items()
            if str(key) not in {"reset_config", "preset_id", "apply_recommended_preset", "remediation_action"}
        }
        resolved_actions: List[Dict[str, Any]] = []

        preset_id = str(cleaned_payload.get("preset_id", "") or "").strip().lower()
        apply_recommended = self._coerce_bool(cleaned_payload.get("apply_recommended_preset", False), default=False)
        preset_resolution = self._resolve_mission_policy_config_preset(
            current=current,
            validation=validation,
            preset_id=preset_id,
            apply_recommended=apply_recommended,
        )
        if preset_resolution is not None:
            preset_changes = (
                preset_resolution.get("changes", {})
                if isinstance(preset_resolution.get("changes"), dict)
                else {}
            )
            explicit_payload = {
                **{str(key): value for key, value in preset_changes.items()},
                **explicit_payload,
            }
            resolved_actions.append(
                {
                    "kind": "preset",
                    "id": str(preset_resolution.get("id", "")).strip().lower(),
                    "label": str(preset_resolution.get("label", "")).strip(),
                    "reason": str(preset_resolution.get("reason", "")).strip(),
                }
            )

        remediation_action = str(cleaned_payload.get("remediation_action", "") or "").strip().lower()
        remediation_resolution = self._resolve_mission_policy_config_remediation(
            current=current,
            validation=validation,
            action_id=remediation_action,
        )
        if remediation_resolution is not None:
            remediation_patch = (
                remediation_resolution.get("config_patch", {})
                if isinstance(remediation_resolution.get("config_patch"), dict)
                else {}
            )
            explicit_payload = {
                **{str(key): value for key, value in remediation_patch.items()},
                **explicit_payload,
            }
            resolved_actions.append(
                {
                    "kind": "remediation",
                    "id": str(remediation_resolution.get("action", "")).strip().lower(),
                    "label": str(remediation_resolution.get("button_label", "") or remediation_resolution.get("action", "")).strip(),
                    "reason": str(remediation_resolution.get("message", "")).strip(),
                }
            )

        return explicit_payload, resolved_actions

    def _resolve_mission_policy_config_preset(
        self,
        *,
        current: Dict[str, Any],
        validation: Dict[str, Any],
        preset_id: str,
        apply_recommended: bool,
    ) -> Optional[Dict[str, Any]]:
        presets = validation.get("presets", []) if isinstance(validation.get("presets"), list) else []
        rows = [row for row in presets if isinstance(row, dict)]
        if not rows:
            return None
        target_id = preset_id
        if not target_id and apply_recommended:
            target_id = str(validation.get("recommended_preset_id", "")).strip().lower()
        if not target_id:
            return None
        for row in rows:
            if str(row.get("id", "")).strip().lower() == target_id:
                return dict(row)
        return None

    def _resolve_mission_policy_config_remediation(
        self,
        *,
        current: Dict[str, Any],
        validation: Dict[str, Any],
        action_id: str,
    ) -> Optional[Dict[str, Any]]:
        if not action_id:
            return None
        remediation_hints = (
            validation.get("remediation_hints", [])
            if isinstance(validation.get("remediation_hints"), list)
            else []
        )
        for row in remediation_hints:
            if not isinstance(row, dict):
                continue
            if str(row.get("action", "")).strip().lower() == action_id:
                return dict(row)
        return None

    @staticmethod
    def _mission_policy_config_value_equal(left: Any, right: Any) -> bool:
        if isinstance(left, bool) and isinstance(right, bool):
            return left is right
        if (
            isinstance(left, (int, float))
            and not isinstance(left, bool)
            and isinstance(right, (int, float))
            and not isinstance(right, bool)
        ):
            return abs(float(left) - float(right)) < 0.000001
        return left == right

    def _build_mission_policy_config_validation(
        self,
        *,
        requested_payload: Dict[str, Any],
        before: Dict[str, Any],
        current: Dict[str, Any],
        changed: Dict[str, Any],
        reset_requested: bool,
    ) -> Dict[str, Any]:
        normalized: List[Dict[str, Any]] = []
        warnings: List[Dict[str, Any]] = []
        remediation_hints: List[Dict[str, Any]] = []
        requested_keys = sorted(str(key) for key in requested_payload.keys() if str(key) != "reset_config")
        for key in requested_keys:
            if key not in current:
                normalized.append(
                    {
                        "field": key,
                        "requested": requested_payload.get(key),
                        "applied": None,
                        "reason": "unsupported_key_ignored",
                    }
                )
                continue
            requested_value = requested_payload.get(key)
            applied_value = current.get(key)
            if not self._mission_policy_config_value_equal(requested_value, applied_value):
                normalized.append(
                    {
                        "field": key,
                        "requested": requested_value,
                        "applied": applied_value,
                        "reason": "normalized_or_clamped",
                    }
                )

        route_block_threshold = float(current.get("outage_route_hard_block_threshold", 0.86) or 0.86)
        preflight_block_threshold = float(current.get("outage_preflight_block_threshold", 0.92) or 0.92)
        trip_threshold = float(current.get("outage_trip_threshold", 0.62) or 0.62)
        recover_threshold = float(current.get("outage_recover_threshold", 0.36) or 0.36)
        bias_gain = float(current.get("mission_outage_bias_gain", 0.48) or 0.48)
        bias_decay = float(current.get("mission_outage_bias_decay", 0.8) or 0.8)
        profile_decay = float(current.get("mission_outage_profile_decay", 0.78) or 0.78)
        profile_stability_decay = float(current.get("mission_outage_profile_stability_decay", 0.82) or 0.82)
        capability_bias_gain = float(current.get("mission_outage_capability_bias_gain", 0.24) or 0.24)
        capability_bias_decay = float(current.get("mission_outage_capability_bias_decay", 0.84) or 0.84)
        capability_limit = int(current.get("mission_outage_capability_limit", 12) or 12)
        provider_max = int(current.get("provider_policy_max_providers", 80) or 80)

        if preflight_block_threshold <= route_block_threshold:
            warnings.append(
                {
                    "code": "preflight_threshold_aligned_to_route_threshold",
                    "severity": "high",
                    "message": (
                        "Preflight block threshold was raised to remain above the route hard-block threshold."
                    ),
                }
            )
            remediation_hints.append(
                {
                    "action": "review_preflight_headroom",
                    "message": "Keep preflight block threshold at least 0.03 above route hard-block threshold.",
                    "button_label": "Apply Headroom Fix",
                    "config_patch": {
                        "outage_preflight_block_threshold": round(min(1.0, route_block_threshold + 0.05), 6),
                    },
                    "preset_id": "stability_guard",
                }
            )
        elif (preflight_block_threshold - route_block_threshold) < 0.03:
            warnings.append(
                {
                    "code": "preflight_threshold_headroom_low",
                    "severity": "medium",
                    "message": (
                        "Preflight block threshold is only slightly above the route hard-block threshold, "
                        "which can reduce recovery headroom."
                    ),
                }
            )
            remediation_hints.append(
                {
                    "action": "review_preflight_headroom",
                    "message": "Increase preflight block threshold headroom to improve degraded-route fallback behavior.",
                    "button_label": "Increase Headroom",
                    "config_patch": {
                        "outage_preflight_block_threshold": round(min(1.0, route_block_threshold + 0.05), 6),
                    },
                    "preset_id": "stability_guard",
                }
            )

        if recover_threshold >= max(0.05, trip_threshold - 0.03):
            warnings.append(
                {
                    "code": "recover_threshold_close_to_trip_threshold",
                    "severity": "medium",
                    "message": "Recovery threshold is close to the trip threshold and may cause breaker flapping.",
                }
            )
            remediation_hints.append(
                {
                    "action": "widen_trip_recover_gap",
                    "message": "Lower recovery threshold or raise trip threshold to preserve at least 0.03 gap.",
                    "button_label": "Widen Recovery Gap",
                    "config_patch": {
                        "outage_recover_threshold": round(min(recover_threshold, max(0.05, trip_threshold - 0.08)), 6),
                    },
                    "preset_id": "stability_guard",
                }
            )

        if bias_gain >= 1.2 and bias_decay <= 0.68:
            warnings.append(
                {
                    "code": "aggressive_bias_feedback_loop",
                    "severity": "medium",
                    "message": "High bias gain combined with low bias decay can overreact to short mission spikes.",
                }
            )
            remediation_hints.append(
                {
                    "action": "stabilize_bias_feedback",
                    "message": "Reduce mission outage bias gain and increase decay to avoid overreacting to short spikes.",
                    "button_label": "Stabilize Bias Loop",
                    "config_patch": {
                        "mission_outage_bias_gain": round(min(bias_gain, 0.72), 6),
                        "mission_outage_bias_decay": round(max(bias_decay, 0.82), 6),
                    },
                    "preset_id": "stability_guard",
                }
            )

        if profile_decay <= 0.62 and profile_stability_decay <= 0.72:
            warnings.append(
                {
                    "code": "profile_decay_too_reactive",
                    "severity": "medium",
                    "message": (
                        "Profile decay and stability decay are both low, which can cause mission profile oscillation "
                        "under short-lived reliability swings."
                    ),
                }
            )
            remediation_hints.append(
                {
                    "action": "stabilize_profile_decay",
                    "message": "Increase profile decay and profile stability decay to reduce mission profile oscillation.",
                    "button_label": "Stabilize Profile Decay",
                    "config_patch": {
                        "mission_outage_profile_decay": round(max(profile_decay, 0.84), 6),
                        "mission_outage_profile_stability_decay": round(max(profile_stability_decay, 0.88), 6),
                        "mission_outage_profile_hysteresis": round(
                            max(float(current.get("mission_outage_profile_hysteresis", 0.09) or 0.09), 0.1),
                            6,
                        ),
                    },
                    "preset_id": "stability_guard",
                }
            )

        if capability_bias_gain >= 0.55 and capability_limit <= 6:
            warnings.append(
                {
                    "code": "capability_bias_scope_too_narrow",
                    "severity": "medium",
                    "message": (
                        "Capability bias gain is aggressive while capability memory scope is narrow, which can "
                        "overfit routing to too few capability classes."
                    ),
                }
            )
            remediation_hints.append(
                {
                    "action": "expand_capability_scope",
                    "message": "Expand capability memory scope and soften capability feedback to avoid route overfitting.",
                    "button_label": "Expand Capability Scope",
                    "config_patch": {
                        "mission_outage_capability_bias_gain": round(min(capability_bias_gain, 0.34), 6),
                        "mission_outage_capability_bias_decay": round(max(capability_bias_decay, 0.88), 6),
                        "mission_outage_capability_limit": int(max(capability_limit, 18)),
                    },
                    "preset_id": "capability_expansion",
                }
            )

        if provider_max <= 4:
            warnings.append(
                {
                    "code": "provider_candidate_pool_narrow",
                    "severity": "medium",
                    "message": "Provider policy max providers is narrow and may reduce fallback routing coverage.",
                }
            )
            remediation_hints.append(
                {
                    "action": "expand_provider_candidate_pool",
                    "message": "Increase provider candidate pool size so fallback routing has enough healthy alternatives.",
                    "button_label": "Expand Provider Pool",
                    "config_patch": {
                        "provider_policy_max_providers": int(max(provider_max, 12)),
                    },
                    "preset_id": "capability_expansion",
                }
            )

        if reset_requested:
            remediation_hints.append(
                {
                    "action": "rerun_mission_policy_analysis",
                    "message": "After reset, rerun mission policy analysis to rebuild profile drift and provider bias baselines.",
                }
            )

        changed_fields = sorted(str(key) for key in changed.keys())
        drift_fields = sorted(
            key
            for key in current.keys()
            if key in before and not self._mission_policy_config_value_equal(before.get(key), current.get(key))
        )
        presets = self._mission_policy_config_presets(
            current=current,
            metrics={
                "trip_threshold": trip_threshold,
                "recover_threshold": recover_threshold,
                "route_block_threshold": route_block_threshold,
                "preflight_block_threshold": preflight_block_threshold,
                "bias_gain": bias_gain,
                "bias_decay": bias_decay,
                "profile_decay": profile_decay,
                "profile_stability_decay": profile_stability_decay,
                "capability_bias_gain": capability_bias_gain,
                "capability_bias_decay": capability_bias_decay,
                "capability_limit": capability_limit,
                "provider_policy_max_providers": provider_max,
            },
            warning_codes=[str(row.get("code", "")).strip().lower() for row in warnings if isinstance(row, dict)],
        )
        history_context = self._mission_policy_validation_history_context()
        recommended_preset_id = ""
        if presets:
            for preset in presets:
                if isinstance(preset, dict) and bool(preset.get("recommended", False)):
                    recommended_preset_id = str(preset.get("id", "")).strip().lower()
                    break
            if not recommended_preset_id:
                recommended_preset_id = str(presets[0].get("id", "")).strip().lower()
        if recommended_preset_id:
            remediation_hints.append(
                {
                    "action": "apply_recommended_preset",
                    "message": "Apply the backend-recommended mission policy preset for the current reliability profile.",
                    "button_label": "Apply Recommended Preset",
                    "preset_id": recommended_preset_id,
                }
            )
        decision_trace = self._mission_policy_validation_decision_trace(
            warnings=warnings,
            history_context=history_context,
            metrics={
                "trip_threshold": trip_threshold,
                "recover_threshold": recover_threshold,
                "route_block_threshold": route_block_threshold,
                "preflight_block_threshold": preflight_block_threshold,
                "bias_gain": bias_gain,
                "bias_decay": bias_decay,
                "profile_decay": profile_decay,
                "profile_stability_decay": profile_stability_decay,
                "capability_bias_gain": capability_bias_gain,
                "capability_bias_decay": capability_bias_decay,
                "capability_limit": capability_limit,
                "provider_policy_max_providers": provider_max,
                "trip_recover_gap": max(0.0, trip_threshold - recover_threshold),
                "route_preflight_gap": max(0.0, preflight_block_threshold - route_block_threshold),
            },
            recommended_preset_id=recommended_preset_id,
        )
        return {
            "summary": {
                "normalized_count": len(normalized),
                "warning_count": len(warnings),
                "changed_count": len(changed_fields),
                "reset_requested": bool(reset_requested),
            },
            "normalized": normalized[:16],
            "warnings": warnings[:8],
            "remediation_hints": remediation_hints[:8],
            "presets": presets[:6],
            "recommended_preset_id": recommended_preset_id,
            "history_context": history_context,
            "decision_trace": decision_trace,
            "changed_fields": changed_fields[:24],
            "drift_fields": drift_fields[:24],
            "metrics": {
                "trip_recover_gap": round(max(0.0, trip_threshold - recover_threshold), 6),
                "route_preflight_gap": round(max(0.0, preflight_block_threshold - route_block_threshold), 6),
                "bias_gain": round(bias_gain, 6),
                "bias_decay": round(bias_decay, 6),
                "profile_decay": round(profile_decay, 6),
                "profile_stability_decay": round(profile_stability_decay, 6),
                "capability_bias_gain": round(capability_bias_gain, 6),
                "capability_bias_decay": round(capability_bias_decay, 6),
                "capability_limit": capability_limit,
                "provider_policy_max_providers": provider_max,
            },
        }

    def _mission_policy_validation_history_context(self) -> Dict[str, Any]:
        rows = [dict(item) for item in self._mission_analysis_history if isinstance(item, dict)]
        diagnostics = self._mission_analysis_drift_diagnostics(rows=rows, window=min(36, max(8, len(rows) or 8)))
        recent_rows = rows[-8:] if len(rows) > 8 else rows
        with self._lock:
            mission_policy = dict(self._mission_outage_policy)
            provider_rows = [dict(row) for row in self._provider_states.values() if isinstance(row, dict)]
        capability_bias = self._serialize_mission_capability_bias(mission_policy.get("capability_bias", {}))
        capability_rows = [
            {
                "capability": self._normalize_mission_capability(str(name)),
                "bias": round(self._coerce_float(row.get("bias", 0.0), minimum=-1.0, maximum=1.0, default=0.0), 6),
                "pressure_ema": round(self._coerce_float(row.get("pressure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6),
                "samples": int(self._coerce_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0)),
            }
            for name, row in capability_bias.items()
            if isinstance(row, dict)
        ]
        capability_rows.sort(
            key=lambda row: (
                abs(float(row.get("bias", 0.0) or 0.0)),
                float(row.get("pressure_ema", 0.0) or 0.0),
                int(row.get("samples", 0) or 0),
            ),
            reverse=True,
        )
        provider_bias_rows: List[Dict[str, Any]] = []
        for row in provider_rows:
            provider = self._normalize_provider(str(row.get("provider", "")).strip())
            if not provider:
                continue
            provider_bias_rows.append(
                {
                    "provider": provider,
                    "outage_policy_bias": round(
                        self._coerce_float(
                            row.get("outage_policy_bias", 0.0),
                            minimum=self.mission_outage_bias_min,
                            maximum=self.mission_outage_bias_max,
                            default=0.0,
                        ),
                        6,
                    ),
                    "cooldown_bias": round(
                        self._coerce_float(
                            row.get("cooldown_bias", 1.0),
                            minimum=self.cooldown_bias_min,
                            maximum=self.cooldown_bias_max,
                            default=1.0,
                        ),
                        6,
                    ),
                    "mission_pressure": round(
                        self._coerce_float(row.get("mission_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                }
            )
        provider_bias_rows.sort(
            key=lambda row: (
                float(row.get("mission_pressure", 0.0) or 0.0),
                abs(float(row.get("outage_policy_bias", 0.0) or 0.0)),
                float(row.get("cooldown_bias", 1.0) or 1.0),
            ),
            reverse=True,
        )
        latest_row = recent_rows[-1] if recent_rows else {}
        return {
            "history_count": len(rows),
            "recent_count": len(recent_rows),
            "latest_mode": str(latest_row.get("mission_mode", mission_policy.get("mode", "stable"))).strip().lower(),
            "latest_profile": self._normalize_mission_outage_profile(
                str(latest_row.get("mission_profile", mission_policy.get("profile", "balanced")))
            ),
            "recent_profiles": [
                self._normalize_mission_outage_profile(str(row.get("mission_profile", mission_policy.get("profile", "balanced"))))
                for row in recent_rows[-6:]
            ],
            "recent_modes": [str(row.get("mission_mode", "")).strip().lower() for row in recent_rows[-6:]],
            "recent_pressure_series": [
                round(self._coerce_float(row.get("trend_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6)
                for row in recent_rows[-10:]
            ],
            "recent_volatility_series": [
                round(self._coerce_float(row.get("volatility_index", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6)
                for row in recent_rows[-10:]
            ],
            "recent_at_risk_series": [
                round(self._coerce_float(row.get("at_risk_ratio", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6)
                for row in recent_rows[-10:]
            ],
            "diagnostics": diagnostics,
            "top_provider_biases": provider_bias_rows[:4],
            "top_capability_biases": capability_rows[:4],
        }

    def _mission_policy_validation_decision_trace(
        self,
        *,
        warnings: List[Dict[str, Any]],
        history_context: Dict[str, Any],
        metrics: Dict[str, Any],
        recommended_preset_id: str,
    ) -> Dict[str, Any]:
        diagnostics_raw = history_context.get("diagnostics", {})
        diagnostics = diagnostics_raw if isinstance(diagnostics_raw, dict) else {}
        drift_mode = str(diagnostics.get("mode", "stable")).strip().lower() or "stable"
        drift_score = self._coerce_float(diagnostics.get("drift_score", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        switch_pressure = self._coerce_float(
            diagnostics.get("switch_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0
        )
        warning_codes = [
            str(row.get("code", "")).strip().lower()
            for row in warnings
            if isinstance(row, dict) and str(row.get("code", "")).strip()
        ]
        trigger_codes = list(dict.fromkeys(
            warning_codes
            + ([f"history_{drift_mode}"] if drift_mode and drift_mode != "stable" else [])
            + (["history_profile_switching"] if switch_pressure >= 0.16 else [])
        ))
        trip_recover_gap = self._coerce_float(
            metrics.get("trip_recover_gap", 0.0), minimum=0.0, maximum=1.0, default=0.0
        )
        route_preflight_gap = self._coerce_float(
            metrics.get("route_preflight_gap", 0.0), minimum=0.0, maximum=1.0, default=0.0
        )
        severity_score = self._coerce_float(
            min(
                1.0,
                (drift_score * 0.36)
                + (switch_pressure * 0.18)
                + (min(1.0, float(len(warning_codes)) / 4.0) * 0.24)
                + (0.12 if trip_recover_gap < 0.08 else 0.0)
                + (0.1 if route_preflight_gap < 0.05 else 0.0),
            ),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        trigger_band = "monitor"
        if severity_score >= 0.68:
            trigger_band = "critical"
        elif severity_score >= 0.42:
            trigger_band = "elevated"
        explanation = (
            f"Recommended preset '{recommended_preset_id or 'balanced_adaptive'}' because drift is '{drift_mode}' "
            f"(score {drift_score:.3f}), switch pressure is {switch_pressure:.3f}, and {len(warning_codes)} warning triggers are active."
        )
        evidence = [
            {"label": "drift_mode", "value": drift_mode},
            {"label": "drift_score", "value": round(drift_score, 6)},
            {"label": "switch_pressure", "value": round(switch_pressure, 6)},
            {"label": "trip_recover_gap", "value": round(trip_recover_gap, 6)},
            {"label": "route_preflight_gap", "value": round(route_preflight_gap, 6)},
            {"label": "warning_count", "value": len(warning_codes)},
        ]
        return {
            "recommended_preset_id": recommended_preset_id,
            "trigger_codes": trigger_codes[:10],
            "trigger_band": trigger_band,
            "severity_score": round(severity_score, 6),
            "summary": explanation,
            "evidence": evidence,
            "history_alignment": {
                "latest_mode": str(history_context.get("latest_mode", "stable")).strip().lower(),
                "latest_profile": str(history_context.get("latest_profile", "balanced")).strip().lower(),
                "recent_profiles": list(history_context.get("recent_profiles", []))[:6]
                if isinstance(history_context.get("recent_profiles", []), list)
                else [],
            },
        }

    def _mission_policy_config_presets(
        self,
        *,
        current: Dict[str, Any],
        metrics: Dict[str, Any],
        warning_codes: List[str],
    ) -> List[Dict[str, Any]]:
        trip_threshold = float(metrics.get("trip_threshold", 0.62) or 0.62)
        recover_threshold = float(metrics.get("recover_threshold", 0.36) or 0.36)
        route_block_threshold = float(metrics.get("route_block_threshold", 0.86) or 0.86)
        preflight_block_threshold = float(metrics.get("preflight_block_threshold", 0.92) or 0.92)
        bias_gain = float(metrics.get("bias_gain", 0.48) or 0.48)
        bias_decay = float(metrics.get("bias_decay", 0.8) or 0.8)
        profile_decay = float(metrics.get("profile_decay", 0.78) or 0.78)
        profile_stability_decay = float(metrics.get("profile_stability_decay", 0.82) or 0.82)
        capability_bias_gain = float(metrics.get("capability_bias_gain", 0.24) or 0.24)
        capability_bias_decay = float(metrics.get("capability_bias_decay", 0.84) or 0.84)
        capability_limit = int(metrics.get("capability_limit", 12) or 12)
        provider_max = int(metrics.get("provider_policy_max_providers", 80) or 80)
        warning_set = {str(code).strip().lower() for code in warning_codes if str(code).strip()}

        stability_changes = {
            "mission_outage_bias_gain": round(min(bias_gain, 0.72), 6),
            "mission_outage_bias_decay": round(max(bias_decay, 0.82), 6),
            "mission_outage_profile_decay": round(max(profile_decay, 0.84), 6),
            "mission_outage_profile_stability_decay": round(max(profile_stability_decay, 0.88), 6),
            "mission_outage_profile_hysteresis": round(
                max(float(current.get("mission_outage_profile_hysteresis", 0.09) or 0.09), 0.1),
                6,
            ),
            "outage_recover_threshold": round(min(recover_threshold, max(0.05, trip_threshold - 0.08)), 6),
            "outage_preflight_block_threshold": round(max(preflight_block_threshold, min(1.0, route_block_threshold + 0.05)), 6),
            "provider_policy_max_providers": int(max(provider_max, 10)),
        }
        balanced_changes = {
            "mission_outage_bias_gain": round(min(max(bias_gain, 0.46), 0.68), 6),
            "mission_outage_bias_decay": round(min(max(bias_decay, 0.78), 0.88), 6),
            "mission_outage_quality_relief": round(
                min(max(float(current.get("mission_outage_quality_relief", 0.18) or 0.18), 0.18), 0.28),
                6,
            ),
            "mission_outage_profile_decay": round(min(max(profile_decay, 0.76), 0.86), 6),
            "mission_outage_profile_stability_decay": round(min(max(profile_stability_decay, 0.82), 0.9), 6),
            "mission_outage_capability_bias_gain": round(min(max(capability_bias_gain, 0.22), 0.34), 6),
            "mission_outage_capability_bias_decay": round(min(max(capability_bias_decay, 0.84), 0.9), 6),
            "mission_outage_capability_limit": int(max(capability_limit, 12)),
            "provider_policy_max_providers": int(max(provider_max, 12)),
            "outage_preflight_block_threshold": round(max(preflight_block_threshold, min(1.0, route_block_threshold + 0.04)), 6),
        }
        capability_changes = {
            "mission_outage_capability_bias_gain": round(max(capability_bias_gain, 0.34), 6),
            "mission_outage_capability_bias_decay": round(max(capability_bias_decay, 0.88), 6),
            "mission_outage_capability_limit": int(max(capability_limit, 18)),
            "provider_policy_max_providers": int(max(provider_max, 14)),
            "mission_outage_profile_hysteresis": round(
                max(float(current.get("mission_outage_profile_hysteresis", 0.09) or 0.09), 0.08),
                6,
            ),
            "mission_outage_profile_decay": round(max(profile_decay, 0.78), 6),
        }

        recommended = "balanced_adaptive"
        if warning_set.intersection(
            {
                "preflight_threshold_aligned_to_route_threshold",
                "recover_threshold_close_to_trip_threshold",
                "aggressive_bias_feedback_loop",
                "profile_decay_too_reactive",
            }
        ):
            recommended = "stability_guard"
        elif warning_set.intersection({"provider_candidate_pool_narrow", "capability_bias_scope_too_narrow"}):
            recommended = "capability_expansion"

        presets = [
            {
                "id": "stability_guard",
                "label": "Stability Guard",
                "priority": 1,
                "recommended": recommended == "stability_guard",
                "confidence": 0.89 if recommended == "stability_guard" else 0.72,
                "reason": "Increase hysteresis and recovery headroom to reduce oscillation under degraded missions.",
                "changes": stability_changes,
            },
            {
                "id": "balanced_adaptive",
                "label": "Balanced Adaptive",
                "priority": 2,
                "recommended": recommended == "balanced_adaptive",
                "confidence": 0.78 if recommended == "balanced_adaptive" else 0.7,
                "reason": "Normalize mission feedback tuning while preserving adaptive provider and capability routing.",
                "changes": balanced_changes,
            },
            {
                "id": "capability_expansion",
                "label": "Capability Expansion",
                "priority": 3,
                "recommended": recommended == "capability_expansion",
                "confidence": 0.82 if recommended == "capability_expansion" else 0.66,
                "reason": "Expand capability-memory scope so provider routing does not overfit to a narrow action class set.",
                "changes": capability_changes,
            },
        ]
        return [
            {
                **row,
                "changes": {
                    str(key): value
                    for key, value in (row.get("changes", {}) if isinstance(row.get("changes"), dict) else {}).items()
                },
            }
            for row in presets
        ]

    def reset_mission_policy(
        self,
        *,
        reset_history: bool = False,
        reset_provider_biases: bool = False,
    ) -> Dict[str, Any]:
        cleared_history = 0
        provider_biases_reset = 0
        operation_bias_entries_reset = 0
        previous_policy = {}
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._lock:
            previous_policy = dict(self._mission_outage_policy)
            self._mission_outage_policy = self._mission_outage_policy_default_state()
            self._mission_outage_policy["updated_at"] = now_iso
            self._mission_outage_policy["last_reason"] = "reset"
            if bool(reset_history):
                cleared_history = len(self._mission_analysis_history)
                self._mission_analysis_history = []
            if bool(reset_provider_biases):
                for provider, row in list(self._provider_states.items()):
                    if not isinstance(row, dict):
                        continue
                    next_row = dict(row)
                    operation_bias_map = (
                        next_row.get("operation_cooldown_bias", {})
                        if isinstance(next_row.get("operation_cooldown_bias"), dict)
                        else {}
                    )
                    if operation_bias_map:
                        operation_bias_entries_reset += len(operation_bias_map)
                    next_row["outage_policy_bias"] = 0.0
                    next_row["cooldown_bias"] = 1.0
                    next_row["operation_cooldown_bias"] = {}
                    next_row["updated_at"] = now_iso
                    self._provider_states[provider] = next_row
                    provider_biases_reset += 1
            self._updates_since_save += 1
            self._maybe_save_locked(force=True)

        return {
            "status": "success",
            "reset_history": bool(reset_history),
            "reset_provider_biases": bool(reset_provider_biases),
            "cleared_history": int(cleared_history),
            "provider_biases_reset": int(provider_biases_reset),
            "operation_bias_entries_reset": int(operation_bias_entries_reset),
            "previous_profile": self._normalize_mission_outage_profile(str(previous_policy.get("profile", "balanced"))),
            "previous_mode": str(previous_policy.get("mode", "stable")).strip().lower() or "stable",
            "policy": {
                "mode": str(self._mission_outage_policy.get("mode", "stable")).strip().lower(),
                "profile": self._normalize_mission_outage_profile(str(self._mission_outage_policy.get("profile", "balanced"))),
                "bias": round(
                    self._coerce_float(
                        self._mission_outage_policy.get("bias", 0.0),
                        minimum=self.mission_outage_bias_min,
                        maximum=self.mission_outage_bias_max,
                        default=0.0,
                    ),
                    6,
                ),
                "updated_at": str(self._mission_outage_policy.get("updated_at", "")).strip(),
                "last_reason": str(self._mission_outage_policy.get("last_reason", "")).strip(),
            },
        }

    def tune_from_operational_signals(
        self,
        *,
        autonomy_report: Dict[str, Any] | None = None,
        mission_summary: Dict[str, Any] | None = None,
        dry_run: bool = False,
        reason: str = "manual",
    ) -> Dict[str, Any]:
        report = autonomy_report if isinstance(autonomy_report, dict) else {}
        missions = mission_summary if isinstance(mission_summary, dict) else {}
        trend = missions.get("trend", {}) if isinstance(missions.get("trend", {}), dict) else {}
        pressures = report.get("pressures", {}) if isinstance(report.get("pressures", {}), dict) else {}
        scores = report.get("scores", {}) if isinstance(report.get("scores", {}), dict) else {}
        risk_payload = missions.get("risk", {}) if isinstance(missions.get("risk", {}), dict) else {}
        quality_payload = missions.get("quality", {}) if isinstance(missions.get("quality", {}), dict) else {}
        recommendation = str(missions.get("recommendation", "")).strip().lower()
        action_hotspots_raw = report.get("action_hotspots", [])
        action_hotspots = [dict(row) for row in action_hotspots_raw if isinstance(row, dict)] if isinstance(action_hotspots_raw, list) else []

        trend_pressure = self._coerce_float(trend.get("pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        failed_ratio = self._coerce_float(missions.get("failed_ratio", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        blocked_ratio = self._coerce_float(missions.get("blocked_ratio", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        failure_pressure = self._coerce_float(pressures.get("failure_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        breaker_pressure = self._coerce_float(
            pressures.get("open_breaker_pressure", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        reliability = self._coerce_float(scores.get("reliability", 0.0), minimum=0.0, maximum=100.0, default=0.0) / 100.0
        mission_risk = self._coerce_float(risk_payload.get("avg_score", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        mission_quality = self._coerce_float(
            quality_payload.get("avg_score", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )

        mode = str(trend.get("mode", "stable")).strip().lower() or "stable"
        target_pressure = (
            (trend_pressure * 0.34)
            + (failed_ratio * 0.24)
            + (blocked_ratio * 0.12)
            + (failure_pressure * 0.18)
            + (breaker_pressure * 0.12)
        )
        target_pressure = self._coerce_float(target_pressure, minimum=0.0, maximum=1.0, default=0.0)
        base_target_bias = self._coerce_float(
            (target_pressure * self.mission_outage_bias_gain) + ((mission_risk - 0.4) * 0.18),
            minimum=self.mission_outage_bias_min,
            maximum=self.mission_outage_bias_max,
            default=0.0,
        )
        capability_targets = self._mission_capability_targets(
            action_hotspots=action_hotspots,
            target_pressure=target_pressure,
            mission_risk=mission_risk,
            mission_quality=mission_quality,
            reliability=reliability,
            mode=mode,
        )

        with self._lock:
            current = dict(self._mission_outage_policy)
            current_bias = self._coerce_float(
                current.get("bias", 0.0),
                minimum=self.mission_outage_bias_min,
                maximum=self.mission_outage_bias_max,
                default=0.0,
            )
            pressure_ema = self._coerce_float(current.get("pressure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            risk_ema = self._coerce_float(current.get("risk_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            quality_ema = self._coerce_float(current.get("quality_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            failed_ratio_ema = self._coerce_float(
                current.get("failed_ratio_ema", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            blocked_ratio_ema = self._coerce_float(
                current.get("blocked_ratio_ema", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            current_profile = self._normalize_mission_outage_profile(str(current.get("profile", "balanced")))
            profile_confidence = self._coerce_float(
                current.get("profile_confidence", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            profile_pressure_ema = self._coerce_float(
                current.get("profile_pressure_ema", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            profile_stability_ema = self._coerce_float(
                current.get("profile_stability_ema", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            profile_switch_count = self._coerce_int(
                current.get("profile_switch_count", 0),
                minimum=0,
                maximum=1_000_000,
                default=0,
            )
            profile_last_switch_at = str(current.get("profile_last_switch_at", "")).strip()
            profile_last_reason = str(current.get("profile_last_reason", "")).strip()
            profile_history_raw = current.get("profile_history", [])
            profile_history = [dict(row) for row in profile_history_raw if isinstance(row, dict)] if isinstance(profile_history_raw, list) else []
            if len(profile_history) > self.mission_outage_profile_history_limit:
                profile_history = profile_history[-self.mission_outage_profile_history_limit :]
            current_capability_bias = self._load_mission_capability_bias(current.get("capability_bias", {}))

            recent_profile_rows = profile_history[-8:] if isinstance(profile_history, list) else []
            recent_switches = 0
            previous_profile = ""
            for row in recent_profile_rows:
                if not isinstance(row, dict):
                    continue
                profile_name = self._normalize_mission_outage_profile(str(row.get("profile", "")))
                if not profile_name:
                    continue
                if previous_profile and previous_profile != profile_name:
                    recent_switches += 1
                previous_profile = profile_name
            volatility_index = self._coerce_float(
                (
                    abs(target_pressure - pressure_ema) * 0.26
                    + abs(failed_ratio - failed_ratio_ema) * 0.2
                    + abs(blocked_ratio - blocked_ratio_ema) * 0.14
                    + abs(mission_risk - risk_ema) * 0.18
                    + abs(mission_quality - quality_ema) * 0.12
                    + min(1.0, float(recent_switches) / 4.0) * 0.1
                ),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            volatility_mode = "stable"
            if volatility_index >= 0.68:
                volatility_mode = "surging"
            elif volatility_index >= 0.44:
                volatility_mode = "elevated"
            elif volatility_index <= 0.16 and mode in {"improving", "stable"}:
                volatility_mode = "calm"

            adaptive_bias_gain = self._coerce_float(
                float(self.mission_outage_bias_gain) * (1.0 + (volatility_index * (0.58 if mode == "worsening" else 0.34))),
                minimum=0.12,
                maximum=1.95,
                default=float(self.mission_outage_bias_gain),
            )
            adaptive_quality_relief = self._coerce_float(
                float(self.mission_outage_quality_relief)
                * (
                    1.0
                    + (volatility_index * (0.22 if mode == "improving" else -0.18))
                ),
                minimum=0.04,
                maximum=1.2,
                default=float(self.mission_outage_quality_relief),
            )
            effective_target_bias = base_target_bias
            effective_target_bias += (target_pressure * adaptive_bias_gain) - (target_pressure * float(self.mission_outage_bias_gain))
            effective_target_bias -= max(0.0, mission_quality - 0.6) * adaptive_quality_relief
            effective_target_bias -= max(0.0, reliability - 0.76) * (0.14 + (volatility_index * 0.04))
            if recommendation == "stability":
                effective_target_bias += 0.08 + (volatility_index * 0.05)
            elif recommendation == "throughput":
                effective_target_bias -= 0.08 + (volatility_index * 0.04)
            if mode in {"worsening"}:
                effective_target_bias += 0.06 + (volatility_index * 0.07)
            elif mode in {"improving"}:
                effective_target_bias -= 0.05 + (volatility_index * 0.04)
            effective_target_bias = self._coerce_float(
                effective_target_bias,
                minimum=self.mission_outage_bias_min,
                maximum=self.mission_outage_bias_max,
                default=base_target_bias,
            )
            adaptive_bias_decay = self._coerce_float(
                float(self.mission_outage_bias_decay)
                - (
                    volatility_index
                    * (
                        0.22
                        if mode == "worsening"
                        else 0.12
                    )
                )
                + (
                    0.04
                    if volatility_mode == "calm"
                    else 0.0
                ),
                minimum=0.42,
                maximum=0.98,
                default=float(self.mission_outage_bias_decay),
            )
            next_bias = self._coerce_float(
                (current_bias * adaptive_bias_decay) + (effective_target_bias * (1.0 - adaptive_bias_decay)),
                minimum=self.mission_outage_bias_min,
                maximum=self.mission_outage_bias_max,
                default=current_bias,
            )
            next_pressure_ema = self._coerce_float((pressure_ema * 0.76) + (target_pressure * 0.24), minimum=0.0, maximum=1.0, default=pressure_ema)
            next_risk_ema = self._coerce_float((risk_ema * 0.8) + (mission_risk * 0.2), minimum=0.0, maximum=1.0, default=risk_ema)
            next_quality_ema = self._coerce_float((quality_ema * 0.8) + (mission_quality * 0.2), minimum=0.0, maximum=1.0, default=quality_ema)
            next_failed_ratio_ema = self._coerce_float((failed_ratio_ema * 0.78) + (failed_ratio * 0.22), minimum=0.0, maximum=1.0, default=failed_ratio_ema)
            next_blocked_ratio_ema = self._coerce_float((blocked_ratio_ema * 0.78) + (blocked_ratio * 0.22), minimum=0.0, maximum=1.0, default=blocked_ratio_ema)
            next_capability_bias: Dict[str, Dict[str, Any]] = {}
            capability_keys = sorted(set(current_capability_bias.keys()) | set(capability_targets.keys()))
            for capability in capability_keys:
                if len(next_capability_bias) >= self.mission_outage_capability_limit:
                    break
                current_capability = current_capability_bias.get(capability, {})
                current_capability_row = current_capability if isinstance(current_capability, dict) else {}
                target_capability = capability_targets.get(capability, {})
                target_capability_row = target_capability if isinstance(target_capability, dict) else {}
                target_capability_pressure = self._coerce_float(
                    target_capability_row.get("target_pressure", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                target_capability_bias = self._coerce_float(
                    target_capability_row.get("target_bias", 0.0),
                    minimum=self.mission_outage_bias_min,
                    maximum=self.mission_outage_bias_max,
                    default=0.0,
                )
                current_capability_bias_value = self._coerce_float(
                    current_capability_row.get("bias", 0.0),
                    minimum=self.mission_outage_bias_min,
                    maximum=self.mission_outage_bias_max,
                    default=0.0,
                )
                current_capability_pressure = self._coerce_float(
                    current_capability_row.get("pressure_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                current_capability_samples = self._coerce_int(
                    current_capability_row.get("samples", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                )
                target_weight = self._coerce_float(
                    target_capability_row.get("weight", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                effective_weight = min(
                    1.0,
                    max(
                        0.08 if current_capability_samples > 0 else 0.0,
                        target_weight,
                    ),
                )
                blend = 1.0 - (
                    self.mission_outage_capability_bias_decay
                    + ((1.0 - self.mission_outage_capability_bias_decay) * (1.0 - effective_weight))
                )
                blend = self._coerce_float(blend, minimum=0.01, maximum=0.45, default=0.12)
                next_capability_bias_value = self._coerce_float(
                    (current_capability_bias_value * (1.0 - blend)) + (target_capability_bias * blend),
                    minimum=self.mission_outage_bias_min,
                    maximum=self.mission_outage_bias_max,
                    default=current_capability_bias_value,
                )
                pressure_blend = self._coerce_float(0.14 + (effective_weight * 0.2), minimum=0.05, maximum=0.42, default=0.2)
                next_capability_pressure = self._coerce_float(
                    (current_capability_pressure * (1.0 - pressure_blend)) + (target_capability_pressure * pressure_blend),
                    minimum=0.0,
                    maximum=1.0,
                    default=current_capability_pressure,
                )
                samples_increment = 1 if (target_weight > 0.0 or target_capability_pressure > 0.0) else 0
                next_capability_samples = current_capability_samples + samples_increment
                if (
                    abs(next_capability_bias_value) < 0.015
                    and next_capability_pressure < 0.08
                    and next_capability_samples <= 1
                ):
                    continue
                next_capability_bias[capability] = {
                    "bias": round(next_capability_bias_value, 6),
                    "pressure_ema": round(next_capability_pressure, 6),
                    "samples": int(next_capability_samples),
                    "weight": round(effective_weight, 6),
                    "top_action": str(target_capability_row.get("top_action", "")).strip().lower(),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            effective_recommendation = recommendation
            if volatility_mode in {"surging", "elevated"} and mode in {"worsening", "stable"}:
                effective_recommendation = "stability"
            elif volatility_mode == "calm" and mode == "improving" and recommendation == "stability":
                effective_recommendation = "throughput"
            profile_projection = self._project_mission_outage_profile(
                current_profile=current_profile,
                current_confidence=profile_confidence,
                current_pressure_ema=profile_pressure_ema,
                current_stability_ema=profile_stability_ema,
                target_pressure=target_pressure,
                mission_risk=mission_risk,
                mission_quality=mission_quality,
                reliability=reliability,
                failed_ratio=failed_ratio,
                blocked_ratio=blocked_ratio,
                recommendation=effective_recommendation,
                mode=mode,
            )
            next_profile = self._normalize_mission_outage_profile(str(profile_projection.get("profile", current_profile)))
            next_profile_confidence = self._coerce_float(
                profile_projection.get("confidence", profile_confidence),
                minimum=0.0,
                maximum=1.0,
                default=profile_confidence,
            )
            next_profile_pressure_ema = self._coerce_float(
                profile_projection.get("pressure_ema", profile_pressure_ema),
                minimum=0.0,
                maximum=1.0,
                default=profile_pressure_ema,
            )
            next_profile_stability_ema = self._coerce_float(
                profile_projection.get("stability_ema", profile_stability_ema),
                minimum=0.0,
                maximum=1.0,
                default=profile_stability_ema,
            )
            profile_reason = str(profile_projection.get("reason", "")).strip() or mode
            profile_changed = bool(next_profile != current_profile)
            now_iso = datetime.now(timezone.utc).isoformat()
            if profile_changed:
                profile_switch_count += 1
                profile_last_switch_at = now_iso
                profile_last_reason = profile_reason

            current_capability_compact = self._serialize_mission_capability_bias(current_capability_bias)
            next_capability_compact = self._serialize_mission_capability_bias(next_capability_bias)
            capability_changed = bool(current_capability_compact != next_capability_compact)
            changed = (
                round(next_bias, 6) != round(current_bias, 6)
                or round(next_pressure_ema, 6) != round(pressure_ema, 6)
                or round(next_risk_ema, 6) != round(risk_ema, 6)
                or round(next_quality_ema, 6) != round(quality_ema, 6)
                or round(next_failed_ratio_ema, 6) != round(failed_ratio_ema, 6)
                or round(next_blocked_ratio_ema, 6) != round(blocked_ratio_ema, 6)
                or str(current.get("mode", "stable")).strip().lower() != mode
                or next_profile != current_profile
                or round(next_profile_confidence, 6) != round(profile_confidence, 6)
                or capability_changed
            )

            profile_history.append(
                {
                    "at": now_iso,
                    "profile": next_profile,
                    "mode": mode,
                    "volatility_index": round(volatility_index, 6),
                    "volatility_mode": volatility_mode,
                    "target_pressure": round(target_pressure, 6),
                    "risk": round(mission_risk, 6),
                    "quality": round(mission_quality, 6),
                    "reliability": round(reliability, 6),
                    "failed_ratio": round(failed_ratio, 6),
                    "blocked_ratio": round(blocked_ratio, 6),
                    "recommendation": effective_recommendation,
                    "reason": profile_reason,
                }
            )
            if len(profile_history) > self.mission_outage_profile_history_limit:
                profile_history = profile_history[-self.mission_outage_profile_history_limit :]

            projected = {
                "bias": round(next_bias, 6),
                "pressure_ema": round(next_pressure_ema, 6),
                "risk_ema": round(next_risk_ema, 6),
                "quality_ema": round(next_quality_ema, 6),
                "failed_ratio_ema": round(next_failed_ratio_ema, 6),
                "blocked_ratio_ema": round(next_blocked_ratio_ema, 6),
                "mode": mode,
                "profile": next_profile,
                "profile_confidence": round(next_profile_confidence, 6),
                "profile_pressure_ema": round(next_profile_pressure_ema, 6),
                "profile_stability_ema": round(next_profile_stability_ema, 6),
                "profile_switch_count": int(profile_switch_count),
                "profile_last_switch_at": profile_last_switch_at,
                "profile_last_reason": profile_last_reason,
                "profile_history": profile_history,
                "capability_bias": next_capability_compact,
                "updated_at": now_iso,
                "last_reason": str(reason or "").strip() or "manual",
            }
            if changed and not dry_run and self.mission_outage_autotune_enabled:
                self._mission_outage_policy = dict(projected)
                self._updates_since_save += 1
                self._maybe_save_locked(force=False)

            applied = dict(self._mission_outage_policy if (changed and not dry_run and self.mission_outage_autotune_enabled) else projected)

        return {
            "status": "success",
            "changed": bool(changed and self.mission_outage_autotune_enabled),
            "dry_run": bool(dry_run),
            "enabled": bool(self.mission_outage_autotune_enabled),
            "reason": str(reason or "").strip() or "manual",
            "mode": mode,
            "targets": {
                "target_pressure": round(target_pressure, 6),
                "target_bias": round(effective_target_bias, 6),
                "base_target_bias": round(base_target_bias, 6),
                "adaptive_bias_gain": round(adaptive_bias_gain, 6),
                "adaptive_bias_decay": round(adaptive_bias_decay, 6),
                "adaptive_quality_relief": round(adaptive_quality_relief, 6),
                "volatility_index": round(volatility_index, 6),
                "volatility_mode": volatility_mode,
                "recommendation": effective_recommendation,
            },
            "profile": next_profile,
            "profile_changed": bool(profile_changed and self.mission_outage_profile_autotune_enabled),
            "profile_reason": profile_reason,
            "capability_targets": capability_targets,
            "state": applied,
        }

    def record_mission_analysis(
        self,
        *,
        analysis: Dict[str, Any] | None = None,
        reason: str = "analysis",
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        payload = analysis if isinstance(analysis, dict) else {}
        profile_analysis_raw = payload.get("profile_history_analysis", {})
        profile_analysis = profile_analysis_raw if isinstance(profile_analysis_raw, dict) else {}
        provider_analysis_raw = payload.get("provider_risk_analysis", {})
        provider_analysis = provider_analysis_raw if isinstance(provider_analysis_raw, dict) else {}
        trend_raw = payload.get("trend", {})
        trend = trend_raw if isinstance(trend_raw, dict) else {}
        mission_policy_raw = payload.get("mission_policy", {})
        mission_policy = mission_policy_raw if isinstance(mission_policy_raw, dict) else {}
        recommendation_rows = payload.get("recommendations", [])
        recommendations = [dict(row) for row in recommendation_rows if isinstance(row, dict)] if isinstance(recommendation_rows, list) else []
        top_at_risk_raw = provider_analysis.get("top_at_risk", [])
        top_at_risk = [dict(row) for row in top_at_risk_raw if isinstance(row, dict)] if isinstance(top_at_risk_raw, list) else []

        generated_at = str(payload.get("generated_at", "")).strip() or datetime.now(timezone.utc).isoformat()
        volatility_index = self._coerce_float(
            profile_analysis.get("volatility_index", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        at_risk_ratio = self._coerce_float(
            provider_analysis.get("at_risk_ratio", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        trend_pressure = self._coerce_float(
            trend.get("trend_pressure", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        pressure_score = self._coerce_float(
            (volatility_index * 0.42) + (at_risk_ratio * 0.35) + (trend_pressure * 0.23),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        top_provider_risk = []
        for row in top_at_risk[:12]:
            provider = self._normalize_provider(str(row.get("provider", "")).strip())
            if not provider:
                continue
            risk = self._coerce_float(
                (
                    (1.0 - self._coerce_float(row.get("health_score", 0.5), minimum=0.0, maximum=1.0, default=0.5)) * 0.34
                    + (self._coerce_float(row.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0) * 0.34)
                    + (self._coerce_float(row.get("outage_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0) * 0.22)
                    + (0.1 if bool(row.get("cooldown_active", False) or row.get("outage_active", False)) else 0.0)
                ),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            top_provider_risk.append({"provider": provider, "risk": round(risk, 6)})

        recommendation_types: List[str] = []
        for row in recommendations[:8]:
            recommendation_type = str(row.get("type", "")).strip().lower()
            if recommendation_type and recommendation_type not in recommendation_types:
                recommendation_types.append(recommendation_type)

        row = {
            "at": generated_at,
            "reason": str(reason or "").strip() or "analysis",
            "provider_count": self._coerce_int(payload.get("provider_count", 0), minimum=0, maximum=1_000_000, default=0),
            "at_risk_count": self._coerce_int(provider_analysis.get("at_risk_count", 0), minimum=0, maximum=1_000_000, default=0),
            "at_risk_ratio": round(at_risk_ratio, 6),
            "trend_pressure": round(trend_pressure, 6),
            "trend_mode": str(trend.get("mode", "")).strip().lower(),
            "volatility_index": round(volatility_index, 6),
            "volatility_mode": str(profile_analysis.get("volatility_mode", "")).strip().lower(),
            "pressure_score": round(pressure_score, 6),
            "mission_mode": str(mission_policy.get("mode", "")).strip().lower(),
            "mission_profile": self._normalize_mission_outage_profile(str(mission_policy.get("profile", "balanced"))),
            "mission_bias": round(
                self._coerce_float(
                    mission_policy.get("bias", 0.0),
                    minimum=self.mission_outage_bias_min,
                    maximum=self.mission_outage_bias_max,
                    default=0.0,
                ),
                6,
            ),
            "recommendation_types": recommendation_types[:6],
            "top_provider_risk": top_provider_risk[:6],
        }
        signature = self._mission_analysis_signature(row)
        now_ts = time.time()

        with self._lock:
            history = [dict(item) for item in self._mission_analysis_history if isinstance(item, dict)]
            last = dict(history[-1]) if history else {}
            should_record = True
            delta_score = 1.0
            elapsed_s = 0.0
            if last:
                last_ts = self._to_timestamp(last.get("at", ""))
                elapsed_s = max(0.0, now_ts - last_ts) if last_ts > 0 else 0.0
                previous_signature = self._mission_analysis_signature(last)
                delta_score = self._coerce_float(
                    max(
                        abs(
                            self._coerce_float(last.get("volatility_index", 0.0), minimum=0.0, maximum=1.0, default=0.0)
                            - volatility_index
                        ),
                        abs(
                            self._coerce_float(last.get("at_risk_ratio", 0.0), minimum=0.0, maximum=1.0, default=0.0)
                            - at_risk_ratio
                        ),
                        abs(
                            self._coerce_float(last.get("trend_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0)
                            - trend_pressure
                        ),
                        abs(
                            self._coerce_float(last.get("pressure_score", 0.0), minimum=0.0, maximum=1.0, default=0.0)
                            - pressure_score
                        ),
                    ),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                min_interval = self._coerce_float(
                    self.mission_analysis_record_min_interval_s,
                    minimum=5.0,
                    maximum=3600.0,
                    default=45.0,
                )
                change_floor = self._coerce_float(
                    self.mission_analysis_record_change_threshold,
                    minimum=0.0,
                    maximum=1.0,
                    default=0.05,
                )
                same_signature = previous_signature == signature
                significant_count_change = abs(
                    self._coerce_int(last.get("at_risk_count", 0), minimum=0, maximum=1_000_000, default=0)
                    - self._coerce_int(row.get("at_risk_count", 0), minimum=0, maximum=1_000_000, default=0)
                ) >= 2
                should_record = bool(
                    elapsed_s >= min_interval
                    and (
                        not same_signature
                        or delta_score >= change_floor
                        or significant_count_change
                    )
                )
            if should_record:
                history.append(row)
                if len(history) > self.mission_analysis_history_limit:
                    history = history[-self.mission_analysis_history_limit :]
                if not dry_run:
                    self._mission_analysis_history = history
                    self._updates_since_save += 1
                    self._maybe_save_locked(force=False)
            drift = self._mission_analysis_drift_diagnostics(
                rows=history if should_record else (history + [row]),
                window=min(120, max(8, int(len(history) // 2) if history else 24)),
            )

        return {
            "status": "success",
            "recorded": bool(should_record and not dry_run),
            "dry_run": bool(dry_run),
            "reason": str(reason or "").strip() or "analysis",
            "delta_score": round(delta_score, 6),
            "elapsed_s": round(elapsed_s, 3),
            "record": row,
            "drift": drift,
        }

    def mission_analysis_history(self, *, limit: int = 120, window: int = 24) -> Dict[str, Any]:
        bounded_limit = self._coerce_int(limit, minimum=1, maximum=5000, default=120)
        bounded_window = self._coerce_int(window, minimum=4, maximum=1200, default=24)
        with self._lock:
            rows = [dict(item) for item in self._mission_analysis_history if isinstance(item, dict)]
        if len(rows) > bounded_limit:
            items = rows[-bounded_limit:]
        else:
            items = rows
        diagnostics = self._mission_analysis_drift_diagnostics(rows=rows, window=bounded_window)
        return {
            "status": "success",
            "count": len(items),
            "total": len(rows),
            "limit": bounded_limit,
            "window": bounded_window,
            "diagnostics": diagnostics,
            "items": items,
        }

    def tune_provider_policy_from_mission_analysis(
        self,
        *,
        analysis: Dict[str, Any] | None = None,
        dry_run: bool = False,
        reason: str = "mission_analysis",
    ) -> Dict[str, Any]:
        if not bool(self.mission_provider_policy_autotune_enabled):
            return {
                "status": "success",
                "changed": False,
                "enabled": False,
                "dry_run": bool(dry_run),
                "reason": str(reason or "").strip() or "mission_analysis",
                "updated": [],
            }

        payload = analysis if isinstance(analysis, dict) else {}
        profile_analysis_raw = payload.get("profile_history_analysis", {})
        profile_analysis = profile_analysis_raw if isinstance(profile_analysis_raw, dict) else {}
        provider_analysis_raw = payload.get("provider_risk_analysis", {})
        provider_analysis = provider_analysis_raw if isinstance(provider_analysis_raw, dict) else {}
        mission_policy_raw = payload.get("mission_policy", {})
        mission_policy = mission_policy_raw if isinstance(mission_policy_raw, dict) else {}
        trend_raw = payload.get("trend", {})
        trend = trend_raw if isinstance(trend_raw, dict) else {}
        top_at_risk_raw = provider_analysis.get("top_at_risk", [])
        top_at_risk = [dict(row) for row in top_at_risk_raw if isinstance(row, dict)] if isinstance(top_at_risk_raw, list) else []

        volatility_index = self._coerce_float(
            profile_analysis.get("volatility_index", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        at_risk_ratio = self._coerce_float(
            provider_analysis.get("at_risk_ratio", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        trend_pressure = self._coerce_float(
            trend.get("trend_pressure", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        mission_mode = str(mission_policy.get("mode", "")).strip().lower()
        mission_profile = self._normalize_mission_outage_profile(str(mission_policy.get("profile", "balanced")))
        mission_bias = self._coerce_float(
            mission_policy.get("bias", 0.0),
            minimum=self.mission_outage_bias_min,
            maximum=self.mission_outage_bias_max,
            default=0.0,
        )
        pressure_score = self._coerce_float(
            (volatility_index * 0.38) + (at_risk_ratio * 0.34) + (trend_pressure * 0.28),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        risk_map: Dict[str, float] = {}
        for row in top_at_risk[:24]:
            provider = self._normalize_provider(str(row.get("provider", "")).strip())
            if not provider:
                continue
            provider_risk = self._coerce_float(
                (
                    (1.0 - self._coerce_float(row.get("health_score", 0.5), minimum=0.0, maximum=1.0, default=0.5)) * 0.36
                    + (self._coerce_float(row.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0) * 0.34)
                    + (self._coerce_float(row.get("outage_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0) * 0.18)
                    + (0.12 if bool(row.get("cooldown_active", False) or row.get("outage_active", False)) else 0.0)
                ),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            risk_map[provider] = max(risk_map.get(provider, 0.0), provider_risk)

        with self._lock:
            provider_rows = [dict(row) for row in self._provider_states.values() if isinstance(row, dict)]
            provider_rows.sort(
                key=lambda row: (
                    -self._coerce_float(row.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                    -self._coerce_float(row.get("outage_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                    -self._coerce_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                ),
            )
            provider_rows = provider_rows[: self.mission_provider_policy_max_providers]
            updates: List[Dict[str, Any]] = []
            changed = False
            for row in provider_rows:
                provider = self._normalize_provider(str(row.get("provider", "")).strip())
                if not provider:
                    continue
                current_state = dict(self._provider_states.get(provider, {}))
                failure_ema = self._coerce_float(current_state.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
                outage_ema = self._coerce_float(current_state.get("outage_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
                failure_trend = self._coerce_float(current_state.get("failure_trend_ema", 0.0), minimum=-1.0, maximum=1.0, default=0.0)
                availability_ema = self._coerce_float(current_state.get("availability_ema", 0.55), minimum=0.0, maximum=1.0, default=0.55)
                outage_active = self._coerce_bool(current_state.get("outage_active", False), default=False)
                baseline_risk = self._coerce_float(
                    ((failure_ema * 0.42) + (outage_ema * 0.28) + ((1.0 - availability_ema) * 0.22) + (max(0.0, failure_trend) * 0.08)),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                provider_risk = max(
                    baseline_risk,
                    self._coerce_float(risk_map.get(provider, 0.0), minimum=0.0, maximum=1.0, default=0.0),
                )
                current_outage_bias = self._coerce_float(
                    current_state.get("outage_policy_bias", 0.0),
                    minimum=self.outage_policy_bias_min,
                    maximum=self.outage_policy_bias_max,
                    default=0.0,
                )
                current_cooldown_bias = self._coerce_float(
                    current_state.get("cooldown_bias", 1.0),
                    minimum=self.cooldown_bias_min,
                    maximum=self.cooldown_bias_max,
                    default=1.0,
                )
                profile_multiplier = {
                    "defensive": 1.18,
                    "cautious": 1.08,
                    "balanced": 1.0,
                    "throughput": 0.92,
                }.get(mission_profile, 1.0)
                target_outage_bias = self._coerce_float(
                    ((provider_risk - 0.46) * 0.74 * profile_multiplier)
                    + (mission_bias * 0.46)
                    + (pressure_score * 0.18)
                    + (0.08 if outage_active else 0.0)
                    + (0.06 if mission_mode == "worsening" else (-0.05 if mission_mode == "improving" else 0.0)),
                    minimum=self.outage_policy_bias_min,
                    maximum=self.outage_policy_bias_max,
                    default=current_outage_bias,
                )
                blend = self._coerce_float(
                    0.18 + (volatility_index * 0.24) + (0.08 if outage_active else 0.0),
                    minimum=0.08,
                    maximum=0.62,
                    default=0.24,
                )
                next_outage_bias = self._coerce_float(
                    (current_outage_bias * (1.0 - blend)) + (target_outage_bias * blend),
                    minimum=self.outage_policy_bias_min,
                    maximum=self.outage_policy_bias_max,
                    default=current_outage_bias,
                )
                target_cooldown_bias = self._coerce_float(
                    1.0
                    + (max(0.0, next_outage_bias) * 0.92)
                    + (provider_risk * 0.28)
                    - (max(0.0, 0.62 - provider_risk) * 0.22),
                    minimum=self.cooldown_bias_min,
                    maximum=self.cooldown_bias_max,
                    default=current_cooldown_bias,
                )
                cooldown_blend = self._coerce_float(
                    0.14 + (pressure_score * 0.22),
                    minimum=0.08,
                    maximum=0.5,
                    default=0.22,
                )
                next_cooldown_bias = self._coerce_float(
                    (current_cooldown_bias * (1.0 - cooldown_blend)) + (target_cooldown_bias * cooldown_blend),
                    minimum=self.cooldown_bias_min,
                    maximum=self.cooldown_bias_max,
                    default=current_cooldown_bias,
                )

                operation_bias_map = current_state.get("operation_cooldown_bias", {})
                operation_bias = (
                    dict(operation_bias_map)
                    if isinstance(operation_bias_map, dict)
                    else {}
                )
                op_updates: Dict[str, float] = {}
                for operation in {"read", "write", "mutate", "auth", "maintenance", "healthcheck"}:
                    current_op_bias = self._coerce_float(
                        operation_bias.get(operation, 1.0),
                        minimum=self.cooldown_bias_min,
                        maximum=self.cooldown_bias_max,
                        default=1.0,
                    )
                    op_risk_gain = 0.0
                    if operation in {"write", "mutate", "auth"}:
                        op_risk_gain = (provider_risk * 0.26) + (pressure_score * 0.12)
                    elif operation == "read":
                        op_risk_gain = (provider_risk * 0.14) + (pressure_score * 0.08)
                    elif operation == "healthcheck":
                        op_risk_gain = max(0.0, provider_risk - 0.45) * 0.08
                    elif operation == "maintenance":
                        op_risk_gain = (provider_risk * 0.18) + (0.08 if outage_active else 0.0)
                    target_op_bias = self._coerce_float(
                        1.0 + op_risk_gain + (0.08 if mission_mode == "worsening" else 0.0),
                        minimum=self.cooldown_bias_min,
                        maximum=self.cooldown_bias_max,
                        default=current_op_bias,
                    )
                    next_op_bias = self._coerce_float(
                        (current_op_bias * 0.76) + (target_op_bias * 0.24),
                        minimum=self.cooldown_bias_min,
                        maximum=self.cooldown_bias_max,
                        default=current_op_bias,
                    )
                    op_updates[operation] = round(next_op_bias, 6)
                state_changed = bool(
                    abs(next_outage_bias - current_outage_bias) >= 0.012
                    or abs(next_cooldown_bias - current_cooldown_bias) >= 0.012
                    or any(
                        abs(
                            self._coerce_float(operation_bias.get(op_name, 1.0), minimum=self.cooldown_bias_min, maximum=self.cooldown_bias_max, default=1.0)
                            - op_value
                        )
                        >= 0.02
                        for op_name, op_value in op_updates.items()
                    )
                )
                if not state_changed:
                    continue
                updates.append(
                    {
                        "provider": provider,
                        "risk": round(provider_risk, 6),
                        "outage_policy_bias": round(next_outage_bias, 6),
                        "cooldown_bias": round(next_cooldown_bias, 6),
                        "operation_cooldown_bias": dict(op_updates),
                    }
                )
                if dry_run:
                    continue
                current_state["outage_policy_bias"] = round(next_outage_bias, 6)
                current_state["cooldown_bias"] = round(next_cooldown_bias, 6)
                current_state["operation_cooldown_bias"] = dict(op_updates)
                current_state["updated_at"] = datetime.now(timezone.utc).isoformat()
                self._provider_states[provider] = current_state
                changed = True
            if changed and not dry_run:
                self._updates_since_save += 1
                self._trim_locked()
                self._maybe_save_locked(force=False)

        updates.sort(
            key=lambda row: (
                -self._coerce_float(row.get("risk", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                str(row.get("provider", "")),
            )
        )
        return {
            "status": "success",
            "enabled": bool(self.mission_provider_policy_autotune_enabled),
            "dry_run": bool(dry_run),
            "reason": str(reason or "").strip() or "mission_analysis",
            "changed": bool(changed and not dry_run),
            "updated_count": len(updates),
            "updates": updates[:24],
            "mission_profile": mission_profile,
            "mission_mode": mission_mode,
            "volatility_index": round(volatility_index, 6),
            "at_risk_ratio": round(at_risk_ratio, 6),
            "trend_pressure": round(trend_pressure, 6),
        }

    def _load_mission_analysis_history(self, raw: Any) -> List[Dict[str, Any]]:
        rows = raw if isinstance(raw, list) else []
        history: List[Dict[str, Any]] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            top_provider_rows = item.get("top_provider_risk", [])
            top_provider_risk = []
            if isinstance(top_provider_rows, list):
                for provider_row in top_provider_rows[:8]:
                    if not isinstance(provider_row, dict):
                        continue
                    provider = self._normalize_provider(str(provider_row.get("provider", "")).strip())
                    if not provider:
                        continue
                    top_provider_risk.append(
                        {
                            "provider": provider,
                            "risk": round(
                                self._coerce_float(provider_row.get("risk", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                                6,
                            ),
                        }
                    )
            recommendation_types_raw = item.get("recommendation_types", [])
            recommendation_types = []
            if isinstance(recommendation_types_raw, list):
                for recommendation_type in recommendation_types_raw[:8]:
                    clean = str(recommendation_type or "").strip().lower()
                    if clean and clean not in recommendation_types:
                        recommendation_types.append(clean)
            history.append(
                {
                    "at": str(item.get("at", "")).strip(),
                    "reason": str(item.get("reason", "")).strip() or "analysis",
                    "provider_count": self._coerce_int(item.get("provider_count", 0), minimum=0, maximum=1_000_000, default=0),
                    "at_risk_count": self._coerce_int(item.get("at_risk_count", 0), minimum=0, maximum=1_000_000, default=0),
                    "at_risk_ratio": round(self._coerce_float(item.get("at_risk_ratio", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6),
                    "trend_pressure": round(self._coerce_float(item.get("trend_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6),
                    "trend_mode": str(item.get("trend_mode", "")).strip().lower(),
                    "volatility_index": round(self._coerce_float(item.get("volatility_index", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6),
                    "volatility_mode": str(item.get("volatility_mode", "")).strip().lower(),
                    "pressure_score": round(self._coerce_float(item.get("pressure_score", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6),
                    "mission_mode": str(item.get("mission_mode", "")).strip().lower(),
                    "mission_profile": self._normalize_mission_outage_profile(str(item.get("mission_profile", "balanced"))),
                    "mission_bias": round(
                        self._coerce_float(
                            item.get("mission_bias", 0.0),
                            minimum=self.mission_outage_bias_min,
                            maximum=self.mission_outage_bias_max,
                            default=0.0,
                        ),
                        6,
                    ),
                    "recommendation_types": recommendation_types,
                    "top_provider_risk": top_provider_risk,
                }
            )
        if len(history) <= self.mission_analysis_history_limit:
            return history
        return history[-self.mission_analysis_history_limit :]

    @staticmethod
    def _mission_analysis_signature(row: Dict[str, Any]) -> str:
        payload = row if isinstance(row, dict) else {}
        top_provider_rows = payload.get("top_provider_risk", [])
        top_provider_risk = []
        if isinstance(top_provider_rows, list):
            for item in top_provider_rows[:4]:
                if not isinstance(item, dict):
                    continue
                provider = str(item.get("provider", "")).strip().lower()
                if not provider:
                    continue
                risk = round(float(item.get("risk", 0.0) or 0.0), 3)
                top_provider_risk.append({"provider": provider, "risk": risk})
        signature_payload = {
            "mission_mode": str(payload.get("mission_mode", "")).strip().lower(),
            "mission_profile": str(payload.get("mission_profile", "")).strip().lower(),
            "volatility_mode": str(payload.get("volatility_mode", "")).strip().lower(),
            "trend_mode": str(payload.get("trend_mode", "")).strip().lower(),
            "at_risk_count": int(payload.get("at_risk_count", 0) or 0),
            "top_provider_risk": top_provider_risk,
            "recommendation_types": [
                str(item).strip().lower()
                for item in (payload.get("recommendation_types", []) if isinstance(payload.get("recommendation_types", []), list) else [])[:4]
                if str(item).strip()
            ],
        }
        digest = hashlib.sha1(  # noqa: S324
            json.dumps(signature_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return digest[:24]

    def _mission_analysis_drift_diagnostics(self, *, rows: List[Dict[str, Any]], window: int) -> Dict[str, Any]:
        history = [dict(row) for row in rows if isinstance(row, dict)]
        if not history:
            return {
                "status": "success",
                "count": 0,
                "mode": "insufficient",
                "drift_score": 0.0,
                "message": "No mission analysis history available.",
            }
        bounded_window = self._coerce_int(window, minimum=4, maximum=1200, default=24)
        recent = history[-bounded_window:]
        baseline = history[-(bounded_window * 2) : -bounded_window] if len(history) > bounded_window else []
        if not baseline and len(history) > 2:
            baseline = history[: max(1, len(history) // 2)]

        def _avg(items: List[Dict[str, Any]], key: str) -> float:
            if not items:
                return 0.0
            return self._coerce_float(
                sum(self._coerce_float(item.get(key, 0.0), minimum=0.0, maximum=1.0, default=0.0) for item in items)
                / float(len(items)),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )

        recent_volatility = _avg(recent, "volatility_index")
        recent_at_risk_ratio = _avg(recent, "at_risk_ratio")
        recent_trend_pressure = _avg(recent, "trend_pressure")
        recent_pressure_score = _avg(recent, "pressure_score")
        baseline_volatility = _avg(baseline, "volatility_index")
        baseline_at_risk_ratio = _avg(baseline, "at_risk_ratio")
        baseline_trend_pressure = _avg(baseline, "trend_pressure")
        baseline_pressure_score = _avg(baseline, "pressure_score")

        mode_switches = 0
        profile_switches = 0
        previous_mode = ""
        previous_profile = ""
        for row in recent:
            mode = str(row.get("mission_mode", "")).strip().lower()
            profile = str(row.get("mission_profile", "")).strip().lower()
            if previous_mode and mode and previous_mode != mode:
                mode_switches += 1
            if previous_profile and profile and previous_profile != profile:
                profile_switches += 1
            previous_mode = mode or previous_mode
            previous_profile = profile or previous_profile
        switch_pressure = self._coerce_float(
            float(mode_switches + profile_switches) / max(1.0, float(len(recent))),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        drift_score = self._coerce_float(
            max(0.0, recent_pressure_score - baseline_pressure_score) * 0.36
            + max(0.0, recent_volatility - baseline_volatility) * 0.22
            + max(0.0, recent_at_risk_ratio - baseline_at_risk_ratio) * 0.22
            + max(0.0, recent_trend_pressure - baseline_trend_pressure) * 0.2
            + (switch_pressure * 0.12),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        mode = "stable"
        if drift_score >= 0.58:
            mode = "worsening"
        elif drift_score <= 0.14 and recent_pressure_score < 0.3:
            mode = "improving"

        return {
            "status": "success",
            "count": len(history),
            "window": bounded_window,
            "mode": mode,
            "drift_score": round(drift_score, 6),
            "switch_pressure": round(switch_pressure, 6),
            "mode_switches": int(mode_switches),
            "profile_switches": int(profile_switches),
            "recent": {
                "volatility_index": round(recent_volatility, 6),
                "at_risk_ratio": round(recent_at_risk_ratio, 6),
                "trend_pressure": round(recent_trend_pressure, 6),
                "pressure_score": round(recent_pressure_score, 6),
            },
            "baseline": {
                "volatility_index": round(baseline_volatility, 6),
                "at_risk_ratio": round(baseline_at_risk_ratio, 6),
                "trend_pressure": round(baseline_trend_pressure, 6),
                "pressure_score": round(baseline_pressure_score, 6),
            },
            "delta": {
                "volatility_index": round(recent_volatility - baseline_volatility, 6),
                "at_risk_ratio": round(recent_at_risk_ratio - baseline_at_risk_ratio, 6),
                "trend_pressure": round(recent_trend_pressure - baseline_trend_pressure, 6),
                "pressure_score": round(recent_pressure_score - baseline_pressure_score, 6),
            },
        }

    @classmethod
    def _normalize_mission_outage_profile(cls, value: str) -> str:
        clean = str(value or "").strip().lower()
        if clean in {"defensive", "cautious", "balanced", "throughput"}:
            return clean
        return "balanced"

    @classmethod
    def _normalize_mission_capability(cls, value: str) -> str:
        clean = str(value or "").strip().lower()
        aliases = {
            "doc": "document",
            "docs": "document",
            "drive": "document",
            "files": "document",
            "mail": "email",
            "gmail": "email",
            "calendar_event": "calendar",
            "todo": "task",
            "tasks": "task",
            "token": "auth",
            "oauth": "auth",
            "integration": "external",
            "connector": "external",
        }
        resolved = aliases.get(clean, clean)
        if resolved in {"email", "calendar", "document", "task", "auth", "external"}:
            return resolved
        return "general"

    def _load_mission_capability_bias(self, raw: Any) -> Dict[str, Dict[str, Any]]:
        payload = raw if isinstance(raw, dict) else {}
        out: Dict[str, Dict[str, Any]] = {}
        for capability, value in payload.items():
            name = self._normalize_mission_capability(str(capability))
            if name == "general" and str(capability or "").strip().lower() != "general":
                continue
            row = value if isinstance(value, dict) else {}
            out[name] = {
                "bias": round(
                    self._coerce_float(
                        row.get("bias", 0.0),
                        minimum=self.mission_outage_bias_min,
                        maximum=self.mission_outage_bias_max,
                        default=0.0,
                    ),
                    6,
                ),
                "pressure_ema": round(
                    self._coerce_float(
                        row.get("pressure_ema", 0.0),
                        minimum=0.0,
                        maximum=1.0,
                        default=0.0,
                    ),
                    6,
                ),
                "samples": self._coerce_int(
                    row.get("samples", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                ),
                "weight": round(
                    self._coerce_float(
                        row.get("weight", 0.0),
                        minimum=0.0,
                        maximum=1.0,
                        default=0.0,
                    ),
                    6,
                ),
                "top_action": str(row.get("top_action", "")).strip().lower(),
                "updated_at": str(row.get("updated_at", "")).strip(),
            }
        if len(out) <= self.mission_outage_capability_limit:
            return out
        ordered = sorted(
            out.items(),
            key=lambda item: (
                abs(self._coerce_float(item[1].get("bias", 0.0), minimum=-1.0, maximum=1.0, default=0.0)),
                self._coerce_float(item[1].get("pressure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                self._coerce_int(item[1].get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                item[0],
            ),
            reverse=True,
        )
        return {name: dict(row) for name, row in ordered[: self.mission_outage_capability_limit]}

    def _serialize_mission_capability_bias(self, raw: Any) -> Dict[str, Dict[str, Any]]:
        rows = self._load_mission_capability_bias(raw)
        ordered = sorted(rows.items(), key=lambda item: item[0])
        return {name: dict(row) for name, row in ordered[: self.mission_outage_capability_limit]}

    def _mission_capability_targets(
        self,
        *,
        action_hotspots: List[Dict[str, Any]],
        target_pressure: float,
        mission_risk: float,
        mission_quality: float,
        reliability: float,
        mode: str,
    ) -> Dict[str, Dict[str, Any]]:
        rows = [row for row in action_hotspots if isinstance(row, dict)]
        if not rows:
            return {}
        buckets: Dict[str, Dict[str, Any]] = {}
        for row in rows[:80]:
            action = str(row.get("action", "")).strip().lower()
            if not action:
                continue
            capability = self._normalize_mission_capability(self._action_domain(action))
            if capability not in {"email", "calendar", "document", "task", "auth", "external"}:
                continue
            failures = self._coerce_int(row.get("failures", 0), minimum=0, maximum=10_000_000, default=0)
            runs = self._coerce_int(row.get("runs", 0), minimum=0, maximum=10_000_000, default=0)
            failure_rate = self._coerce_float(row.get("failure_rate", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            if runs <= 0 and failures <= 0 and failure_rate <= 0.0:
                continue
            severity = self._coerce_float(
                (failure_rate * 0.68)
                + (min(1.0, float(failures) / 10.0) * 0.22)
                + (min(1.0, float(runs) / 24.0) * 0.1),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            weight = self._coerce_float(
                min(1.0, 0.12 + (severity * 0.72) + (0.12 if failure_rate >= 0.5 else 0.0)),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            bucket = buckets.setdefault(
                capability,
                {
                    "weighted_pressure_sum": 0.0,
                    "weight_sum": 0.0,
                    "sample_count": 0,
                    "top_action": "",
                    "top_action_severity": -1.0,
                },
            )
            bucket["weighted_pressure_sum"] = float(bucket.get("weighted_pressure_sum", 0.0)) + (severity * weight)
            bucket["weight_sum"] = float(bucket.get("weight_sum", 0.0)) + weight
            bucket["sample_count"] = int(bucket.get("sample_count", 0) or 0) + 1
            if severity > float(bucket.get("top_action_severity", -1.0)):
                bucket["top_action_severity"] = severity
                bucket["top_action"] = action

        if not buckets:
            return {}

        mode_factor = 0.06 if str(mode).strip().lower() == "worsening" else (-0.04 if str(mode).strip().lower() == "improving" else 0.0)
        capability_targets: Dict[str, Dict[str, Any]] = {}
        for capability, bucket in buckets.items():
            weight_sum = self._coerce_float(bucket.get("weight_sum", 0.0), minimum=0.0, maximum=1000.0, default=0.0)
            weighted_pressure_sum = self._coerce_float(
                bucket.get("weighted_pressure_sum", 0.0),
                minimum=0.0,
                maximum=1000.0,
                default=0.0,
            )
            capability_pressure = self._coerce_float(
                weighted_pressure_sum / max(0.0001, weight_sum),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            capability_pressure = self._coerce_float(
                capability_pressure + (target_pressure * 0.16) + mode_factor,
                minimum=0.0,
                maximum=1.0,
                default=capability_pressure,
            )
            raw_bias = (capability_pressure * self.mission_outage_capability_bias_gain) + ((mission_risk - 0.42) * 0.14)
            raw_bias -= max(0.0, mission_quality - 0.62) * 0.08
            raw_bias -= max(0.0, reliability - 0.78) * 0.06
            target_bias = self._coerce_float(
                raw_bias,
                minimum=self.mission_outage_bias_min,
                maximum=self.mission_outage_bias_max,
                default=0.0,
            )
            capability_targets[capability] = {
                "target_pressure": round(capability_pressure, 6),
                "target_bias": round(target_bias, 6),
                "weight": round(self._coerce_float(weight_sum / max(1.0, float(bucket.get("sample_count", 1))), minimum=0.0, maximum=1.0, default=0.0), 6),
                "sample_count": self._coerce_int(bucket.get("sample_count", 0), minimum=0, maximum=10_000_000, default=0),
                "top_action": str(bucket.get("top_action", "")).strip().lower(),
            }
        ordered_targets = sorted(
            capability_targets.items(),
            key=lambda item: (
                abs(self._coerce_float(item[1].get("target_bias", 0.0), minimum=-1.0, maximum=1.0, default=0.0)),
                self._coerce_float(item[1].get("target_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                self._coerce_int(item[1].get("sample_count", 0), minimum=0, maximum=10_000_000, default=0),
                item[0],
            ),
            reverse=True,
        )
        return {
            capability: dict(payload)
            for capability, payload in ordered_targets[: self.mission_outage_capability_limit]
        }

    def _project_mission_outage_profile(
        self,
        *,
        current_profile: str,
        current_confidence: float,
        current_pressure_ema: float,
        current_stability_ema: float,
        target_pressure: float,
        mission_risk: float,
        mission_quality: float,
        reliability: float,
        failed_ratio: float,
        blocked_ratio: float,
        recommendation: str,
        mode: str,
    ) -> Dict[str, Any]:
        active_profile = self._normalize_mission_outage_profile(current_profile)
        confidence = self._coerce_float(current_confidence, minimum=0.0, maximum=1.0, default=0.0)
        pressure_ema = self._coerce_float(current_pressure_ema, minimum=0.0, maximum=1.0, default=0.0)
        stability_ema = self._coerce_float(current_stability_ema, minimum=0.0, maximum=1.0, default=0.0)

        pressure = self._coerce_float(target_pressure, minimum=0.0, maximum=1.0, default=0.0)
        risk = self._coerce_float(mission_risk, minimum=0.0, maximum=1.0, default=0.0)
        quality = self._coerce_float(mission_quality, minimum=0.0, maximum=1.0, default=0.0)
        reliability_score = self._coerce_float(reliability, minimum=0.0, maximum=1.0, default=0.0)
        failed = self._coerce_float(failed_ratio, minimum=0.0, maximum=1.0, default=0.0)
        blocked = self._coerce_float(blocked_ratio, minimum=0.0, maximum=1.0, default=0.0)
        clean_mode = str(mode or "").strip().lower() or "stable"
        clean_recommendation = str(recommendation or "").strip().lower()

        danger_score = (
            (pressure * 0.42)
            + (risk * 0.2)
            + (failed * 0.16)
            + (blocked * 0.1)
            + ((1.0 - reliability_score) * 0.12)
        )
        danger_score = self._coerce_float(danger_score, minimum=0.0, maximum=1.0, default=0.0)
        throughput_score = (
            (reliability_score * 0.34)
            + (quality * 0.3)
            + ((1.0 - pressure) * 0.2)
            + ((1.0 - failed) * 0.1)
            + ((1.0 - blocked) * 0.06)
        )
        throughput_score = self._coerce_float(throughput_score, minimum=0.0, maximum=1.0, default=0.0)

        target_profile = "balanced"
        reason = "balanced_operating_window"
        if clean_mode == "worsening" and danger_score >= 0.58:
            target_profile = "defensive"
            reason = "mission_worsening_pressure"
        elif danger_score >= 0.72:
            target_profile = "defensive"
            reason = "high_danger_score"
        elif danger_score >= 0.54 or clean_recommendation == "stability":
            target_profile = "cautious"
            reason = "elevated_risk_or_stability_recommendation"
        elif throughput_score >= 0.74 and clean_mode in {"stable", "improving"} and clean_recommendation in {"", "throughput"}:
            target_profile = "throughput"
            reason = "sustained_stability_and_throughput_headroom"
        elif clean_mode == "improving" and danger_score <= 0.36 and throughput_score >= 0.66:
            target_profile = "throughput"
            reason = "improving_trend_with_low_danger"

        pressure_ema = self._coerce_float(
            (pressure_ema * self.mission_outage_profile_decay) + (pressure * (1.0 - self.mission_outage_profile_decay)),
            minimum=0.0,
            maximum=1.0,
            default=pressure_ema,
        )
        stability_signal = self._coerce_float(
            (throughput_score * 0.6) + ((1.0 - danger_score) * 0.4),
            minimum=0.0,
            maximum=1.0,
            default=0.5,
        )
        stability_ema = self._coerce_float(
            (stability_ema * self.mission_outage_profile_stability_decay)
            + (stability_signal * (1.0 - self.mission_outage_profile_stability_decay)),
            minimum=0.0,
            maximum=1.0,
            default=stability_ema,
        )

        if target_profile == active_profile:
            confidence = self._coerce_float((confidence * 0.72) + 0.28, minimum=0.0, maximum=1.0, default=confidence)
            return {
                "profile": active_profile,
                "confidence": confidence,
                "pressure_ema": pressure_ema,
                "stability_ema": stability_ema,
                "reason": reason,
            }

        if not self.mission_outage_profile_autotune_enabled:
            confidence = self._coerce_float(confidence * 0.9, minimum=0.0, maximum=1.0, default=confidence)
            return {
                "profile": active_profile,
                "confidence": confidence,
                "pressure_ema": pressure_ema,
                "stability_ema": stability_ema,
                "reason": "profile_autotune_disabled",
            }

        switch_pressure = abs(danger_score - throughput_score)
        if target_profile in {"defensive", "cautious"}:
            switch_pressure = max(switch_pressure, danger_score - 0.5)
        elif target_profile == "throughput":
            switch_pressure = max(switch_pressure, throughput_score - 0.56)
        switch_pressure = self._coerce_float(switch_pressure, minimum=0.0, maximum=1.0, default=0.0)
        hysteresis = self._coerce_float(
            self.mission_outage_profile_hysteresis,
            minimum=0.01,
            maximum=0.5,
            default=0.09,
        )

        if switch_pressure < hysteresis:
            confidence = self._coerce_float(confidence * 0.88, minimum=0.0, maximum=1.0, default=confidence)
            return {
                "profile": active_profile,
                "confidence": confidence,
                "pressure_ema": pressure_ema,
                "stability_ema": stability_ema,
                "reason": f"hysteresis_hold:{reason}",
            }

        confidence = self._coerce_float(
            (confidence * 0.58) + (switch_pressure * 0.42),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        return {
            "profile": target_profile,
            "confidence": confidence,
            "pressure_ema": pressure_ema,
            "stability_ema": stability_ema,
            "reason": reason,
        }

    @staticmethod
    def _build_remediation_contract(
        *,
        hints: List[Dict[str, Any]],
        diagnostics: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        rows = hints if isinstance(hints, list) else []
        strategies: List[Dict[str, Any]] = []
        tool_action_count = 0
        args_patch_count = 0
        confidence_values: List[float] = []
        automated_strategy_count = 0
        manual_strategy_count = 0
        for index, hint in enumerate(rows):
            if not isinstance(hint, dict):
                continue
            hint_id = str(hint.get("id", "")).strip().lower() or f"strategy_{index + 1}"
            confidence = hint.get("confidence", 0.0)
            try:
                confidence_value = float(confidence)
            except Exception:
                confidence_value = 0.0
            bounded_confidence = max(0.0, min(1.0, confidence_value))
            strategy: Dict[str, Any] = {
                "id": hint_id,
                "priority": int(hint.get("priority", index + 1) or (index + 1)),
                "confidence": bounded_confidence,
            }
            confidence_values.append(float(bounded_confidence))
            args_patch = hint.get("args_patch")
            if isinstance(args_patch, dict) and args_patch:
                strategy["type"] = "args_patch"
                strategy["args_patch"] = dict(args_patch)
                args_patch_count += 1
                automated_strategy_count += 1
            tool_action = hint.get("tool_action")
            if isinstance(tool_action, dict) and tool_action:
                strategy["type"] = "tool_action"
                strategy["tool_action"] = dict(tool_action)
                tool_action_count += 1
                automated_strategy_count += 1
            if "type" not in strategy:
                strategy["type"] = "advice"
                manual_strategy_count += 1
            strategies.append(strategy)
        strategies.sort(
            key=lambda item: (
                int(item.get("priority", 0)),
                -float(item.get("confidence", 0.0)),
                str(item.get("id", "")),
            )
        )
        confidence_avg = 0.0
        confidence_max = 0.0
        confidence_min = 0.0
        if confidence_values:
            confidence_avg = sum(confidence_values) / float(len(confidence_values))
            confidence_max = max(confidence_values)
            confidence_min = min(confidence_values)
        confidence_p90 = 0.0
        if confidence_values:
            ordered_confidence = sorted(confidence_values)
            confidence_p90 = ordered_confidence[min(len(ordered_confidence) - 1, int(max(0, round((len(ordered_confidence) - 1) * 0.9))))]
        diagnostics_payload = diagnostics if isinstance(diagnostics, dict) else {}
        failure_code = str(diagnostics_payload.get("failure_code", "")).strip().lower()
        domain = str(diagnostics_payload.get("domain", "")).strip().lower()
        blocking_class = "generic"
        if "auth" in failure_code or domain == "auth_preflight":
            blocking_class = "auth"
        elif "provider" in failure_code or domain in {"provider_contract", "runtime_reliability"}:
            blocking_class = "provider"
        elif "contract" in failure_code or domain == "connector_contract":
            blocking_class = "contract"
        elif "rate" in failure_code or "timeout" in failure_code or "outage" in failure_code:
            blocking_class = "reliability"
        automation_ready = bool(
            automated_strategy_count >= 1
            and confidence_avg >= 0.58
        )
        automation_tier = "manual"
        if automation_ready:
            automation_tier = "automated"
        elif automated_strategy_count >= 1:
            automation_tier = "assisted"
        estimated_recovery_s = 0
        for strategy in strategies:
            strategy_type = str(strategy.get("type", "")).strip().lower()
            if strategy_type == "tool_action":
                estimated_recovery_s += 45
            elif strategy_type == "args_patch":
                estimated_recovery_s += 8
            else:
                estimated_recovery_s += 20
        if blocking_class == "auth":
            estimated_recovery_s += 90
        elif blocking_class == "provider":
            estimated_recovery_s += 60
        elif blocking_class == "contract":
            estimated_recovery_s += 20
        if "outage" in failure_code:
            estimated_recovery_s += 120
        elif "cooldown" in failure_code:
            estimated_recovery_s += 45
        estimated_recovery_s = max(10, min(3600, int(estimated_recovery_s)))

        execution_phases: List[Dict[str, Any]] = [
            {
                "phase": "diagnose",
                "description": "Capture contract/provider/auth diagnostics snapshot before mutating payload.",
                "required": True,
            }
        ]
        if args_patch_count > 0:
            execution_phases.append(
                {
                    "phase": "normalize_args",
                    "description": "Apply deterministic args patches from highest-confidence strategies.",
                    "required": True,
                }
            )
        if tool_action_count > 0:
            execution_phases.append(
                {
                    "phase": "repair_dependencies",
                    "description": "Run tool-driven remediation for auth/provider readiness.",
                    "required": automation_tier in {"automated", "assisted"},
                }
            )
        execution_phases.append(
            {
                "phase": "retry_with_verification",
                "description": "Retry action with provider routing and verify success contract.",
                "required": True,
            }
        )
        payload = {
            "version": "1.0",
            "strategy_count": len(strategies),
            "tool_action_count": int(tool_action_count),
            "args_patch_count": int(args_patch_count),
            "advice_count": max(0, len(strategies) - tool_action_count - args_patch_count),
            "automated_strategy_count": int(automated_strategy_count),
            "manual_strategy_count": int(manual_strategy_count),
            "confidence_avg": round(confidence_avg, 6),
            "confidence_max": round(confidence_max, 6),
            "confidence_min": round(confidence_min, 6),
            "confidence_p90": round(confidence_p90, 6),
            "automation_ready": bool(automation_ready),
            "automation_tier": automation_tier,
            "blocking_class": blocking_class,
            "estimated_recovery_s": int(estimated_recovery_s),
            "strategy_order": [str(item.get("id", "")).strip().lower() for item in strategies[:12] if str(item.get("id", "")).strip()],
            "strategies": strategies[:10],
            "execution_contract": {
                "mode": automation_tier,
                "blocking_class": blocking_class,
                "estimated_recovery_s": int(estimated_recovery_s),
                "max_retry_attempts": 2 if automation_tier == "automated" else 1,
                "phases": execution_phases[:6],
                "verification": {
                    "expect_status": "success",
                    "enforce_contract_checks": True,
                    "allow_provider_reroute": True,
                },
                "stop_conditions": [
                    "non_retryable_contract_failure",
                    "auth_hard_block_without_refresh_path",
                    "risk_budget_exceeded",
                ],
            },
        }
        if isinstance(diagnostics, dict) and diagnostics:
            payload["diagnostics"] = dict(diagnostics)
        return payload

    @staticmethod
    def _diagnostic_id(*, stage: str, action: str, code: str, fingerprint: Dict[str, Any]) -> str:
        seed_payload = {
            "stage": str(stage or "").strip().lower(),
            "action": str(action or "").strip().lower(),
            "code": str(code or "").strip().lower(),
            "fingerprint": fingerprint if isinstance(fingerprint, dict) else {},
        }
        seed = json.dumps(seed_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(seed.encode("utf-8", errors="ignore")).hexdigest()[:16]
        return f"diag_{digest}"

    @staticmethod
    def _diagnostic_severity_score(
        *,
        severity: str,
        checks: List[Dict[str, Any]] | None = None,
        remediation_contract: Dict[str, Any] | None = None,
    ) -> float:
        severity_value = str(severity or "").strip().lower()
        base = {"info": 0.12, "warning": 0.48, "error": 0.78, "critical": 0.92}.get(severity_value, 0.5)
        check_rows = checks if isinstance(checks, list) else []
        error_count = 0
        warning_count = 0
        for row in check_rows:
            if not isinstance(row, dict):
                continue
            check_severity = str(row.get("severity", "")).strip().lower()
            check_status = str(row.get("status", "")).strip().lower()
            if check_severity == "error" or check_status == "failed":
                error_count += 1
            elif check_severity == "warning" or check_status == "warning":
                warning_count += 1
        value = base + min(0.16, float(error_count) * 0.04) + min(0.08, float(warning_count) * 0.02)
        contract = remediation_contract if isinstance(remediation_contract, dict) else {}
        automation_tier = str(contract.get("automation_tier", "")).strip().lower()
        confidence_avg = 0.0
        try:
            confidence_avg = float(contract.get("confidence_avg", 0.0) or 0.0)
        except Exception:
            confidence_avg = 0.0
        if automation_tier == "manual":
            value += 0.06
        elif automation_tier == "automated":
            value -= min(0.08, confidence_avg * 0.08)
        return max(0.0, min(1.0, value))

    def _structured_remediation_plan(
        self,
        *,
        stage: str,
        action: str,
        code: str,
        hints: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        rows = [row for row in hints if isinstance(row, dict)]
        plan: List[Dict[str, Any]] = [
            {
                "phase": "diagnose",
                "objective": "Validate connector contract and provider eligibility before retry.",
                "stage": str(stage or "").strip().lower(),
                "action": str(action or "").strip().lower(),
                "failure_code": str(code or "").strip().lower(),
            }
        ]
        arg_patch_hints = [row for row in rows if isinstance(row.get("args_patch"), dict)]
        tool_hints = [row for row in rows if isinstance(row.get("tool_action"), dict)]
        if arg_patch_hints:
            top_patch = arg_patch_hints[0]
            plan.append(
                {
                    "phase": "normalize_args",
                    "objective": "Apply highest-confidence argument patch to satisfy contract.",
                    "hint_id": str(top_patch.get("id", "")).strip().lower(),
                    "confidence": round(
                        self._coerce_float(
                            top_patch.get("confidence", 0.0),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        6,
                    ),
                    "args_patch": dict(top_patch.get("args_patch", {})),
                }
            )
        if tool_hints:
            top_tool = tool_hints[0]
            plan.append(
                {
                    "phase": "repair_dependency",
                    "objective": "Run repair action to restore external readiness (auth/provider/connector).",
                    "hint_id": str(top_tool.get("id", "")).strip().lower(),
                    "confidence": round(
                        self._coerce_float(
                            top_tool.get("confidence", 0.0),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        6,
                    ),
                    "tool_action": dict(top_tool.get("tool_action", {})),
                }
            )
        plan.append(
            {
                "phase": "retry",
                "objective": "Retry action after remediation and verify success contract.",
                "verification": {
                    "expect_status": "success",
                    "allow_provider_reroute": True,
                },
            }
        )
        return plan[:6]

    def _build_contract_diagnostic(self, *, action: str, payload: Dict[str, Any], message: str) -> Dict[str, Any]:
        clean_action = str(action or "").strip().lower()
        text = str(message or "").strip()
        lowered = text.lower()
        code = "contract_validation_failed"
        fields: List[str] = []
        any_of_groups: List[List[str]] = []
        if lowered.startswith("missing required fields for"):
            code = "missing_required_fields"
            marker = text.split(":", 1)
            if len(marker) > 1:
                fields = [str(item).strip() for item in marker[1].split(",") if str(item).strip()]
        elif "requires at least one of:" in lowered:
            code = "missing_any_of_fields"
            marker = text.split(":", 1)
            if len(marker) > 1:
                any_fields = [str(item).strip() for item in marker[1].split(",") if str(item).strip()]
                if any_fields:
                    any_of_groups = [any_fields]
        elif "must be" in lowered or "must contain" in lowered:
            code = "invalid_field_type_or_range"
            match = re.search(rf"{re.escape(clean_action)}\.([a-zA-Z0-9_]+)", lowered)
            if match:
                fields = [match.group(1)]
        elif "calendar_create_event requires start and end" in lowered:
            code = "missing_event_window"
            fields = ["start", "end"]
        elif "calendar_create_event requires end to be after start" in lowered:
            code = "invalid_event_window"
            fields = ["start", "end"]
        elif "task_update requires due to be" in lowered:
            code = "invalid_due_timestamp"
            fields = ["due"]

        remediation_hints: List[Dict[str, Any]] = []
        if code == "missing_required_fields" and fields:
            remediation_hints.append(
                {
                    "id": "provide_required_fields",
                    "priority": 1,
                    "confidence": 0.94,
                    "summary": "Provide all required fields before execution.",
                    "fields": fields,
                    "remediation": {
                        "type": "payload_completion",
                        "required_fields": fields,
                    },
                }
            )
        if code == "missing_any_of_fields" and any_of_groups:
            remediation_hints.append(
                {
                    "id": "provide_mutation_payload",
                    "priority": 2,
                    "confidence": 0.88,
                    "summary": "Provide at least one mutable field.",
                    "any_of": any_of_groups,
                    "remediation": {
                        "type": "payload_mutation_set",
                        "any_of": any_of_groups,
                    },
                }
            )
        if code in {"invalid_field_type_or_range", "invalid_event_window", "invalid_due_timestamp"}:
            remediation_hints.append(
                {
                    "id": "fix_field_format",
                    "priority": 2,
                    "confidence": 0.78,
                    "summary": "Normalize payload types/formats to connector contract.",
                    "fields": fields,
                    "remediation": {
                        "type": "payload_normalization",
                        "fields": fields,
                    },
                }
            )
        if code == "missing_event_window":
            remediation_hints.append(
                {
                    "id": "set_event_window",
                    "priority": 1,
                    "confidence": 0.86,
                    "summary": "Set both start and end as ISO-8601 datetimes.",
                    "fields": ["start", "end"],
                    "remediation": {
                        "type": "calendar_time_window",
                        "required_fields": ["start", "end"],
                    },
                }
            )

        diagnostics = {
            "domain": "connector_contract",
            "failure_code": code,
            "payload_key_count": len(payload.keys()),
            "missing_fields": fields,
            "missing_any_of_groups": any_of_groups,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        checks: List[Dict[str, Any]] = [
            {
                "check": "payload_schema",
                "status": "failed",
                "severity": "error",
                "details": {"failure_code": code},
            }
        ]
        if fields:
            checks.append(
                {
                    "check": "required_fields",
                    "status": "failed",
                    "severity": "error",
                    "details": {"fields": list(fields)},
                }
            )
        if any_of_groups:
            checks.append(
                {
                    "check": "any_of_fields",
                    "status": "failed",
                    "severity": "error",
                    "details": {"groups": [list(group) for group in any_of_groups[:6]]},
                }
            )
        diagnostic_id = self._diagnostic_id(
            stage="payload_contract",
            action=clean_action,
            code=code,
            fingerprint={
                "fields": fields[:10],
                "any_of": any_of_groups[:6],
                "payload_keys": sorted(str(key) for key in payload.keys())[:16],
            },
        )
        remediation_plan = self._structured_remediation_plan(
            stage="payload_contract",
            action=clean_action,
            code=code,
            hints=remediation_hints,
        )
        remediation_contract = self._build_remediation_contract(
            hints=remediation_hints,
            diagnostics=diagnostics,
        )
        severity_score = self._diagnostic_severity_score(
            severity="error",
            checks=checks,
            remediation_contract=remediation_contract,
        )

        return {
            "diagnostic_id": diagnostic_id,
            "code": code,
            "contract_stage": "payload_contract",
            "action": clean_action,
            "message": text,
            "fields": fields,
            "any_of": any_of_groups,
            "payload_keys": sorted(str(key) for key in payload.keys()),
            "severity": "error",
            "severity_score": round(severity_score, 6),
            "blocking_class": str(remediation_contract.get("blocking_class", "")).strip().lower(),
            "estimated_recovery_s": int(remediation_contract.get("estimated_recovery_s", 0) or 0),
            "automation_tier": str(remediation_contract.get("automation_tier", "")).strip().lower(),
            "diagnostics": diagnostics,
            "checks": checks,
            "remediation_hints": remediation_hints,
            "remediation_plan": remediation_plan,
            "remediation_contract": remediation_contract,
        }

    def _build_provider_contract_diagnostic(self, *, action: str, negotiation: Dict[str, Any]) -> Dict[str, Any]:
        clean_action = str(action or "").strip().lower()
        payload = negotiation if isinstance(negotiation, dict) else {}
        requested_provider = self._normalize_provider(str(payload.get("requested_provider", "")).strip())
        allowed_rows = payload.get("allowed_providers", [])
        allowed = [self._normalize_provider(str(item)) for item in (allowed_rows if isinstance(allowed_rows, list) else []) if self._normalize_provider(str(item))]
        dropped_rows = payload.get("dropped_providers", [])
        dropped = [dict(row) for row in dropped_rows if isinstance(row, dict)] if isinstance(dropped_rows, list) else []
        capability_code = str(payload.get("capability_contract_code", "")).strip().lower()
        capability_reason = str(payload.get("capability_contract_reason", "")).strip().lower()
        required_capability = self._normalize_mission_capability(
            str(payload.get("required_capability", self._action_domain(clean_action))).strip()
        )
        operation_class = str(payload.get("operation_class", self._operation_class(clean_action))).strip().lower()
        code = capability_code or "provider_contract_failed"
        if requested_provider and allowed and requested_provider not in allowed:
            code = "provider_not_supported_for_action"
        elif not payload.get("providers") and not capability_code:
            code = "no_provider_candidates_after_contract"
        remediation_hints: List[Dict[str, Any]] = []
        if code == "provider_not_supported_for_action" and allowed:
            remediation_hints.append(
                {
                    "id": "switch_provider",
                    "priority": 1,
                    "confidence": 0.94,
                    "summary": "Use a provider supported by this connector action.",
                    "allowed_providers": allowed,
                    "args_patch": {"provider": allowed[0]},
                    "remediation": {
                        "type": "provider_selection",
                        "preferred_provider": allowed[0],
                        "allowed_providers": allowed,
                    },
                }
            )
        if code == "no_provider_candidates_after_contract":
            remediation_hints.append(
                {
                    "id": "set_provider_auto",
                    "priority": 2,
                    "confidence": 0.78,
                    "summary": "Use provider=auto or relax payload constraints to allow compatible providers.",
                    "args_patch": {"provider": "auto"},
                    "remediation": {
                        "type": "provider_auto_negotiation",
                        "fallback_provider": "auto",
                    },
                }
            )
        if code in {
            "provider_capability_not_supported_for_action",
            "provider_runtime_capability_mismatch",
            "provider_runtime_capability_disabled",
            "provider_runtime_unavailable",
            "provider_runtime_operation_not_supported",
            "provider_runtime_action_not_allowed",
            "provider_runtime_action_blocked",
        }:
            if allowed:
                remediation_hints.append(
                    {
                        "id": "switch_provider_capability",
                        "priority": 1,
                        "confidence": 0.9,
                        "summary": (
                            f"Switch to a provider that supports capability '{required_capability}' "
                            f"for operation '{operation_class}'."
                        ),
                        "allowed_providers": allowed[:8],
                        "args_patch": {"provider": allowed[0]},
                        "remediation": {
                            "type": "provider_capability_selection",
                            "required_capability": required_capability,
                            "required_operation_class": operation_class,
                            "preferred_provider": allowed[0],
                            "allowed_providers": allowed[:8],
                        },
                    }
                )
            remediation_hints.append(
                {
                    "id": "refresh_provider_capability_contract",
                    "priority": 2,
                    "confidence": 0.76,
                    "summary": "Refresh provider capability registry and runtime availability before retry.",
                    "tool_action": {
                        "action": "external_connector_status",
                        "args": {"provider": requested_provider or "auto"},
                    },
                    "remediation": {
                        "type": "provider_capability_refresh",
                        "required_capability": required_capability,
                        "required_operation_class": operation_class,
                    },
                }
            )
        if dropped:
            remediation_hints.append(
                {
                    "id": "review_dropped_providers",
                    "priority": 3,
                    "confidence": 0.62,
                    "summary": "Inspect dropped providers and adjust provider/action pairing.",
                    "dropped_providers": dropped[:8],
                    "remediation": {
                        "type": "provider_contract_review",
                        "dropped_count": len(dropped),
                    },
                }
            )
        diagnostics = {
            "domain": "provider_contract",
            "failure_code": code,
            "requested_provider": requested_provider,
            "allowed_provider_count": len(allowed),
            "dropped_provider_count": len(dropped),
            "required_capability": required_capability,
            "required_operation_class": operation_class,
            "capability_contract_reason": capability_reason,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        checks: List[Dict[str, Any]] = [
            {
                "check": "provider_contract",
                "status": "failed",
                "severity": "error",
                "details": {
                    "requested_provider": requested_provider,
                    "allowed_provider_count": len(allowed),
                    "required_capability": required_capability,
                    "required_operation_class": operation_class,
                },
            }
        ]
        if dropped:
            checks.append(
                {
                    "check": "provider_drop_analysis",
                    "status": "warning",
                    "severity": "warning",
                    "details": {"dropped_count": len(dropped)},
                }
            )
        diagnostic_id = self._diagnostic_id(
            stage="provider_contract",
            action=clean_action,
            code=code,
            fingerprint={
                "requested_provider": requested_provider,
                "allowed_providers": allowed[:8],
                "dropped": [
                    {
                        "provider": str(row.get("provider", "")).strip().lower(),
                        "reason": str(row.get("reason", "")).strip().lower(),
                    }
                    for row in dropped[:8]
                    if isinstance(row, dict)
                ],
            },
        )
        remediation_plan = self._structured_remediation_plan(
            stage="provider_contract",
            action=clean_action,
            code=code,
            hints=remediation_hints,
        )
        remediation_contract = self._build_remediation_contract(
            hints=remediation_hints,
            diagnostics=diagnostics,
        )
        severity_score = self._diagnostic_severity_score(
            severity="error",
            checks=checks,
            remediation_contract=remediation_contract,
        )
        return {
            "diagnostic_id": diagnostic_id,
            "code": code,
            "contract_stage": "provider_contract",
            "action": clean_action,
            "message": str(payload.get("message", "")).strip(),
            "requested_provider": requested_provider,
            "allowed_providers": allowed,
            "dropped_providers": dropped,
            "required_capability": required_capability,
            "operation_class": operation_class,
            "capability_contract_reason": capability_reason,
            "severity": "error",
            "severity_score": round(severity_score, 6),
            "blocking_class": str(remediation_contract.get("blocking_class", "")).strip().lower(),
            "estimated_recovery_s": int(remediation_contract.get("estimated_recovery_s", 0) or 0),
            "automation_tier": str(remediation_contract.get("automation_tier", "")).strip().lower(),
            "diagnostics": diagnostics,
            "checks": checks,
            "remediation_hints": remediation_hints,
            "remediation_plan": remediation_plan,
            "remediation_contract": remediation_contract,
        }

    def _build_auth_contract_diagnostic(self, *, action: str, auth_preflight: Dict[str, Any]) -> Dict[str, Any]:
        clean_action = str(action or "").strip().lower()
        payload = auth_preflight if isinstance(auth_preflight, dict) else {}
        auth_rows_raw = payload.get("auth_rows", [])
        auth_rows = [dict(row) for row in auth_rows_raw if isinstance(row, dict)] if isinstance(auth_rows_raw, list) else []
        blocked_rows = [row for row in auth_rows if str(row.get("status", "")).strip().lower() in {"blocked", "degraded"}]
        missing_scope_rows = [row for row in blocked_rows if str(row.get("reason", "")).strip().lower() == "missing_required_scope"]
        expired_rows = [row for row in blocked_rows if str(row.get("reason", "")).strip().lower() in {"token_expired", "token_ttl_low"}]
        missing_credentials_rows = [row for row in blocked_rows if bool(row.get("credentials_missing", False))]
        remediation_hints: List[Dict[str, Any]] = []
        if missing_scope_rows:
            providers = [str(row.get("provider", "")).strip().lower() for row in missing_scope_rows if str(row.get("provider", "")).strip()]
            remediation_hints.append(
                {
                    "id": "reauthorize_with_scopes",
                    "priority": 1,
                    "confidence": 0.9,
                    "summary": "Reauthorize provider with required scopes for this action.",
                    "providers": providers,
                    "tool_action": {
                        "action": "oauth_token_maintain",
                        "args": {"provider": providers[0] if providers else "auto", "limit": 20, "window_s": 7200},
                    },
                    "remediation": {
                        "type": "oauth_scope_repair",
                        "providers": providers,
                    },
                }
            )
        if expired_rows:
            providers = [str(row.get("provider", "")).strip().lower() for row in expired_rows if str(row.get("provider", "")).strip()]
            remediation_hints.append(
                {
                    "id": "refresh_access_token",
                    "priority": 1,
                    "confidence": 0.92,
                    "summary": "Refresh or rotate expired provider tokens before retrying.",
                    "providers": providers,
                    "tool_action": {
                        "action": "oauth_token_refresh",
                        "args": {"provider": providers[0] if providers else "auto"},
                    },
                    "remediation": {
                        "type": "oauth_token_rotation",
                        "providers": providers,
                    },
                }
            )
        if missing_credentials_rows:
            providers = [
                str(row.get("provider", "")).strip().lower()
                for row in missing_credentials_rows
                if str(row.get("provider", "")).strip()
            ]
            remediation_hints.append(
                {
                    "id": "connect_provider_account",
                    "priority": 2,
                    "confidence": 0.74,
                    "summary": "Connect provider credentials in OAuth token store.",
                    "providers": providers,
                    "remediation": {
                        "type": "oauth_account_connect",
                        "providers": providers,
                    },
                }
            )
        if not remediation_hints:
            remediation_hints.append(
                {
                    "id": "run_auth_maintenance",
                    "priority": 2,
                    "confidence": 0.7,
                    "summary": "Run oauth_token_maintain and re-check external_auth_state.",
                    "tool_action": {
                        "action": "oauth_token_maintain",
                        "args": {"provider": "auto", "limit": 40, "window_s": 14400},
                    },
                    "remediation": {
                        "type": "oauth_maintenance_cycle",
                        "providers": ["auto"],
                    },
                }
            )
        diagnostics = {
            "domain": "auth_preflight",
            "failure_code": "auth_preflight_failed",
            "blocked_provider_count": len(blocked_rows),
            "missing_scope_count": len(missing_scope_rows),
            "expired_count": len(expired_rows),
            "missing_credentials_count": len(missing_credentials_rows),
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        checks: List[Dict[str, Any]] = [
            {
                "check": "auth_credentials",
                "status": "failed" if missing_credentials_rows else "warning",
                "severity": "error" if missing_credentials_rows else "warning",
                "details": {"missing_credentials_count": len(missing_credentials_rows)},
            },
            {
                "check": "auth_scope_contract",
                "status": "failed" if missing_scope_rows else "passed",
                "severity": "error" if missing_scope_rows else "info",
                "details": {"missing_scope_count": len(missing_scope_rows)},
            },
            {
                "check": "token_ttl",
                "status": "failed" if expired_rows else "passed",
                "severity": "error" if expired_rows else "info",
                "details": {"expired_count": len(expired_rows)},
            },
        ]
        diagnostic_id = self._diagnostic_id(
            stage="auth_preflight",
            action=clean_action,
            code="auth_preflight_failed",
            fingerprint={
                "blocked_provider_count": len(blocked_rows),
                "missing_scope_count": len(missing_scope_rows),
                "expired_count": len(expired_rows),
                "missing_credentials_count": len(missing_credentials_rows),
            },
        )
        remediation_plan = self._structured_remediation_plan(
            stage="auth_preflight",
            action=clean_action,
            code="auth_preflight_failed",
            hints=remediation_hints,
        )
        remediation_contract = self._build_remediation_contract(
            hints=remediation_hints,
            diagnostics=diagnostics,
        )
        severity_score = self._diagnostic_severity_score(
            severity="error",
            checks=checks,
            remediation_contract=remediation_contract,
        )
        return {
            "diagnostic_id": diagnostic_id,
            "code": "auth_preflight_failed",
            "contract_stage": "auth_preflight",
            "action": clean_action,
            "message": str(payload.get("message", "")).strip(),
            "operation_class": str(payload.get("operation_class", "")).strip().lower(),
            "required_min_ttl_s": self._coerce_int(payload.get("required_min_ttl_s", 0), minimum=0, maximum=86_400, default=0),
            "auth_rows": auth_rows[:12],
            "severity": "error",
            "severity_score": round(severity_score, 6),
            "blocking_class": str(remediation_contract.get("blocking_class", "")).strip().lower(),
            "estimated_recovery_s": int(remediation_contract.get("estimated_recovery_s", 0) or 0),
            "automation_tier": str(remediation_contract.get("automation_tier", "")).strip().lower(),
            "diagnostics": diagnostics,
            "checks": checks,
            "remediation_hints": remediation_hints,
            "remediation_plan": remediation_plan,
            "remediation_contract": remediation_contract,
        }

    def _runtime_provider_orchestration_rows(
        self,
        *,
        providers: List[str],
        blocked_candidates: List[Dict[str, Any]] | None = None,
        outage_blocked: List[Dict[str, Any]] | None = None,
        auth_preflight: Dict[str, Any] | None = None,
    ) -> List[Dict[str, Any]]:
        blocked_rows = [dict(row) for row in (blocked_candidates if isinstance(blocked_candidates, list) else []) if isinstance(row, dict)]
        outage_rows = [dict(row) for row in (outage_blocked if isinstance(outage_blocked, list) else []) if isinstance(row, dict)]
        auth_payload = auth_preflight if isinstance(auth_preflight, dict) else {}

        provider_set: List[str] = []
        for provider in providers:
            clean_provider = self._normalize_provider(str(provider))
            if clean_provider and clean_provider not in provider_set:
                provider_set.append(clean_provider)
        for row in blocked_rows:
            clean_provider = self._normalize_provider(str(row.get("provider", "")))
            if clean_provider and clean_provider not in provider_set:
                provider_set.append(clean_provider)
        for row in outage_rows:
            clean_provider = self._normalize_provider(str(row.get("provider", "")))
            if clean_provider and clean_provider not in provider_set:
                provider_set.append(clean_provider)
        auth_rows_raw = auth_payload.get("auth_rows", [])
        auth_rows = [dict(row) for row in auth_rows_raw if isinstance(row, dict)] if isinstance(auth_rows_raw, list) else []
        for row in auth_rows:
            clean_provider = self._normalize_provider(str(row.get("provider", "")))
            if clean_provider and clean_provider not in provider_set:
                provider_set.append(clean_provider)

        blocked_map: Dict[str, Dict[str, Any]] = {}
        for row in blocked_rows:
            provider = self._normalize_provider(str(row.get("provider", "")))
            if not provider:
                continue
            retry_after_s = self._coerce_float(
                row.get("retry_after_s", 0.0),
                minimum=0.0,
                maximum=float(self.max_cooldown_s),
                default=0.0,
            )
            failure_ema = self._coerce_float(
                row.get("failure_ema", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            existing = blocked_map.get(provider)
            if (
                existing is None
                or retry_after_s
                < self._coerce_float(
                    existing.get("retry_after_s", float(self.max_cooldown_s)),
                    minimum=0.0,
                    maximum=float(self.max_cooldown_s),
                    default=float(self.max_cooldown_s),
                )
                or failure_ema
                > self._coerce_float(
                    existing.get("failure_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
            ):
                blocked_map[provider] = {
                    "retry_after_s": round(retry_after_s, 3),
                    "failure_ema": round(failure_ema, 6),
                    "last_category": str(row.get("last_category", "")).strip().lower(),
                }

        outage_map: Dict[str, Dict[str, Any]] = {}
        for row in outage_rows:
            provider = self._normalize_provider(str(row.get("provider", "")))
            if not provider:
                continue
            outage_pressure = self._coerce_float(
                row.get("outage_pressure", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            threshold = self._coerce_float(
                row.get("preflight_block_threshold", self.outage_preflight_block_threshold),
                minimum=0.2,
                maximum=1.0,
                default=self.outage_preflight_block_threshold,
            )
            existing = outage_map.get(provider)
            if (
                existing is None
                or outage_pressure
                > self._coerce_float(
                    existing.get("outage_pressure", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
            ):
                outage_map[provider] = {
                    "outage_pressure": round(outage_pressure, 6),
                    "preflight_block_threshold": round(threshold, 6),
                }

        auth_map: Dict[str, Dict[str, Any]] = {}
        for row in auth_rows:
            provider = self._normalize_provider(str(row.get("provider", "")))
            if not provider:
                continue
            status = str(row.get("status", "")).strip().lower()
            auth_map[provider] = {
                "status": status or "unknown",
                "reason": str(row.get("reason", "")).strip().lower(),
                "auth_ready": bool(status == "ready"),
                "credentials_missing": bool(row.get("credentials_missing", False)),
            }

        with self._lock:
            provider_states = {
                provider: dict(self._provider_states.get(provider, {}))
                for provider in provider_set
            }

        rows: List[Dict[str, Any]] = []
        for provider in provider_set:
            state = provider_states.get(provider, {})
            blocked = blocked_map.get(provider, {})
            outage = outage_map.get(provider, {})
            auth = auth_map.get(provider, {})
            retry_after_s = self._coerce_float(
                blocked.get("retry_after_s", 0.0),
                minimum=0.0,
                maximum=float(self.max_cooldown_s),
                default=0.0,
            )
            failure_ema = self._coerce_float(
                state.get("failure_ema", blocked.get("failure_ema", 0.0)),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            outage_ema = self._coerce_float(
                state.get("outage_ema", outage.get("outage_pressure", 0.0)),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            availability_ema = self._coerce_float(
                state.get("availability_ema", 0.55),
                minimum=0.0,
                maximum=1.0,
                default=0.55,
            )
            outage_pressure = self._coerce_float(
                outage.get("outage_pressure", outage_ema),
                minimum=0.0,
                maximum=1.0,
                default=outage_ema,
            )
            auth_status = str(auth.get("status", "unknown")).strip().lower()
            auth_ready = bool(auth.get("auth_ready", False))
            cooldown_penalty = min(0.46, (retry_after_s / max(30.0, float(self.max_cooldown_s))) * 0.52)
            outage_penalty = min(0.5, outage_pressure * 0.46)
            auth_penalty = 0.0
            if auth_status in {"blocked", "degraded"}:
                auth_penalty = 0.26 if auth_status == "blocked" else 0.14
            reliability_score = self._coerce_float(
                (availability_ema * 0.48)
                + ((1.0 - failure_ema) * 0.28)
                + ((1.0 - outage_ema) * 0.24),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            orchestration_score = self._coerce_float(
                reliability_score - cooldown_penalty - outage_penalty - auth_penalty,
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            rows.append(
                {
                    "provider": provider,
                    "score": round(orchestration_score, 6),
                    "reliability_score": round(reliability_score, 6),
                    "retry_after_s": round(retry_after_s, 3),
                    "failure_ema": round(failure_ema, 6),
                    "outage_ema": round(outage_ema, 6),
                    "outage_pressure": round(outage_pressure, 6),
                    "cooldown_active": bool(retry_after_s > 0.0),
                    "outage_active": bool(outage_pressure >= self.outage_preflight_block_threshold),
                    "auth_status": auth_status or "unknown",
                    "auth_ready": bool(auth_ready),
                    "auth_reason": str(auth.get("reason", "")).strip().lower(),
                    "credentials_missing": bool(auth.get("credentials_missing", False)),
                }
            )
        rows.sort(
            key=lambda row: (
                -self._coerce_float(row.get("score", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                self._coerce_float(row.get("retry_after_s", 0.0), minimum=0.0, maximum=float(self.max_cooldown_s), default=0.0),
                self._coerce_float(row.get("outage_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                str(row.get("provider", "")),
            )
        )
        return rows[:16]

    def _build_runtime_block_contract_diagnostic(
        self,
        *,
        action: str,
        reason: str,
        providers: List[str],
        blocked_candidates: List[Dict[str, Any]] | None = None,
        outage_blocked: List[Dict[str, Any]] | None = None,
        auth_preflight: Dict[str, Any] | None = None,
        message: str = "",
    ) -> Dict[str, Any]:
        clean_action = str(action or "").strip().lower()
        clean_reason = str(reason or "").strip().lower()
        if clean_reason not in {"cooldown", "outage"}:
            clean_reason = "runtime"
        blocked = [dict(row) for row in (blocked_candidates if isinstance(blocked_candidates, list) else []) if isinstance(row, dict)]
        outage = [dict(row) for row in (outage_blocked if isinstance(outage_blocked, list) else []) if isinstance(row, dict)]
        auth = auth_preflight if isinstance(auth_preflight, dict) else {}
        normalized_providers: List[str] = []
        for provider in providers:
            clean_provider = self._normalize_provider(str(provider))
            if clean_provider and clean_provider not in normalized_providers:
                normalized_providers.append(clean_provider)

        code = {
            "cooldown": "provider_cooldown_blocked",
            "outage": "provider_outage_blocked",
            "runtime": "provider_runtime_blocked",
        }.get(clean_reason, "provider_runtime_blocked")
        severity = "error" if clean_reason == "outage" else "warning"
        blocked_provider_rows: List[Dict[str, Any]] = []
        for row in blocked:
            provider = self._normalize_provider(str(row.get("provider", "")))
            if not provider:
                continue
            blocked_provider_rows.append(
                {
                    "provider": provider,
                    "retry_after_s": round(
                        self._coerce_float(
                            row.get("retry_after_s", 0.0),
                            minimum=0.0,
                            maximum=float(self.max_cooldown_s),
                            default=0.0,
                        ),
                        3,
                    ),
                    "failure_ema": round(
                        self._coerce_float(
                            row.get("failure_ema", 0.0),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        6,
                    ),
                    "last_category": str(row.get("last_category", "")).strip().lower(),
                }
            )
        outage_provider_rows: List[Dict[str, Any]] = []
        for row in outage:
            provider = self._normalize_provider(str(row.get("provider", "")))
            if not provider:
                continue
            outage_provider_rows.append(
                {
                    "provider": provider,
                    "outage_pressure": round(
                        self._coerce_float(
                            row.get("outage_pressure", 0.0),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        6,
                    ),
                    "preflight_block_threshold": round(
                        self._coerce_float(
                            row.get("preflight_block_threshold", self.outage_preflight_block_threshold),
                            minimum=0.2,
                            maximum=1.0,
                            default=self.outage_preflight_block_threshold,
                        ),
                        6,
                    ),
                }
            )
        retry_after_s = 0.0
        if blocked_provider_rows:
            retry_after_s = min(
                self._coerce_float(
                    row.get("retry_after_s", 0.0),
                    minimum=0.0,
                    maximum=float(self.max_cooldown_s),
                    default=0.0,
                )
                for row in blocked_provider_rows
            )
        providers_blocked = [str(row.get("provider", "")).strip().lower() for row in blocked_provider_rows]
        for row in outage_provider_rows:
            provider = str(row.get("provider", "")).strip().lower()
            if provider and provider not in providers_blocked:
                providers_blocked.append(provider)
        providers_blocked = [provider for provider in providers_blocked if provider]
        providers_total = len(normalized_providers)
        blocked_ratio = min(1.0, float(len(providers_blocked)) / max(1.0, float(providers_total)))
        provider_orchestration = self._runtime_provider_orchestration_rows(
            providers=normalized_providers,
            blocked_candidates=blocked,
            outage_blocked=outage,
            auth_preflight=auth,
        )
        primary_provider = str(provider_orchestration[0].get("provider", "")).strip().lower() if provider_orchestration else ""
        fallback_provider = ""
        for row in provider_orchestration[1:]:
            provider = self._normalize_provider(str(row.get("provider", "")))
            if provider and provider != primary_provider:
                fallback_provider = provider
                break
        orchestration_pressure = self._coerce_float(
            1.0
            - (
                self._coerce_float(
                    provider_orchestration[0].get("score", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                if provider_orchestration
                else 0.0
            ),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )

        remediation_hints = self._build_runtime_remediation_hints(
            action=clean_action,
            providers=normalized_providers,
            blocked_candidates=blocked,
            outage_blocked=outage,
            auth_preflight=auth,
            reason=clean_reason,
        )
        remediation_plan = self._structured_remediation_plan(
            stage="runtime_reliability",
            action=clean_action,
            code=code,
            hints=remediation_hints,
        )
        diagnostics = {
            "domain": "runtime_reliability",
            "failure_code": code,
            "reason": clean_reason,
            "provider_count": int(providers_total),
            "blocked_provider_count": int(len(providers_blocked)),
            "blocked_ratio": round(blocked_ratio, 6),
            "cooldown_block_count": int(len(blocked_provider_rows)),
            "outage_block_count": int(len(outage_provider_rows)),
            "retry_after_s": round(retry_after_s, 3),
            "primary_provider": primary_provider,
            "fallback_provider": fallback_provider,
            "orchestration_pressure": round(orchestration_pressure, 6),
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        checks: List[Dict[str, Any]] = [
            {
                "check": "provider_availability",
                "status": "failed" if providers_total > 0 and len(providers_blocked) >= providers_total else "warning",
                "severity": "error" if providers_total > 0 and len(providers_blocked) >= providers_total else "warning",
                "details": {
                    "provider_count": int(providers_total),
                    "blocked_provider_count": int(len(providers_blocked)),
                    "blocked_ratio": round(blocked_ratio, 6),
                },
            },
            {
                "check": "provider_cooldown",
                "status": "failed" if clean_reason == "cooldown" else ("warning" if blocked_provider_rows else "passed"),
                "severity": "warning" if blocked_provider_rows else "info",
                "details": {
                    "count": int(len(blocked_provider_rows)),
                    "retry_after_s": round(retry_after_s, 3),
                },
            },
            {
                "check": "provider_outage",
                "status": "failed" if clean_reason == "outage" else ("warning" if outage_provider_rows else "passed"),
                "severity": "error" if clean_reason == "outage" else ("warning" if outage_provider_rows else "info"),
                "details": {
                    "count": int(len(outage_provider_rows)),
                    "max_pressure": round(
                        max(
                            [
                                self._coerce_float(
                                    row.get("outage_pressure", 0.0),
                                    minimum=0.0,
                                    maximum=1.0,
                                    default=0.0,
                                )
                                for row in outage_provider_rows
                            ]
                            or [0.0]
                        ),
                        6,
                    ),
                },
            },
        ]
        if provider_orchestration:
            checks.append(
                {
                    "check": "provider_orchestration",
                    "status": "warning" if orchestration_pressure >= 0.52 else "passed",
                    "severity": "warning" if orchestration_pressure >= 0.52 else "info",
                    "details": {
                        "primary_provider": primary_provider,
                        "fallback_provider": fallback_provider,
                        "orchestration_pressure": round(orchestration_pressure, 6),
                    },
                }
            )
        auth_rows_raw = auth.get("auth_rows", [])
        auth_rows = [dict(row) for row in auth_rows_raw if isinstance(row, dict)] if isinstance(auth_rows_raw, list) else []
        auth_blocked = [
            str(row.get("provider", "")).strip().lower()
            for row in auth_rows
            if str(row.get("status", "")).strip().lower() in {"blocked", "degraded"}
            and str(row.get("provider", "")).strip()
        ]
        if auth_blocked:
            checks.append(
                {
                    "check": "auth_contract_alignment",
                    "status": "warning",
                    "severity": "warning",
                    "details": {"auth_blocked_providers": auth_blocked[:8]},
                }
            )

        diagnostic_id = self._diagnostic_id(
            stage="runtime_reliability",
            action=clean_action,
            code=code,
            fingerprint={
                "reason": clean_reason,
                "providers": normalized_providers[:12],
                "blocked": blocked_provider_rows[:8],
                "outage": outage_provider_rows[:8],
                "primary_provider": primary_provider,
                "fallback_provider": fallback_provider,
            },
        )
        remediation_contract = self._build_remediation_contract(
            hints=remediation_hints,
            diagnostics=diagnostics,
        )
        severity_score = self._diagnostic_severity_score(
            severity=severity,
            checks=checks,
            remediation_contract=remediation_contract,
        )
        return {
            "diagnostic_id": diagnostic_id,
            "code": code,
            "contract_stage": "runtime_reliability",
            "action": clean_action,
            "message": str(message or "").strip(),
            "reason": clean_reason,
            "providers": normalized_providers[:16],
            "blocked_providers": providers_blocked[:16],
            "retry_after_s": round(retry_after_s, 3),
            "severity": severity,
            "severity_score": round(severity_score, 6),
            "blocking_class": str(remediation_contract.get("blocking_class", "")).strip().lower(),
            "estimated_recovery_s": int(remediation_contract.get("estimated_recovery_s", 0) or 0),
            "automation_tier": str(remediation_contract.get("automation_tier", "")).strip().lower(),
            "diagnostics": diagnostics,
            "checks": checks,
            "cooldown_providers": blocked_provider_rows[:16],
            "outage_providers": outage_provider_rows[:16],
            "provider_orchestration": provider_orchestration[:12],
            "primary_provider": primary_provider,
            "fallback_provider": fallback_provider,
            "remediation_hints": remediation_hints,
            "remediation_plan": remediation_plan,
            "remediation_contract": remediation_contract,
        }

    def _build_runtime_remediation_hints(
        self,
        *,
        action: str,
        providers: List[str],
        blocked_candidates: List[Dict[str, Any]] | None = None,
        outage_blocked: List[Dict[str, Any]] | None = None,
        auth_preflight: Dict[str, Any] | None = None,
        reason: str = "",
    ) -> List[Dict[str, Any]]:
        clean_action = str(action or "").strip().lower()
        operation_class = self._operation_class(clean_action)
        action_domain = self._action_domain(clean_action)
        normalized_providers: List[str] = []
        for provider in providers:
            clean_provider = self._normalize_provider(str(provider))
            if clean_provider and clean_provider not in normalized_providers:
                normalized_providers.append(clean_provider)

        blocked = blocked_candidates if isinstance(blocked_candidates, list) else []
        outage = outage_blocked if isinstance(outage_blocked, list) else []
        auth = auth_preflight if isinstance(auth_preflight, dict) else {}
        provider_orchestration = self._runtime_provider_orchestration_rows(
            providers=normalized_providers,
            blocked_candidates=[dict(row) for row in blocked if isinstance(row, dict)],
            outage_blocked=[dict(row) for row in outage if isinstance(row, dict)],
            auth_preflight=auth,
        )
        selected_provider = ""
        if provider_orchestration:
            selected_provider = self._normalize_provider(str(provider_orchestration[0].get("provider", "")))
        if not selected_provider and normalized_providers:
            selected_provider = self._normalize_provider(str(normalized_providers[0]))
        provider_for_auth = selected_provider or "auto"
        fallback_provider = ""
        for candidate in [str(row.get("provider", "")) for row in provider_orchestration if isinstance(row, dict)] + normalized_providers:
            clean_candidate = self._normalize_provider(candidate)
            if not clean_candidate:
                continue
            if clean_candidate == selected_provider:
                continue
            fallback_provider = clean_candidate
            break
        if not fallback_provider:
            for candidate in normalized_providers:
                if candidate and candidate != selected_provider:
                    fallback_provider = candidate
                    break

        hints: List[Dict[str, Any]] = [
            {
                "id": "connector_status_probe",
                "priority": 1,
                "confidence": 0.82,
                "summary": "Probe connector health and auth routing before retrying external action.",
                "tool_action": {
                    "action": "external_connector_status",
                    "args": {"provider": provider_for_auth if provider_for_auth != "auto" else "auto"},
                },
                "provider": provider_for_auth,
                "operation_class": operation_class,
                "action_domain": action_domain,
                "reason": str(reason or "").strip().lower(),
            }
        ]
        hints.append(
            {
                "id": "connector_contract_preflight",
                "priority": 1,
                "confidence": 0.8,
                "summary": "Re-run connector preflight with contract diagnostics before remediation retry.",
                "tool_action": {
                    "action": "external_connector_preflight",
                    "args": {
                        "action": clean_action,
                        "provider": provider_for_auth if provider_for_auth else "auto",
                    },
                },
                "provider": provider_for_auth,
                "operation_class": operation_class,
                "action_domain": action_domain,
                "reason": str(reason or "").strip().lower(),
            }
        )

        clean_reason = str(reason or "").strip().lower()
        if selected_provider:
            hints.extend(
                self._provider_operation_playbook_hints(
                    provider=selected_provider,
                    action=clean_action,
                    operation_class=operation_class,
                    action_domain=action_domain,
                    reason=clean_reason,
                    fallback_provider=fallback_provider,
                )
            )

        refresh_window_s = 900
        if operation_class in {"write", "mutate"}:
            refresh_window_s = 2100
        elif operation_class in {"auth"}:
            refresh_window_s = 1500
        elif operation_class in {"maintenance"}:
            refresh_window_s = 1200
        if clean_reason in {"cooldown", "outage"}:
            hints.append(
                {
                    "id": "oauth_maintenance_preflight",
                    "priority": 2,
                    "confidence": 0.74,
                    "summary": "Run proactive token maintenance to improve provider readiness.",
                    "tool_action": {
                        "action": "oauth_token_maintain",
                        "args": {
                            "provider": provider_for_auth,
                            "refresh_window_s": refresh_window_s,
                            "limit": 80 if operation_class in {"write", "mutate"} else 40,
                        },
                    },
                    "provider": provider_for_auth,
                    "operation_class": operation_class,
                    "action_domain": action_domain,
                    "reason": clean_reason,
                }
            )
            hints.append(
                {
                    "id": "provider_auto_route",
                    "priority": 3,
                    "confidence": 0.66,
                    "summary": "Route via provider=auto to allow reliability selector fallback.",
                    "args_patch": {"provider": "auto"},
                    "provider": provider_for_auth,
                    "operation_class": operation_class,
                    "action_domain": action_domain,
                    "reason": clean_reason,
                }
            )
            if fallback_provider:
                fallback_confidence = 0.78 if clean_reason == "outage" else 0.72
                hints.append(
                    {
                        "id": f"switch_to_fallback_{fallback_provider}",
                        "priority": 2,
                        "confidence": fallback_confidence,
                        "summary": (
                            f"Route retry to fallback provider '{fallback_provider}' "
                            f"for {action_domain} {operation_class} workload."
                        ),
                        "args_patch": {"provider": fallback_provider},
                        "provider": selected_provider or provider_for_auth,
                        "fallback_provider": fallback_provider,
                        "operation_class": operation_class,
                        "action_domain": action_domain,
                        "reason": clean_reason,
                    }
                )

        if provider_orchestration:
            retry_schedule: List[Dict[str, Any]] = []
            for row in provider_orchestration[:4]:
                if not isinstance(row, dict):
                    continue
                provider = self._normalize_provider(str(row.get("provider", "")))
                if not provider:
                    continue
                retry_after_s = self._coerce_float(
                    row.get("retry_after_s", 0.0),
                    minimum=0.0,
                    maximum=float(self.max_cooldown_s),
                    default=0.0,
                )
                outage_pressure = self._coerce_float(
                    row.get("outage_pressure", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                score = self._coerce_float(
                    row.get("score", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                staged_delay_s = retry_after_s + (outage_pressure * 90.0)
                retry_schedule.append(
                    {
                        "provider": provider,
                        "delay_s": round(staged_delay_s, 3),
                        "score": round(score, 6),
                        "auth_status": str(row.get("auth_status", "")).strip().lower(),
                    }
                )
            if retry_schedule:
                hints.append(
                    {
                        "id": "staggered_provider_retry",
                        "priority": 2,
                        "confidence": 0.76 if clean_reason in {"cooldown", "outage"} else 0.68,
                        "summary": "Apply staged provider retry cadence based on cooldown/outage/auth diagnostics.",
                        "provider": provider_for_auth,
                        "operation_class": operation_class,
                        "action_domain": action_domain,
                        "reason": clean_reason or "runtime",
                        "retry_schedule": retry_schedule[:4],
                        "remediation": {
                            "type": "staggered_provider_retry",
                            "jitter_s": 0.2,
                            "schedule": retry_schedule[:4],
                        },
                    }
                )

        auth_rows_raw = auth.get("auth_rows", [])
        auth_rows = [row for row in auth_rows_raw if isinstance(row, dict)] if isinstance(auth_rows_raw, list) else []
        for row in auth_rows[:6]:
            status = str(row.get("status", "")).strip().lower()
            provider = self._normalize_provider(str(row.get("provider", "")))
            reason_code = str(row.get("reason", "")).strip().lower()
            if not provider or status not in {"blocked", "degraded"}:
                continue
            if reason_code in {"token_expired", "token_ttl_low"}:
                hints.append(
                    {
                        "id": f"refresh_token_{provider}",
                        "priority": 1,
                        "confidence": 0.9,
                        "summary": f"Refresh OAuth token for provider '{provider}'.",
                        "tool_action": {
                            "action": "oauth_token_refresh",
                            "args": {"provider": provider},
                        },
                        "provider": provider,
                        "operation_class": operation_class,
                        "action_domain": action_domain,
                        "reason": reason_code,
                    }
                )
            elif reason_code == "missing_required_scope":
                hints.append(
                    {
                        "id": f"repair_scope_contract_{provider}",
                        "priority": 1,
                        "confidence": 0.88,
                        "summary": f"Repair OAuth scopes for provider '{provider}' and re-run auth preflight.",
                        "tool_action": {
                            "action": "oauth_token_maintain",
                            "args": {
                                "provider": provider,
                                "refresh_window_s": max(refresh_window_s, 1800),
                                "limit": 120 if operation_class in {"write", "mutate"} else 80,
                            },
                        },
                        "provider": provider,
                        "operation_class": operation_class,
                        "action_domain": action_domain,
                        "reason": reason_code,
                    }
                )
            elif bool(row.get("credentials_missing", False)):
                hints.append(
                    {
                        "id": f"connect_credentials_{provider}",
                        "priority": 2,
                        "confidence": 0.74,
                        "summary": f"Connect credentials for provider '{provider}' before retrying this action.",
                        "provider": provider,
                        "operation_class": operation_class,
                        "action_domain": action_domain,
                        "reason": reason_code or "credentials_missing",
                        "remediation": {
                            "type": "oauth_account_connect",
                            "provider": provider,
                        },
                    }
                )

        dedup: Dict[str, Dict[str, Any]] = {}
        for hint in hints:
            if not isinstance(hint, dict):
                continue
            hint_id = str(hint.get("id", "")).strip().lower()
            if not hint_id:
                continue
            existing = dedup.get(hint_id)
            if existing is None:
                dedup[hint_id] = hint
                continue
            old_conf = self._coerce_float(existing.get("confidence", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            new_conf = self._coerce_float(hint.get("confidence", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            if new_conf > old_conf:
                dedup[hint_id] = hint

        selected = list(dedup.values())
        selected.sort(
            key=lambda row: (
                self._coerce_int(row.get("priority", 999), minimum=1, maximum=9999, default=999),
                -self._coerce_float(row.get("confidence", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                str(row.get("id", "")),
            )
        )
        return selected[:14]

    @staticmethod
    def _action_domain(action: str) -> str:
        clean_action = str(action or "").strip().lower()
        if clean_action.startswith("external_email_"):
            return "email"
        if clean_action.startswith("external_calendar_"):
            return "calendar"
        if clean_action.startswith("external_doc_"):
            return "document"
        if clean_action.startswith("external_task_"):
            return "task"
        if clean_action.startswith("oauth_token_"):
            return "auth"
        if clean_action.startswith("external_"):
            return "external"
        return "general"

    def _provider_operation_playbook_hints(
        self,
        *,
        provider: str,
        action: str,
        operation_class: str,
        action_domain: str,
        reason: str,
        fallback_provider: str = "",
    ) -> List[Dict[str, Any]]:
        clean_provider = self._normalize_provider(str(provider))
        if clean_provider not in {"google", "graph", "smtp"}:
            return []

        clean_action = str(action or "").strip().lower()
        clean_operation = str(operation_class or "").strip().lower() or self._operation_class(clean_action)
        clean_domain = str(action_domain or "").strip().lower() or self._action_domain(clean_action)
        clean_reason = str(reason or "").strip().lower()
        fallback = self._normalize_provider(str(fallback_provider))
        hints: List[Dict[str, Any]] = []

        if clean_provider in {"google", "graph"}:
            maintain_window_s = 900
            maintain_limit = 50
            if clean_operation in {"write", "mutate"}:
                maintain_window_s = 2400
                maintain_limit = 110
            elif clean_operation in {"auth"}:
                maintain_window_s = 1800
                maintain_limit = 80
            confidence = 0.7
            if clean_reason in {"cooldown", "outage"}:
                confidence = 0.83
            hints.append(
                {
                    "id": f"{clean_provider}_{clean_domain}_{clean_operation}_playbook_maintain",
                    "priority": 2,
                    "confidence": confidence,
                    "summary": (
                        f"Run {clean_provider} readiness playbook for "
                        f"{clean_domain} {clean_operation} workflow."
                    ),
                    "tool_action": {
                        "action": "oauth_token_maintain",
                        "args": {
                            "provider": clean_provider,
                            "refresh_window_s": maintain_window_s,
                            "limit": maintain_limit,
                        },
                    },
                    "provider": clean_provider,
                    "operation_class": clean_operation,
                    "action_domain": clean_domain,
                    "reason": clean_reason,
                    "remediation": {
                        "type": "provider_playbook_maintenance",
                        "provider": clean_provider,
                        "operation_class": clean_operation,
                        "domain": clean_domain,
                    },
                }
            )
            hints.append(
                {
                    "id": f"{clean_provider}_{clean_domain}_connector_probe",
                    "priority": 3,
                    "confidence": 0.68 if clean_reason in {"cooldown", "outage"} else 0.6,
                    "summary": (
                        f"Probe {clean_provider} connector state after maintenance "
                        f"for {clean_domain} operations."
                    ),
                    "tool_action": {
                        "action": "external_connector_status",
                        "args": {"provider": clean_provider},
                    },
                    "provider": clean_provider,
                    "operation_class": clean_operation,
                    "action_domain": clean_domain,
                    "reason": clean_reason,
                }
            )

        if clean_provider == "smtp":
            hints.append(
                {
                    "id": "smtp_delivery_diagnostics",
                    "priority": 2,
                    "confidence": 0.71,
                    "summary": "Probe SMTP connector status and route through OAuth provider if instability persists.",
                    "tool_action": {
                        "action": "external_connector_status",
                        "args": {"provider": "smtp"},
                    },
                    "provider": "smtp",
                    "operation_class": clean_operation,
                    "action_domain": clean_domain,
                    "reason": clean_reason,
                }
            )

        if (
            fallback
            and fallback != clean_provider
            and clean_reason in {"cooldown", "outage", "fallback"}
        ):
            hints.append(
                {
                    "id": f"{clean_provider}_fallback_route_{fallback}",
                    "priority": 2,
                    "confidence": 0.8 if clean_reason == "outage" else 0.74,
                    "summary": (
                        f"Route retry from '{clean_provider}' to fallback provider '{fallback}' "
                        f"for {clean_domain} {clean_operation} action."
                    ),
                    "args_patch": {"provider": fallback},
                    "provider": clean_provider,
                    "fallback_provider": fallback,
                    "operation_class": clean_operation,
                    "action_domain": clean_domain,
                    "reason": clean_reason,
                    "remediation": {
                        "type": "fallback_provider_route",
                        "provider": clean_provider,
                        "fallback_provider": fallback,
                    },
                }
            )
        return hints[:6]

    def _check_contract(self, action: str, payload: Dict[str, Any]) -> str:
        if not self.preflight_contract_strict:
            return ""
        clean_action = str(action or "").strip().lower()
        contract = self._ACTION_CONTRACTS.get(clean_action, {})
        if isinstance(contract, dict) and contract:
            required_all = contract.get("required_all", [])
            if isinstance(required_all, list):
                missing_all = [field for field in required_all if not self._has_payload_value(payload, str(field))]
                if missing_all:
                    return f"Missing required fields for {clean_action}: {', '.join(missing_all)}"

            required_any = contract.get("required_any", [])
            if isinstance(required_any, list):
                for group in required_any:
                    fields = [str(item) for item in group if str(item).strip()] if isinstance(group, list) else []
                    if not fields:
                        continue
                    if any(self._has_payload_value(payload, field) for field in fields):
                        continue
                    return f"{clean_action} requires at least one of: {', '.join(fields)}"

        schema_error = self._check_schema_contract(clean_action, payload)
        if schema_error:
            return schema_error

        semantic_error = self._check_semantic_contract(clean_action, payload)
        if semantic_error:
            return semantic_error
        return ""

    @staticmethod
    def _has_payload_value(payload: Dict[str, Any], field: str) -> bool:
        value = payload.get(str(field))
        if value is None:
            return False
        if isinstance(value, str):
            return bool(value.strip())
        if isinstance(value, list):
            return len(value) > 0
        if isinstance(value, dict):
            return len(value) > 0
        return True

    def _check_schema_contract(self, action: str, payload: Dict[str, Any]) -> str:
        schema_rows = self._ACTION_FIELD_SCHEMAS.get(str(action or "").strip().lower(), {})
        if not isinstance(schema_rows, dict) or not schema_rows:
            return ""
        for field, schema in schema_rows.items():
            clean_field = str(field or "").strip()
            if not clean_field or not isinstance(schema, dict):
                continue
            required = self._coerce_bool(schema.get("required", False), default=False)
            if required and not self._has_payload_value(payload, clean_field):
                return f"Missing required field for {action}: {clean_field}"
            if clean_field not in payload:
                continue
            value = payload.get(clean_field)
            if value is None:
                if required:
                    return f"Missing required field for {action}: {clean_field}"
                continue
            error = self._validate_field_schema(action=action, field=clean_field, value=value, schema=schema)
            if error:
                return error
        return ""

    def _check_semantic_contract(self, action: str, payload: Dict[str, Any]) -> str:
        clean_action = str(action or "").strip().lower()
        if clean_action == "external_email_send":
            recipients = payload.get("to")
            rows = recipients if isinstance(recipients, list) else [recipients]
            invalid: List[str] = []
            for item in rows:
                if not isinstance(item, str):
                    invalid.append(str(item))
                    continue
                candidate = item.strip()
                if not candidate:
                    invalid.append(candidate)
                    continue
                if "@" not in candidate or candidate.startswith("@") or candidate.endswith("@"):
                    invalid.append(candidate)
            if invalid:
                return "external_email_send has invalid recipient addresses in 'to'."
            subject = str(payload.get("subject", "")).strip()
            body = str(payload.get("body", "")).strip()
            if not subject and not body:
                return "external_email_send requires at least one of: subject, body"

        if clean_action in {"external_calendar_create_event", "external_calendar_update_event"}:
            start_ts = self._to_timestamp(str(payload.get("start", "")).strip())
            end_ts = self._to_timestamp(str(payload.get("end", "")).strip())
            if start_ts > 0.0 and end_ts > 0.0 and end_ts <= start_ts:
                return f"{clean_action} requires end to be after start."

        if clean_action in {"external_task_create", "external_task_update"} and "status" in payload:
            status = str(payload.get("status", "")).strip().lower()
            if status:
                allowed = {"needsaction", "notstarted", "completed", "inprogress"}
                if status not in allowed:
                    return f"{clean_action} status must be one of: needsAction, notStarted, completed, inProgress"

        if clean_action.startswith("oauth_token_") and "provider" in payload:
            provider = self._normalize_provider(str(payload.get("provider", "")).strip())
            if provider and provider not in {"google", "graph"}:
                return f"{clean_action} provider must be one of: google, graph"

        return ""

    def _validate_field_schema(
        self,
        *,
        action: str,
        field: str,
        value: Any,
        schema: Dict[str, Any],
    ) -> str:
        expected = str(schema.get("type", "")).strip().lower()
        if expected == "string":
            if not isinstance(value, str):
                return f"{action}.{field} must be a string."
            text = value.strip()
            min_len = self._coerce_int(schema.get("min_len", 0), minimum=0, maximum=1_000_000, default=0)
            max_len = self._coerce_int(schema.get("max_len", 1_000_000), minimum=1, maximum=1_000_000, default=1_000_000)
            if min_len > 0 and len(text) < min_len:
                return f"{action}.{field} is too short (min {min_len})."
            if max_len > 0 and len(text) > max_len:
                return f"{action}.{field} is too long (max {max_len})."
            pattern = str(schema.get("regex", "")).strip()
            if pattern:
                try:
                    if not re.fullmatch(pattern, text):
                        return f"{action}.{field} has invalid format."
                except re.error:
                    pass
            allowed = schema.get("allowed", [])
            if isinstance(allowed, list) and allowed:
                lowered_allowed = {str(item).strip().lower() for item in allowed if str(item).strip()}
                if field == "provider":
                    normalized_provider = self._normalize_provider(text)
                    if normalized_provider not in lowered_allowed:
                        return f"{action}.{field} must be one of: {', '.join(sorted(lowered_allowed))}"
                elif text.lower() not in lowered_allowed:
                    return f"{action}.{field} must be one of: {', '.join(sorted(lowered_allowed))}"
            if self._coerce_bool(schema.get("iso_datetime", False), default=False):
                if self._to_timestamp(text) <= 0.0:
                    return f"{action}.{field} must be an ISO datetime."
            return ""

        if expected == "int":
            if isinstance(value, bool):
                return f"{action}.{field} must be an integer."
            try:
                parsed = int(value)
            except Exception:
                return f"{action}.{field} must be an integer."
            minimum = int(schema.get("min", -10_000_000_000))
            maximum = int(schema.get("max", 10_000_000_000))
            if parsed < minimum:
                return f"{action}.{field} must be >= {minimum}."
            if parsed > maximum:
                return f"{action}.{field} must be <= {maximum}."
            return ""

        if expected == "float":
            if isinstance(value, bool):
                return f"{action}.{field} must be a number."
            try:
                parsed_float = float(value)
            except Exception:
                return f"{action}.{field} must be a number."
            min_float = float(schema.get("min", -1_000_000_000.0))
            max_float = float(schema.get("max", 1_000_000_000.0))
            if parsed_float < min_float:
                return f"{action}.{field} must be >= {min_float}."
            if parsed_float > max_float:
                return f"{action}.{field} must be <= {max_float}."
            return ""

        if expected == "bool":
            if isinstance(value, bool):
                return ""
            lowered = str(value).strip().lower()
            if lowered in {"true", "false", "1", "0", "yes", "no", "on", "off"}:
                return ""
            return f"{action}.{field} must be a boolean."

        if expected == "list":
            if not isinstance(value, list):
                return f"{action}.{field} must be a list."
            min_items = self._coerce_int(schema.get("min_items", 0), minimum=0, maximum=100_000, default=0)
            max_items = self._coerce_int(schema.get("max_items", 100_000), minimum=1, maximum=100_000, default=100_000)
            if len(value) < min_items:
                return f"{action}.{field} must contain at least {min_items} items."
            if len(value) > max_items:
                return f"{action}.{field} must contain at most {max_items} items."
            item_type = str(schema.get("item_type", "")).strip().lower()
            if item_type == "string":
                item_min_len = self._coerce_int(schema.get("item_min_len", 0), minimum=0, maximum=1_000_000, default=0)
                item_max_len = self._coerce_int(schema.get("item_max_len", 1_000_000), minimum=1, maximum=1_000_000, default=1_000_000)
                for item in value:
                    if not isinstance(item, str):
                        return f"{action}.{field} entries must be strings."
                    text = item.strip()
                    if item_min_len > 0 and len(text) < item_min_len:
                        return f"{action}.{field} entries are too short."
                    if item_max_len > 0 and len(text) > item_max_len:
                        return f"{action}.{field} entries are too long."
            return ""

        if expected == "dict" and not isinstance(value, dict):
            return f"{action}.{field} must be an object."
        return ""

    def _provider_candidates(self, action: str, payload: Dict[str, Any]) -> List[str]:
        provider = self._normalize_provider(str(payload.get("provider", "")).strip())
        clean_action = str(action or "").strip().lower()
        rule_raw = self._ACTION_PROVIDER_RULES.get(clean_action, {})
        rule = rule_raw if isinstance(rule_raw, dict) else {}
        allow_raw = rule.get("allow", [])
        allow = [
            self._normalize_provider(str(item))
            for item in (allow_raw if isinstance(allow_raw, list) else [])
            if self._normalize_provider(str(item))
        ]
        if provider and provider != "auto":
            return [provider]
        if allow:
            return list(dict.fromkeys(allow))
        if clean_action in {"external_email_send", "external_email_list", "external_email_read"}:
            return ["google", "graph", "smtp"] if provider == "auto" or not provider else [provider]
        if clean_action.startswith("external_"):
            return ["google", "graph"] if provider == "auto" or not provider else [provider]
        if clean_action.startswith("oauth_token_"):
            from_payload = self._normalize_provider(str(payload.get("provider", "")).strip())
            return [from_payload] if from_payload and from_payload != "auto" else ["google", "graph"]
        return [provider] if provider else []

    def _auth_preflight_contract(
        self,
        *,
        action: str,
        payload: Dict[str, Any],
        providers: List[str],
        explicit_provider: str,
        metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not self.auth_precheck_enabled:
            return {"status": "success", "providers": list(providers), "auth_rows": [], "dropped_providers": []}
        clean_action = str(action or "").strip().lower()
        clean_providers = [self._normalize_provider(str(item)) for item in providers if self._normalize_provider(str(item))]
        if not clean_providers:
            return {"status": "success", "providers": [], "auth_rows": [], "dropped_providers": []}
        clean_explicit = self._normalize_provider(explicit_provider)
        operation_class = self._operation_class(clean_action)
        min_ttl_s = self._coerce_int(
            self._OPERATION_MIN_TOKEN_TTL_S.get(operation_class, self._OPERATION_MIN_TOKEN_TTL_S["default"]),
            minimum=0,
            maximum=86400,
            default=self._OPERATION_MIN_TOKEN_TTL_S["default"],
        )
        auth_rows: List[Dict[str, Any]] = []
        dropped: List[Dict[str, Any]] = []
        kept: List[str] = []
        ready_count = 0
        missing_count = 0
        for provider in clean_providers:
            row = self._evaluate_provider_auth(
                action=clean_action,
                provider=provider,
                payload=payload,
                metadata=metadata,
                min_ttl_s=min_ttl_s,
            )
            auth_rows.append(row)
            if bool(row.get("ready", False)):
                ready_count += 1
            if bool(row.get("credentials_missing", False)):
                missing_count += 1
            if bool(row.get("hard_fail", False)):
                dropped.append(
                    {
                        "provider": provider,
                        "reason": str(row.get("reason", "auth_precheck_failed")).strip().lower() or "auth_precheck_failed",
                        "source": str(row.get("source", "")).strip().lower(),
                    }
                )
                continue
            kept.append(provider)

        if ready_count > 0:
            filtered_soft: List[str] = []
            for provider in kept:
                row = next((item for item in auth_rows if str(item.get("provider", "")) == provider), {})
                if isinstance(row, dict) and bool(row.get("credentials_missing", False)):
                    dropped.append({"provider": provider, "reason": "credentials_missing", "source": str(row.get("source", ""))})
                    continue
                filtered_soft.append(provider)
            kept = filtered_soft

        explicit_row = next((row for row in auth_rows if str(row.get("provider", "")) == clean_explicit), {})
        if clean_explicit and clean_explicit not in {"", "auto"} and isinstance(explicit_row, dict):
            if bool(explicit_row.get("hard_fail", False)):
                reason = str(explicit_row.get("reason", "auth_precheck_failed")).strip() or "auth_precheck_failed"
                return {
                    "status": "error",
                    "action": clean_action,
                    "operation_class": operation_class,
                    "message": f"External auth preflight failed for provider '{clean_explicit}': {reason}.",
                    "providers": kept,
                    "auth_rows": auth_rows,
                    "dropped_providers": dropped,
                    "required_min_ttl_s": int(min_ttl_s),
                }

        if not kept:
            if self.auth_precheck_fail_closed and (missing_count > 0 or dropped):
                return {
                    "status": "error",
                    "action": clean_action,
                    "operation_class": operation_class,
                    "message": f"External auth preflight blocked all providers for action '{clean_action}'.",
                    "providers": [],
                    "auth_rows": auth_rows,
                    "dropped_providers": dropped,
                    "required_min_ttl_s": int(min_ttl_s),
                }
            kept = list(clean_providers)

        return {
            "status": "success",
            "action": clean_action,
            "operation_class": operation_class,
            "providers": list(dict.fromkeys(kept)),
            "auth_rows": auth_rows,
            "dropped_providers": dropped,
            "required_min_ttl_s": int(min_ttl_s),
        }

    def _evaluate_provider_auth(
        self,
        *,
        action: str,
        provider: str,
        payload: Dict[str, Any],
        metadata: Dict[str, Any],
        min_ttl_s: int,
    ) -> Dict[str, Any]:
        state = self._provider_auth_state(provider=provider, payload=payload, metadata=metadata)
        scopes = self._normalize_scope_list(state.get("scopes"))
        expires_in_s_raw = state.get("expires_in_s", None)
        expires_in_s = None
        if expires_in_s_raw is not None:
            expires_in_s = self._coerce_int(expires_in_s_raw, minimum=-86400 * 7, maximum=86400 * 365, default=0)
        has_refresh_token = self._coerce_bool(state.get("has_refresh_token", False), default=False)
        has_credentials_value = state.get("has_credentials", None)
        has_credentials: bool | None
        if isinstance(has_credentials_value, bool):
            has_credentials = has_credentials_value
        else:
            has_credentials = None

        reason = ""
        hard_fail = False
        ready = True
        credentials_missing = has_credentials is False
        missing_scopes: List[str] = []
        scope_status = "unknown"
        required_scope_spec = self._ACTION_SCOPE_REQUIREMENTS.get(action, {}).get(provider, {})
        any_of = required_scope_spec.get("any_of", []) if isinstance(required_scope_spec, dict) else []
        required_patterns = [str(item).strip().lower() for item in any_of if str(item).strip()]
        if required_patterns:
            if scopes:
                matched = any(self._scope_matches(required=required, scopes=scopes) for required in required_patterns)
                if not matched:
                    missing_scopes = list(required_patterns)
                    scope_status = "missing"
                    if self.auth_precheck_scope_strict:
                        hard_fail = True
                        ready = False
                        reason = "missing_required_scope"
                else:
                    scope_status = "ready"
            else:
                scope_status = "unknown"
        else:
            scope_status = "n/a"

        if has_credentials is False:
            ready = False
            reason = reason or "credentials_missing"
            if self.auth_precheck_fail_closed:
                hard_fail = True
        elif has_credentials is None:
            ready = False
            reason = reason or "credentials_unknown"

        if expires_in_s is not None:
            if expires_in_s <= 0:
                ready = False
                reason = reason or "token_expired"
                if self.auth_precheck_expiry_strict and not has_refresh_token:
                    hard_fail = True
            elif expires_in_s < min_ttl_s and not has_refresh_token:
                ready = False
                reason = reason or "token_ttl_low"
                if self.auth_precheck_expiry_strict:
                    hard_fail = True
            elif expires_in_s < self.auth_precheck_warn_ttl_s:
                ready = False
                reason = reason or "token_near_expiry"

        source = str(state.get("source", "")).strip().lower()
        status = "ready" if ready and not hard_fail else "warning"
        if hard_fail:
            status = "blocked"
        elif not ready and reason in {"credentials_missing", "token_expired", "token_ttl_low", "missing_required_scope"}:
            status = "degraded"

        return {
            "provider": provider,
            "status": status,
            "ready": bool(ready and not hard_fail),
            "hard_fail": bool(hard_fail),
            "reason": reason,
            "source": source,
            "has_credentials": has_credentials,
            "credentials_missing": bool(credentials_missing),
            "expires_in_s": expires_in_s,
            "min_ttl_s": int(min_ttl_s),
            "has_refresh_token": bool(has_refresh_token),
            "scope_status": scope_status,
            "missing_scopes": missing_scopes,
            "scopes_known": bool(scopes),
        }

    def _provider_auth_state(self, *, provider: str, payload: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        normalized_provider = self._normalize_provider(provider)
        meta_state = metadata.get("external_auth_state", {})
        if isinstance(meta_state, dict):
            providers_raw = meta_state.get("providers", {})
            if isinstance(providers_raw, dict):
                row = providers_raw.get(normalized_provider, {})
                if isinstance(row, dict) and row:
                    return {
                        "source": "runtime_auth_state",
                        "has_credentials": row.get("has_credentials"),
                        "expires_in_s": row.get("expires_in_s"),
                        "has_refresh_token": row.get("has_refresh_token"),
                        "scopes": row.get("scopes", []),
                    }

        if normalized_provider == "smtp":
            smtp_payload_host = str(payload.get("smtp_host", "")).strip()
            return {
                "source": "payload_or_env",
                "has_credentials": bool(smtp_payload_host or os.getenv("SMTP_HOST")),
                "expires_in_s": None,
                "has_refresh_token": False,
                "scopes": [],
            }
        if normalized_provider == "google":
            token = str(payload.get("google_access_token") or payload.get("access_token") or "").strip()
            if not token:
                token = str(os.getenv("GOOGLE_OAUTH_ACCESS_TOKEN") or os.getenv("GOOGLE_ACCESS_TOKEN") or "").strip()
            return {
                "source": "payload_or_env",
                "has_credentials": bool(token),
                "expires_in_s": payload.get("expires_in_s"),
                "has_refresh_token": bool(payload.get("refresh_token")),
                "scopes": payload.get("scopes", payload.get("scope", [])),
            }
        if normalized_provider == "graph":
            token = str(payload.get("graph_access_token") or payload.get("access_token") or "").strip()
            if not token:
                token = str(os.getenv("MICROSOFT_GRAPH_ACCESS_TOKEN") or "").strip()
            return {
                "source": "payload_or_env",
                "has_credentials": bool(token),
                "expires_in_s": payload.get("expires_in_s"),
                "has_refresh_token": bool(payload.get("refresh_token")),
                "scopes": payload.get("scopes", payload.get("scope", [])),
            }
        return {"source": "unknown", "has_credentials": None, "expires_in_s": None, "has_refresh_token": False, "scopes": []}

    @staticmethod
    def _normalize_scope_list(raw: Any) -> List[str]:
        if isinstance(raw, str):
            tokens = [item.strip().lower() for item in raw.replace(",", " ").split(" ")]
            return [item for item in tokens if item]
        if not isinstance(raw, list):
            return []
        out: List[str] = []
        for item in raw:
            value = str(item or "").strip().lower()
            if value and value not in out:
                out.append(value)
        return out

    @staticmethod
    def _scope_matches(*, required: str, scopes: List[str]) -> bool:
        need = str(required or "").strip().lower()
        if not need:
            return True
        for scope in scopes:
            clean_scope = str(scope or "").strip().lower()
            if not clean_scope:
                continue
            if clean_scope == need or need in clean_scope or clean_scope in need:
                return True
        return False

    def _providers_from_result(self, action: str, payload: Dict[str, Any], output: Dict[str, Any]) -> List[str]:
        providers: List[str] = []
        provider_keys = [
            str(output.get("provider", "")).strip(),
            str(payload.get("provider", "")).strip(),
        ]
        resilience = output.get("resilience")
        if isinstance(resilience, dict):
            fallback = resilience.get("provider_fallback")
            if isinstance(fallback, dict):
                provider_keys.append(str(fallback.get("selected_provider", "")).strip())
                failed_attempts = fallback.get("failed_attempts", [])
                if isinstance(failed_attempts, list):
                    for row in failed_attempts:
                        if not isinstance(row, dict):
                            continue
                        provider_keys.append(str(row.get("provider", "")).strip())

        for value in provider_keys:
            normalized = self._normalize_provider(value)
            if normalized and normalized not in providers and normalized != "auto":
                providers.append(normalized)
        if providers:
            return providers
        return self._provider_candidates(action, payload)

    def route_provider(
        self,
        *,
        action: str,
        args: Dict[str, Any] | None = None,
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        clean_action = str(action or "").strip().lower()
        if not self.is_managed_action(clean_action) or not self.provider_routing_enabled:
            return {"status": "skip"}
        payload = args if isinstance(args, dict) else {}
        explicit_provider = self._normalize_provider(str(payload.get("provider", "")).strip())
        providers = self._provider_candidates(clean_action, payload)
        contract_negotiation = self._negotiate_provider_contract(
            action=clean_action,
            payload=payload,
            providers=providers,
            explicit_provider=explicit_provider,
        )
        if str(contract_negotiation.get("status", "")).strip().lower() == "error":
            return {
                "status": "error",
                "action": clean_action,
                "message": str(contract_negotiation.get("message", "")).strip(),
                "contract_negotiation": contract_negotiation,
            }
        providers = contract_negotiation.get("providers", [])
        if not isinstance(providers, list):
            providers = []
        providers = [self._normalize_provider(str(item)) for item in providers if self._normalize_provider(str(item))]
        runtime_meta = metadata if isinstance(metadata, dict) else {}
        capability_negotiation = self._negotiate_provider_capability_contract(
            action=clean_action,
            payload=payload,
            providers=providers,
            explicit_provider=explicit_provider,
            metadata=runtime_meta,
        )
        if str(capability_negotiation.get("status", "")).strip().lower() == "error":
            return {
                "status": "error",
                "action": clean_action,
                "message": str(capability_negotiation.get("message", "")).strip(),
                "contract_negotiation": contract_negotiation,
                "capability_negotiation": capability_negotiation,
            }
        providers = capability_negotiation.get("providers", providers)
        if not isinstance(providers, list):
            providers = []
        providers = [self._normalize_provider(str(item)) for item in providers if self._normalize_provider(str(item))]
        if isinstance(contract_negotiation, dict):
            dropped_base = contract_negotiation.get("dropped_providers", [])
            dropped_rows = dropped_base if isinstance(dropped_base, list) else []
            capability_dropped = capability_negotiation.get("dropped_providers", [])
            if isinstance(capability_dropped, list) and capability_dropped:
                contract_negotiation["dropped_providers"] = self._merge_dropped_provider_rows(
                    dropped_rows,
                    [dict(row) for row in capability_dropped if isinstance(row, dict)],
                )
            contract_negotiation["capability_negotiation"] = capability_negotiation
        auth_preflight = self._auth_preflight_contract(
            action=clean_action,
            payload=payload,
            providers=providers,
            explicit_provider=explicit_provider,
            metadata=runtime_meta,
        )
        if str(auth_preflight.get("status", "")).strip().lower() == "error":
            return {
                "status": "error",
                "action": clean_action,
                "message": str(auth_preflight.get("message", "")).strip(),
                "contract_negotiation": contract_negotiation,
                "auth_preflight": auth_preflight,
            }
        providers = auth_preflight.get("providers", providers)
        if not isinstance(providers, list):
            providers = []
        providers = [self._normalize_provider(str(item)) for item in providers if self._normalize_provider(str(item))]
        if isinstance(contract_negotiation, dict):
            existing_dropped = contract_negotiation.get("dropped_providers", [])
            existing_rows = existing_dropped if isinstance(existing_dropped, list) else []
            auth_dropped = auth_preflight.get("dropped_providers", [])
            if isinstance(auth_dropped, list) and auth_dropped:
                contract_negotiation["dropped_providers"] = self._merge_dropped_provider_rows(
                    existing_rows,
                    [dict(row) for row in auth_dropped if isinstance(row, dict)],
                )
            contract_negotiation["auth_preflight"] = auth_preflight
        if not providers:
            return {"status": "skip", "contract_negotiation": contract_negotiation, "auth_preflight": auth_preflight}
        now_ts = time.time()
        with self._lock:
            provider_health = [
                self._provider_health_row(
                    provider=provider,
                    action=clean_action,
                    state=self._provider_states.get(provider, {}),
                    now_ts=now_ts,
                    metadata=runtime_meta,
                )
                for provider in providers
            ]
        rows = [row for row in provider_health if isinstance(row, dict)]
        blocked_candidates = [
            {
                "provider": str(row.get("provider", "")),
                "retry_after_s": float(row.get("retry_after_s", 0.0) or 0.0),
                "failure_ema": float(row.get("failure_ema", 0.0) or 0.0),
                "last_category": str(row.get("last_category", "")),
            }
            for row in rows
            if bool(row.get("cooldown_active", False))
        ]
        route = self._build_provider_route(
            action=clean_action,
            payload=payload,
            providers=providers,
            blocked_candidates=blocked_candidates,
            provider_health=rows,
            explicit_provider=explicit_provider,
            override=self._coerce_bool(runtime_meta.get("external_cooldown_override", False), default=False),
            contract_negotiation=contract_negotiation,
            routing_context=runtime_meta,
        )
        if isinstance(route, dict):
            route["auth_preflight"] = auth_preflight
        return route

    def _build_provider_route(
        self,
        *,
        action: str,
        payload: Dict[str, Any],
        providers: List[str],
        blocked_candidates: List[Dict[str, Any]],
        provider_health: List[Dict[str, Any]],
        explicit_provider: str,
        override: bool,
        contract_negotiation: Dict[str, Any] | None = None,
        routing_context: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        if not providers:
            return {"status": "skip"}
        clean_action = str(action or "").strip().lower()
        operation_class = self._operation_class(clean_action)
        negotiation = contract_negotiation if isinstance(contract_negotiation, dict) else {}
        preferred = negotiation.get("preferred_providers", [])
        preferred_providers = [
            self._normalize_provider(str(item))
            for item in (preferred if isinstance(preferred, list) else [])
            if self._normalize_provider(str(item))
        ]
        preferred_rank = {provider: idx for idx, provider in enumerate(preferred_providers)}
        auth_preflight = negotiation.get("auth_preflight", {})
        auth_rows_raw = auth_preflight.get("auth_rows", []) if isinstance(auth_preflight, dict) else []
        auth_rows: Dict[str, Dict[str, Any]] = {}
        if isinstance(auth_rows_raw, list):
            for row in auth_rows_raw:
                if not isinstance(row, dict):
                    continue
                provider_name = self._normalize_provider(str(row.get("provider", "")).strip())
                if not provider_name:
                    continue
                auth_rows[provider_name] = {
                    "status": str(row.get("status", "")).strip().lower(),
                    "ready": bool(row.get("ready", False)),
                    "hard_fail": bool(row.get("hard_fail", False)),
                    "reason": str(row.get("reason", "")).strip().lower(),
                    "scope_status": str(row.get("scope_status", "")).strip().lower(),
                }
        ranked = list(provider_health)
        if not ranked:
            ranked = [
                {
                    "provider": provider,
                    "health_score": 0.45,
                    "cooldown_active": False,
                    "operation_failure_ema": 0.0,
                    "operation_consecutive_failures": 0,
                }
                for provider in providers
            ]
        scored_rows: List[Dict[str, Any]] = []
        for row in ranked:
            if not isinstance(row, dict):
                continue
            provider_name = self._normalize_provider(str(row.get("provider", "")).strip())
            if not provider_name:
                continue
            item = dict(row)
            health_score = self._coerce_float(item.get("health_score", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            operation_failure_ema = self._coerce_float(
                item.get("operation_failure_ema", item.get("action_failure_ema", 0.0)),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            operation_failure_trend_ema = self._coerce_float(
                item.get("operation_failure_trend_ema", item.get("action_failure_trend_ema", 0.0)),
                minimum=-1.0,
                maximum=1.0,
                default=0.0,
            )
            operation_consecutive = self._coerce_int(
                item.get("operation_consecutive_failures", item.get("action_consecutive_failures", 0)),
                minimum=0,
                maximum=1_000_000,
                default=0,
            )
            sla_penalty = self._coerce_float(
                item.get("sla_penalty", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            outage_active = bool(item.get("outage_active", False))
            outage_pressure = self._coerce_float(
                item.get("outage_pressure", item.get("outage_ema", 0.0)),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            auth_row = auth_rows.get(provider_name, {})
            auth_status = str(auth_row.get("status", "unknown")).strip().lower() or "unknown"
            auth_ready = bool(auth_row.get("ready", False))
            auth_hard_fail = bool(auth_row.get("hard_fail", False))
            auth_penalty = 0.0
            if auth_hard_fail or auth_status == "blocked":
                auth_penalty = 1.0
            elif auth_status == "degraded":
                auth_penalty = 0.34
            elif auth_status == "warning" or not auth_ready:
                auth_penalty = 0.18
            operation_penalty = min(
                1.0,
                (operation_failure_ema * 0.78) + min(0.32, float(operation_consecutive) * 0.08),
            )
            if operation_failure_trend_ema > 0.0:
                operation_penalty = min(1.0, operation_penalty + (operation_failure_trend_ema * 0.22))
            elif operation_failure_trend_ema < 0.0:
                operation_penalty = max(0.0, operation_penalty - (abs(operation_failure_trend_ema) * 0.08))
            preference_bonus = 0.0
            if provider_name in preferred_rank:
                rank = preferred_rank[provider_name]
                preference_bonus = max(0.0, self.route_preference_boost * (1.0 - (float(rank) * 0.34)))
            effective_score = health_score + preference_bonus - (operation_penalty * self.route_operation_penalty_weight)
            effective_score -= sla_penalty * 0.18
            effective_score -= auth_penalty * 0.2
            effective_score -= outage_pressure * self.outage_route_penalty_weight
            if bool(item.get("cooldown_active", False)):
                effective_score -= 0.08
            item["provider"] = provider_name
            item["operation_penalty"] = round(operation_penalty, 6)
            item["sla_penalty"] = round(sla_penalty, 6)
            item["outage_active"] = bool(outage_active)
            item["outage_pressure"] = round(outage_pressure, 6)
            item["auth_status"] = auth_status
            item["auth_ready"] = auth_ready
            item["auth_penalty"] = round(auth_penalty, 6)
            item["preference_bonus"] = round(preference_bonus, 6)
            item["effective_score"] = round(max(0.0, min(1.0, effective_score)), 6)
            scored_rows.append(item)
        ranked = scored_rows
        ranked.sort(
            key=lambda row: (
                -self._coerce_float(row.get("effective_score", row.get("health_score", 0.0)), minimum=0.0, maximum=1.0, default=0.0),
                bool(row.get("cooldown_active", False)),
                str(row.get("provider", "")),
            )
        )
        available = [row for row in ranked if not bool(row.get("cooldown_active", False))]
        outage_blocked_rows: List[Dict[str, Any]] = []
        if self.outage_filter_enabled and not override:
            routed_explicit = explicit_provider and explicit_provider not in {"", "auto"}
            if not routed_explicit:
                outage_blocked_rows = [
                    row
                    for row in available
                    if bool(row.get("outage_active", False))
                    and self._coerce_float(
                        row.get("outage_pressure", row.get("outage_ema", 0.0)),
                        minimum=0.0,
                        maximum=1.0,
                        default=0.0,
                    )
                    >= self._coerce_float(
                        row.get("route_block_threshold", self.outage_route_hard_block_threshold),
                        minimum=0.2,
                        maximum=1.0,
                        default=self.outage_route_hard_block_threshold,
                    )
                ]
                if outage_blocked_rows:
                    filtered_available = [row for row in available if row not in outage_blocked_rows]
                    if filtered_available:
                        available = filtered_available
        blocked = [row for row in ranked if bool(row.get("cooldown_active", False))]
        runtime_meta = routing_context if isinstance(routing_context, dict) else {}
        with self._lock:
            mission_policy_snapshot = dict(self._mission_outage_policy)
        mission_profile = self._normalize_mission_outage_profile(
            str(runtime_meta.get("external_route_profile", mission_policy_snapshot.get("profile", "balanced")))
        )

        selected_provider = ""
        strategy = "ranked_health"
        entropy_payload: Dict[str, Any] = {"enabled": bool(self.route_entropy_enabled)}
        if explicit_provider and explicit_provider not in {"", "auto"}:
            selected_provider = explicit_provider
            strategy = "explicit"
        elif available:
            selected_provider = str(available[0].get("provider", "")).strip()
            strategy = "healthiest_available"
            entropy_payload = self._build_route_entropy_payload(
                action=clean_action,
                payload=payload,
                candidates=available,
                mission_profile=mission_profile,
                context=runtime_meta,
            )
            if bool(entropy_payload.get("explore_applied", False)):
                selected_provider = str(entropy_payload.get("selected_provider", "")).strip() or selected_provider
                strategy = "entropy_explore"
        elif override and ranked:
            selected_provider = str(ranked[0].get("provider", "")).strip()
            strategy = "override_cooldown"
        elif ranked:
            selected_provider = str(ranked[0].get("provider", "")).strip()
            strategy = "fallback_ranked"

        args_patch: Dict[str, Any] = {}
        payload_provider = self._normalize_provider(str(payload.get("provider", "")).strip())
        if (
            selected_provider
            and payload_provider in {"", "auto"}
            and strategy in {"healthiest_available", "entropy_explore", "override_cooldown", "fallback_ranked"}
        ):
            args_patch["provider"] = selected_provider

        selected_row = {}
        for row in ranked:
            if str(row.get("provider", "")).strip() == selected_provider:
                selected_row = row
                break
        return {
            "status": "success",
            "action": clean_action,
            "operation_class": operation_class,
            "strategy": strategy,
            "mission_profile": mission_profile,
            "selected_provider": selected_provider,
            "selected_health_score": self._coerce_float(
                selected_row.get("health_score", 0.0) if isinstance(selected_row, dict) else 0.0,
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            ),
            "selected_effective_score": self._coerce_float(
                selected_row.get("effective_score", 0.0) if isinstance(selected_row, dict) else 0.0,
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            ),
            "selected_auth_status": str(selected_row.get("auth_status", "unknown")) if isinstance(selected_row, dict) else "unknown",
            "selected_outage_pressure": self._coerce_float(
                selected_row.get("outage_pressure", selected_row.get("outage_ema", 0.0))
                if isinstance(selected_row, dict)
                else 0.0,
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            ),
            "selected_mission_profile_alignment": self._coerce_float(
                selected_row.get("mission_profile_alignment", 0.0)
                if isinstance(selected_row, dict)
                else 0.0,
                minimum=-1.0,
                maximum=1.0,
                default=0.0,
            ),
            "selected_mission_profile_samples": self._coerce_int(
                selected_row.get("mission_profile_samples", 0)
                if isinstance(selected_row, dict)
                else 0,
                minimum=0,
                maximum=10_000_000,
                default=0,
            ),
            "available_providers": [str(row.get("provider", "")).strip() for row in available if str(row.get("provider", "")).strip()],
            "preferred_providers": preferred_providers,
            "routing_entropy": entropy_payload,
            "blocked_providers": [
                {
                    "provider": str(row.get("provider", "")).strip(),
                    "retry_after_s": round(float(row.get("retry_after_s", 0.0) or 0.0), 3),
                    "health_score": round(
                        self._coerce_float(row.get("health_score", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                }
                for row in blocked
                if str(row.get("provider", "")).strip()
            ],
            "outage_blocked_providers": [
                {
                    "provider": str(row.get("provider", "")).strip(),
                    "outage_pressure": round(
                        self._coerce_float(
                            row.get("outage_pressure", row.get("outage_ema", 0.0)),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        6,
                    ),
                    "outage_active": bool(row.get("outage_active", False)),
                    "route_block_threshold": round(
                        self._coerce_float(
                            row.get("route_block_threshold", self.outage_route_hard_block_threshold),
                            minimum=0.2,
                            maximum=1.0,
                            default=self.outage_route_hard_block_threshold,
                        ),
                        6,
                    ),
                }
                for row in outage_blocked_rows
                if str(row.get("provider", "")).strip()
            ],
            "ranked": [
                {
                    "provider": str(row.get("provider", "")).strip(),
                    "health_score": round(
                        self._coerce_float(row.get("health_score", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "cooldown_active": bool(row.get("cooldown_active", False)),
                    "retry_after_s": round(float(row.get("retry_after_s", 0.0) or 0.0), 3),
                    "failure_ema": round(
                        self._coerce_float(row.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "action_failure_ema": round(
                        self._coerce_float(row.get("action_failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "operation_failure_ema": round(
                        self._coerce_float(row.get("operation_failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "operation_failure_trend_ema": round(
                        self._coerce_float(row.get("operation_failure_trend_ema", 0.0), minimum=-1.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "operation_consecutive_failures": self._coerce_int(
                        row.get("operation_consecutive_failures", 0),
                        minimum=0,
                        maximum=1_000_000,
                        default=0,
                    ),
                    "operation_penalty": round(
                        self._coerce_float(row.get("operation_penalty", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "sla_penalty": round(
                        self._coerce_float(row.get("sla_penalty", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "availability_ema": round(
                        self._coerce_float(row.get("availability_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "latency_ema_ms": round(
                        self._coerce_float(row.get("latency_ema_ms", 0.0), minimum=0.0, maximum=3_600_000.0, default=0.0),
                        3,
                    ),
                    "latency_pressure": round(
                        self._coerce_float(row.get("latency_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "outage_active": bool(row.get("outage_active", False)),
                    "outage_pressure": round(
                        self._coerce_float(
                            row.get("outage_pressure", row.get("outage_ema", 0.0)),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        6,
                    ),
                    "route_block_threshold": round(
                        self._coerce_float(
                            row.get("route_block_threshold", self.outage_route_hard_block_threshold),
                            minimum=0.2,
                            maximum=1.0,
                            default=self.outage_route_hard_block_threshold,
                        ),
                        6,
                    ),
                    "preflight_block_threshold": round(
                        self._coerce_float(
                            row.get("preflight_block_threshold", self.outage_preflight_block_threshold),
                            minimum=0.2,
                            maximum=1.0,
                            default=self.outage_preflight_block_threshold,
                        ),
                        6,
                    ),
                    "mission_profile_alignment": round(
                        self._coerce_float(
                            row.get("mission_profile_alignment", 0.0),
                            minimum=-1.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        6,
                    ),
                    "mission_profile_samples": self._coerce_int(
                        row.get("mission_profile_samples", 0),
                        minimum=0,
                        maximum=10_000_000,
                        default=0,
                    ),
                    "mission_profile_success_rate": round(
                        self._coerce_float(
                            row.get("mission_profile_success_rate", 0.0),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        6,
                    ),
                    "mission_profile_success_ema": round(
                        self._coerce_float(
                            row.get("mission_profile_success_ema", 0.5),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.5,
                        ),
                        6,
                    ),
                    "auth_status": str(row.get("auth_status", "unknown")).strip().lower(),
                    "auth_ready": bool(row.get("auth_ready", False)),
                    "auth_penalty": round(
                        self._coerce_float(row.get("auth_penalty", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "preference_bonus": round(
                        self._coerce_float(row.get("preference_bonus", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "effective_score": round(
                        self._coerce_float(row.get("effective_score", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                }
                for row in ranked[: min(6, len(ranked))]
            ],
            "args_patch": args_patch,
            "cooldown_override": bool(override),
            "blocked_candidates": blocked_candidates,
            "contract_negotiation": negotiation,
        }

    @staticmethod
    def _deterministic_probe(seed: str) -> float:
        text = str(seed or "").strip()
        if not text:
            return 0.5
        digest = hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()[:14]
        try:
            value = int(digest, 16)
        except Exception:
            return 0.5
        maximum = float((16**14) - 1)
        if maximum <= 0.0:
            return 0.5
        probe = float(value) / maximum
        return max(0.0, min(1.0, probe))

    def _build_route_entropy_payload(
        self,
        *,
        action: str,
        payload: Dict[str, Any],
        candidates: List[Dict[str, Any]],
        mission_profile: str,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        rows: List[Dict[str, Any]] = []
        for row in candidates:
            if not isinstance(row, dict):
                continue
            provider = self._normalize_provider(str(row.get("provider", "")).strip())
            if not provider:
                continue
            item = dict(row)
            item["provider"] = provider
            rows.append(item)
        if not rows:
            return {
                "enabled": bool(self.route_entropy_enabled),
                "candidate_count": 0,
                "entropy": 0.0,
                "normalized_entropy": 0.0,
                "explore_probability": 0.0,
                "explore_applied": False,
                "selected_provider": "",
                "selected_probability": 0.0,
                "distribution": [],
            }

        scores: List[float] = [
            self._coerce_float(
                row.get("effective_score", row.get("health_score", 0.0)),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            for row in rows
        ]
        top_score = scores[0] if scores else 0.0
        second_score = scores[1] if len(scores) >= 2 else top_score
        score_gap_top2 = max(0.0, top_score - second_score)
        temperature = self._coerce_float(
            self.route_entropy_temperature,
            minimum=0.15,
            maximum=2.5,
            default=0.72,
        )
        scaled_scores = [score / max(temperature, 0.001) for score in scores]
        max_scaled = max(scaled_scores) if scaled_scores else 0.0
        exp_scores = [math.exp(max(-80.0, min(80.0, value - max_scaled))) for value in scaled_scores]
        exp_total = sum(exp_scores) or 1.0
        probabilities = [max(0.0, min(1.0, value / exp_total)) for value in exp_scores]

        entropy = 0.0
        for probability in probabilities:
            if probability > 0.0:
                entropy -= probability * math.log2(probability)
        candidate_count = len(rows)
        normalized_entropy = (
            entropy / max(1.0, math.log2(float(candidate_count)))
            if candidate_count > 1
            else 0.0
        )
        normalized_entropy = self._coerce_float(normalized_entropy, minimum=0.0, maximum=1.0, default=0.0)

        profile_modifiers = {
            "defensive": -0.08,
            "cautious": -0.04,
            "balanced": 0.02,
            "throughput": 0.12,
        }
        explore_probability = self._coerce_float(
            self.route_entropy_explore_base_probability + (normalized_entropy * 0.22) + profile_modifiers.get(mission_profile, 0.0),
            minimum=0.0,
            maximum=self.route_entropy_explore_max_probability,
            default=self.route_entropy_explore_base_probability,
        )
        if score_gap_top2 > self.route_entropy_score_gap_threshold:
            explore_probability *= 0.5
        explore_probability = self._coerce_float(
            explore_probability,
            minimum=0.0,
            maximum=self.route_entropy_explore_max_probability,
            default=0.0,
        )

        forced = self._coerce_bool(context.get("external_route_entropy_force", False), default=False)
        if forced:
            explore_probability = 1.0

        seed_source = {
            "action": str(action or "").strip().lower(),
            "providers": [str(row.get("provider", "")).strip().lower() for row in rows],
            "payload_provider": str(payload.get("provider", "")).strip().lower(),
            "mission_profile": str(mission_profile or "").strip().lower(),
            "goal_id": str(context.get("goal_id", context.get("__goal_id", ""))).strip(),
            "mission_id": str(context.get("mission_id", context.get("__mission_id", ""))).strip(),
        }
        seed_json = json.dumps(seed_source, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        probe_override = context.get("external_route_entropy_probe")
        if probe_override is None:
            probe = self._deterministic_probe(seed_json)
        else:
            probe = self._coerce_float(probe_override, minimum=0.0, maximum=1.0, default=0.5)

        selected_provider = str(rows[0].get("provider", "")).strip()
        selected_probability = probabilities[0] if probabilities else 0.0
        explore_applied = False
        if (
            bool(self.route_entropy_enabled)
            and candidate_count >= self.route_entropy_min_candidates
            and (forced or probe <= explore_probability)
        ):
            select_probe_override = context.get("external_route_entropy_select_probe")
            if select_probe_override is None:
                select_probe = self._deterministic_probe(f"{seed_json}|select")
            else:
                select_probe = self._coerce_float(select_probe_override, minimum=0.0, maximum=1.0, default=0.5)
            cumulative = 0.0
            selected_index = 0
            for index, probability in enumerate(probabilities):
                cumulative += probability
                if select_probe <= cumulative or index >= (len(probabilities) - 1):
                    selected_index = index
                    break
            selected_provider = str(rows[selected_index].get("provider", "")).strip()
            selected_probability = probabilities[selected_index]
            explore_applied = True

        distribution: List[Dict[str, Any]] = []
        for row, probability in zip(rows[:8], probabilities[:8]):
            distribution.append(
                {
                    "provider": str(row.get("provider", "")).strip(),
                    "probability": round(probability, 6),
                    "effective_score": round(
                        self._coerce_float(row.get("effective_score", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                }
            )
        return {
            "enabled": bool(self.route_entropy_enabled),
            "candidate_count": candidate_count,
            "temperature": round(temperature, 6),
            "entropy": round(entropy, 6),
            "normalized_entropy": round(normalized_entropy, 6),
            "mission_profile": mission_profile,
            "score_gap_top2": round(score_gap_top2, 6),
            "explore_probability": round(explore_probability, 6),
            "probe": round(probe, 6),
            "forced": bool(forced),
            "explore_applied": bool(explore_applied),
            "selected_provider": selected_provider,
            "selected_probability": round(selected_probability, 6),
            "distribution": distribution,
        }

    def _mission_outage_pressure(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        payload = metadata if isinstance(metadata, dict) else {}
        trend_payload = payload.get("mission_trend_feedback", {})
        trend = trend_payload if isinstance(trend_payload, dict) else {}
        mission_feedback_raw = payload.get("mission_feedback", {})
        mission_feedback = mission_feedback_raw if isinstance(mission_feedback_raw, dict) else {}
        retry_contract_mode = str(payload.get("__external_retry_contract_mode", "")).strip().lower()
        retry_contract_risk = self._coerce_float(
            payload.get("__external_retry_contract_risk", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        retry_contract_cooldown_s = self._coerce_float(
            payload.get("__external_retry_contract_cooldown_s", 0.0),
            minimum=0.0,
            maximum=float(self.max_cooldown_s),
            default=0.0,
        )
        trend_pressure = self._coerce_float(
            trend.get("trend_pressure", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        mode = str(trend.get("mode", "")).strip().lower()
        risk_trend = str(trend.get("risk_trend", "")).strip().lower()
        quality_trend = str(trend.get("quality_trend", "")).strip().lower()
        mission_risk_level = str(mission_feedback.get("risk_level", "")).strip().lower()
        mission_quality_level = str(mission_feedback.get("quality_level", "")).strip().lower()

        worsening = bool(
            mode == "worsening"
            or risk_trend == "worsening"
            or quality_trend == "degrading"
            or mission_risk_level == "high"
            or mission_quality_level == "low"
        )
        improving = bool(
            mode == "improving"
            or risk_trend == "improving"
            or quality_trend == "improving"
        )
        if retry_contract_mode in {"adaptive_backoff", "stabilize"} and retry_contract_risk >= 0.35:
            worsening = True
        if retry_contract_mode == "light_retry" and retry_contract_risk <= 0.12:
            improving = True
        pressure = trend_pressure
        if worsening:
            pressure = min(1.0, pressure + 0.14)
        if mission_risk_level == "high":
            pressure = min(1.0, pressure + 0.1)
        if mission_quality_level == "low":
            pressure = min(1.0, pressure + 0.08)
        if retry_contract_mode:
            pressure = min(1.0, pressure + (retry_contract_risk * 0.22))
            if retry_contract_cooldown_s > 0.0:
                cooldown_pressure = min(1.0, retry_contract_cooldown_s / max(1.0, float(self.max_cooldown_s)))
                pressure = min(1.0, pressure + (cooldown_pressure * 0.12))
        if improving:
            pressure = max(0.0, pressure - 0.08)
        return {
            "pressure": pressure,
            "worsening": worsening,
            "improving": improving,
            "mode": mode,
        }

    def _smooth_outage_policy_bias(self, *, previous: float, target: float) -> float:
        prev = self._coerce_float(
            previous,
            minimum=self.outage_policy_bias_min,
            maximum=self.outage_policy_bias_max,
            default=0.0,
        )
        tgt = self._coerce_float(
            target,
            minimum=self.outage_policy_bias_min,
            maximum=self.outage_policy_bias_max,
            default=0.0,
        )
        decay = self._coerce_float(
            self.outage_policy_bias_decay,
            minimum=0.3,
            maximum=0.99,
            default=0.82,
        )
        value = (prev * decay) + (tgt * (1.0 - decay))
        return self._coerce_float(
            value,
            minimum=self.outage_policy_bias_min,
            maximum=self.outage_policy_bias_max,
            default=0.0,
        )

    def _outage_policy_thresholds(
        self,
        *,
        state: Dict[str, Any],
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        state_payload = state if isinstance(state, dict) else {}
        runtime_meta = metadata if isinstance(metadata, dict) else {}
        state_bias = self._coerce_float(
            state_payload.get("outage_policy_bias", 0.0),
            minimum=self.outage_policy_bias_min,
            maximum=self.outage_policy_bias_max,
            default=0.0,
        )
        mission_pressure_row = self._mission_outage_pressure(runtime_meta)
        mission_pressure = self._coerce_float(
            mission_pressure_row.get("pressure", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        worsening = bool(mission_pressure_row.get("worsening", False))
        improving = bool(mission_pressure_row.get("improving", False))
        runtime_bias = 0.0
        if worsening:
            runtime_bias += mission_pressure * 0.28
        elif improving:
            runtime_bias -= mission_pressure * 0.18
        mission_policy_row = self._mission_outage_policy if isinstance(self._mission_outage_policy, dict) else {}
        capability_bias_map = self._load_mission_capability_bias(mission_policy_row.get("capability_bias", {}))
        capability_hint = self._normalize_mission_capability(
            str(
                runtime_meta.get(
                    "__external_capability",
                    self._action_domain(str(runtime_meta.get("__external_action", ""))),
                )
            )
        )
        capability_row = capability_bias_map.get(capability_hint, {})
        mission_capability_bias = self._coerce_float(
            capability_row.get("bias", 0.0) if isinstance(capability_row, dict) else 0.0,
            minimum=self.mission_outage_bias_min,
            maximum=self.mission_outage_bias_max,
            default=0.0,
        )
        mission_capability_pressure = self._coerce_float(
            capability_row.get("pressure_ema", 0.0) if isinstance(capability_row, dict) else 0.0,
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        mission_capability_samples = self._coerce_int(
            capability_row.get("samples", 0) if isinstance(capability_row, dict) else 0,
            minimum=0,
            maximum=10_000_000,
            default=0,
        )
        mission_global_bias = self._coerce_float(
            mission_policy_row.get("bias", 0.0),
            minimum=self.mission_outage_bias_min,
            maximum=self.mission_outage_bias_max,
            default=0.0,
        )
        mission_profile = self._normalize_mission_outage_profile(str(mission_policy_row.get("profile", "balanced")))
        profile_adjustments = self._MISSION_OUTAGE_PROFILE_ADJUSTMENTS.get(
            mission_profile,
            self._MISSION_OUTAGE_PROFILE_ADJUSTMENTS["balanced"],
        )
        profile_bias_gain = self._coerce_float(
            profile_adjustments.get("bias_gain", 0.0),
            minimum=-0.3,
            maximum=0.5,
            default=0.0,
        )
        capability_bias_adjustment = mission_capability_bias * 0.72
        if worsening:
            capability_bias_adjustment += mission_capability_pressure * 0.1
        elif improving:
            capability_bias_adjustment -= mission_capability_pressure * 0.06
        combined_bias = self._coerce_float(
            state_bias + runtime_bias + (mission_global_bias * 0.78) + (profile_bias_gain * 0.7) + capability_bias_adjustment,
            minimum=self.outage_policy_bias_min,
            maximum=self.outage_policy_bias_max,
            default=state_bias,
        )
        trip_delta = self._coerce_float(
            profile_adjustments.get("trip_delta", 0.0),
            minimum=-0.1,
            maximum=0.1,
            default=0.0,
        )
        recover_delta = self._coerce_float(
            profile_adjustments.get("recover_delta", 0.0),
            minimum=-0.1,
            maximum=0.1,
            default=0.0,
        )
        route_delta = self._coerce_float(
            profile_adjustments.get("route_block_delta", 0.0),
            minimum=-0.16,
            maximum=0.16,
            default=0.0,
        )
        preflight_delta = self._coerce_float(
            profile_adjustments.get("preflight_block_delta", 0.0),
            minimum=-0.16,
            maximum=0.16,
            default=0.0,
        )
        trip = self._coerce_float(
            self.outage_trip_threshold - (combined_bias * 0.18) + trip_delta,
            minimum=0.15,
            maximum=0.98,
            default=self.outage_trip_threshold,
        )
        recover = self._coerce_float(
            self.outage_recover_threshold - (combined_bias * 0.1) + recover_delta,
            minimum=0.05,
            maximum=max(0.05, trip - 0.03),
            default=min(self.outage_recover_threshold, trip - 0.03),
        )
        route_block = self._coerce_float(
            self.outage_route_hard_block_threshold - (combined_bias * 0.22) + route_delta,
            minimum=0.2,
            maximum=1.0,
            default=self.outage_route_hard_block_threshold,
        )
        preflight_block = self._coerce_float(
            self.outage_preflight_block_threshold - (combined_bias * 0.2) + preflight_delta,
            minimum=0.2,
            maximum=1.0,
            default=self.outage_preflight_block_threshold,
        )
        return {
            "bias": combined_bias,
            "trip_threshold": trip,
            "recover_threshold": recover,
            "route_block_threshold": route_block,
            "preflight_block_threshold": preflight_block,
            "mission_pressure": mission_pressure,
            "mission_global_bias": mission_global_bias,
            "mission_profile": mission_profile,
            "mission_capability": capability_hint,
            "mission_capability_bias": mission_capability_bias,
            "mission_capability_pressure": mission_capability_pressure,
            "mission_capability_samples": mission_capability_samples,
            "mission_profile_adjustment": {
                "bias_gain": profile_bias_gain,
                "trip_delta": trip_delta,
                "recover_delta": recover_delta,
                "route_block_delta": route_delta,
                "preflight_block_delta": preflight_delta,
            },
        }

    def _provider_health_row(
        self,
        *,
        provider: str,
        action: str,
        state: Dict[str, Any],
        now_ts: float,
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        clean_provider = self._normalize_provider(provider)
        if not clean_provider:
            return {}
        payload = state if isinstance(state, dict) else {}
        failure_ema = self._coerce_float(payload.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        failure_trend_ema = self._coerce_float(
            payload.get("failure_trend_ema", 0.0),
            minimum=-1.0,
            maximum=1.0,
            default=0.0,
        )
        availability_ema = self._coerce_float(
            payload.get("availability_ema", 0.55),
            minimum=0.0,
            maximum=1.0,
            default=0.55,
        )
        latency_ema_ms = self._coerce_float(
            payload.get("latency_ema_ms", 0.0),
            minimum=0.0,
            maximum=3_600_000.0,
            default=0.0,
        )
        outage_ema = self._coerce_float(
            payload.get("outage_ema", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        outage_streak = self._coerce_int(payload.get("outage_streak", 0), minimum=0, maximum=1_000_000, default=0)
        outage_active = self._coerce_bool(payload.get("outage_active", False), default=False)
        cooldown_bias = self._coerce_float(
            payload.get("cooldown_bias", 1.0),
            minimum=self.cooldown_bias_min,
            maximum=self.cooldown_bias_max,
            default=1.0,
        )
        consecutive_failures = self._coerce_int(payload.get("consecutive_failures", 0), minimum=0, maximum=1_000_000, default=0)
        samples = self._coerce_int(payload.get("samples", 0), minimum=0, maximum=10_000_000, default=0)
        successes = self._coerce_int(payload.get("successes", 0), minimum=0, maximum=10_000_000, default=0)
        success_ratio = float(successes) / max(1.0, float(samples))
        action_state = self._action_state_for(state=payload, action=action)
        operation_class = self._operation_class(action)
        operation_state = self._operation_state_for(state=payload, operation_class=operation_class)
        action_failure_ema = self._coerce_float(
            action_state.get("failure_ema", failure_ema),
            minimum=0.0,
            maximum=1.0,
            default=failure_ema,
        )
        action_failure_trend_ema = self._coerce_float(
            action_state.get("failure_trend_ema", failure_trend_ema),
            minimum=-1.0,
            maximum=1.0,
            default=failure_trend_ema,
        )
        action_consecutive = self._coerce_int(
            action_state.get("consecutive_failures", consecutive_failures),
            minimum=0,
            maximum=1_000_000,
            default=consecutive_failures,
        )
        operation_failure_ema = self._coerce_float(
            operation_state.get("failure_ema", action_failure_ema),
            minimum=0.0,
            maximum=1.0,
            default=action_failure_ema,
        )
        operation_failure_trend_ema = self._coerce_float(
            operation_state.get("failure_trend_ema", action_failure_trend_ema),
            minimum=-1.0,
            maximum=1.0,
            default=action_failure_trend_ema,
        )
        operation_latency_ema_ms = self._coerce_float(
            operation_state.get("latency_ema_ms", latency_ema_ms),
            minimum=0.0,
            maximum=3_600_000.0,
            default=latency_ema_ms,
        )
        operation_consecutive = self._coerce_int(
            operation_state.get("consecutive_failures", action_consecutive),
            minimum=0,
            maximum=1_000_000,
            default=action_consecutive,
        )
        operation_cooldown_rows = payload.get("operation_cooldown_bias", {})
        operation_cooldown_map = operation_cooldown_rows if isinstance(operation_cooldown_rows, dict) else {}
        operation_cooldown_bias = self._coerce_float(
            operation_cooldown_map.get(operation_class, 1.0),
            minimum=self.cooldown_bias_min,
            maximum=self.cooldown_bias_max,
            default=1.0,
        )

        cooldown_until_ts = self._to_timestamp(payload.get("cooldown_until", ""))
        cooldown_active = cooldown_until_ts > now_ts
        retry_after_s = max(0.0, cooldown_until_ts - now_ts) if cooldown_active else 0.0
        target_latency_ms = {
            "healthcheck": 600.0,
            "read": 1800.0,
            "write": 2600.0,
            "mutate": 3200.0,
            "auth": 2500.0,
            "maintenance": 1600.0,
            "default": 2200.0,
        }.get(operation_class, 2200.0)
        latency_pressure = min(1.0, max(0.0, (operation_latency_ema_ms / max(1.0, target_latency_ms)) - 1.0))
        availability_pressure = max(0.0, min(1.0, 1.0 - availability_ema))
        sla_penalty = min(1.0, (availability_pressure * 0.72) + (latency_pressure * 0.45))
        outage_pressure = min(1.0, max(outage_ema, min(1.0, float(outage_streak) / 7.0)))
        outage_policy = self._outage_policy_thresholds(state=payload, metadata=metadata)
        outage_policy_bias = self._coerce_float(
            outage_policy.get("bias", 0.0),
            minimum=self.outage_policy_bias_min,
            maximum=self.outage_policy_bias_max,
            default=0.0,
        )
        route_block_threshold = self._coerce_float(
            outage_policy.get("route_block_threshold", self.outage_route_hard_block_threshold),
            minimum=0.2,
            maximum=1.0,
            default=self.outage_route_hard_block_threshold,
        )
        preflight_block_threshold = self._coerce_float(
            outage_policy.get("preflight_block_threshold", self.outage_preflight_block_threshold),
            minimum=0.2,
            maximum=1.0,
            default=self.outage_preflight_block_threshold,
        )
        mission_profile = self._normalize_mission_outage_profile(
            str(outage_policy.get("mission_profile", "balanced"))
        )
        profile_performance_rows = payload.get("profile_performance", {})
        profile_performance_map = (
            profile_performance_rows if isinstance(profile_performance_rows, dict) else {}
        )
        profile_row_raw = profile_performance_map.get(mission_profile, {})
        profile_row = profile_row_raw if isinstance(profile_row_raw, dict) else {}
        profile_samples = self._coerce_int(
            profile_row.get("samples", 0),
            minimum=0,
            maximum=10_000_000,
            default=0,
        )
        profile_success_rate = self._coerce_float(
            profile_row.get("success_rate", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        profile_success_ema = self._coerce_float(
            profile_row.get("success_ema", 0.5),
            minimum=0.0,
            maximum=1.0,
            default=0.5,
        )
        mission_profile_alignment = 0.0
        if (
            self.profile_performance_enabled
            and profile_samples >= self.profile_performance_min_samples
        ):
            centered_rate = (profile_success_rate - 0.5) * 2.0
            centered_ema = (profile_success_ema - 0.5) * 2.0
            alignment_signal = (centered_rate * 0.62) + (centered_ema * 0.38)
            if alignment_signal >= 0.0:
                mission_profile_alignment = min(
                    self.profile_performance_bonus_weight,
                    alignment_signal * self.profile_performance_bonus_weight,
                )
            else:
                mission_profile_alignment = max(
                    -self.profile_performance_penalty_weight,
                    alignment_signal * self.profile_performance_penalty_weight,
                )
        sample_confidence = min(1.0, float(samples) / 20.0)
        health_score = 0.52
        health_score += (success_ratio * 0.22)
        health_score += (availability_ema - 0.5) * 0.16
        health_score -= (failure_ema * 0.26)
        health_score -= (action_failure_ema * 0.22)
        health_score -= (operation_failure_ema * 0.18)
        health_score -= (sla_penalty * 0.14)
        health_score -= max(0.0, (cooldown_bias - 1.0)) * 0.09
        health_score -= max(0.0, (operation_cooldown_bias - 1.0)) * 0.07
        health_score -= outage_pressure * (0.16 if outage_active else 0.08)
        health_score -= max(0.0, outage_policy_bias) * 0.08
        health_score += max(0.0, -outage_policy_bias) * 0.03
        health_score -= max(0.0, failure_trend_ema) * 0.12
        health_score -= max(0.0, action_failure_trend_ema) * 0.1
        health_score -= max(0.0, operation_failure_trend_ema) * 0.08
        health_score += max(0.0, -failure_trend_ema) * 0.04
        health_score -= min(0.28, float(action_consecutive) * 0.05)
        health_score -= min(0.2, float(operation_consecutive) * 0.035)
        health_score += min(0.08, sample_confidence * 0.08)
        health_score += mission_profile_alignment

        last_success_at = str(payload.get("last_success_at", "")).strip()
        if last_success_at:
            success_ts = self._to_timestamp(last_success_at)
            if success_ts > 0 and (now_ts - success_ts) <= float(self.recent_success_window_s):
                health_score += 0.06
        last_category = str(action_state.get("last_category", payload.get("last_category", ""))).strip().lower()
        if last_category == "auth":
            health_score -= 0.08
        elif last_category == "rate_limited":
            health_score -= 0.04
        elif last_category == "non_retryable":
            health_score -= 0.05
        if cooldown_active:
            health_score -= 0.22
        health_score = max(0.0, min(1.0, health_score))

        return {
            "provider": clean_provider,
            "operation_class": operation_class,
            "health_score": round(health_score, 6),
            "cooldown_active": bool(cooldown_active),
            "retry_after_s": round(retry_after_s, 3),
            "failure_ema": round(failure_ema, 6),
            "failure_trend_ema": round(failure_trend_ema, 6),
            "outage_ema": round(outage_ema, 6),
            "outage_streak": int(outage_streak),
            "outage_active": bool(outage_active),
            "outage_pressure": round(outage_pressure, 6),
            "outage_policy_bias": round(outage_policy_bias, 6),
            "outage_mission_pressure": round(
                self._coerce_float(outage_policy.get("mission_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                6,
            ),
            "outage_mission_global_bias": round(
                self._coerce_float(
                    outage_policy.get("mission_global_bias", 0.0),
                    minimum=self.mission_outage_bias_min,
                    maximum=self.mission_outage_bias_max,
                    default=0.0,
                ),
                6,
            ),
            "outage_mission_profile": self._normalize_mission_outage_profile(
                str(outage_policy.get("mission_profile", "balanced"))
            ),
            "outage_mission_profile_adjustment": outage_policy.get("mission_profile_adjustment", {}),
            "mission_profile_alignment": round(mission_profile_alignment, 6),
            "mission_profile_samples": int(profile_samples),
            "mission_profile_success_rate": round(profile_success_rate, 6),
            "mission_profile_success_ema": round(profile_success_ema, 6),
            "trip_threshold": round(
                self._coerce_float(outage_policy.get("trip_threshold", self.outage_trip_threshold), minimum=0.15, maximum=0.98, default=self.outage_trip_threshold),
                6,
            ),
            "recover_threshold": round(
                self._coerce_float(outage_policy.get("recover_threshold", self.outage_recover_threshold), minimum=0.05, maximum=0.95, default=self.outage_recover_threshold),
                6,
            ),
            "route_block_threshold": round(route_block_threshold, 6),
            "preflight_block_threshold": round(preflight_block_threshold, 6),
            "cooldown_bias": round(cooldown_bias, 6),
            "operation_cooldown_bias": round(operation_cooldown_bias, 6),
            "availability_ema": round(availability_ema, 6),
            "latency_ema_ms": round(latency_ema_ms, 3),
            "consecutive_failures": int(consecutive_failures),
            "action_failure_ema": round(action_failure_ema, 6),
            "action_failure_trend_ema": round(action_failure_trend_ema, 6),
            "action_consecutive_failures": int(action_consecutive),
            "operation_failure_ema": round(operation_failure_ema, 6),
            "operation_failure_trend_ema": round(operation_failure_trend_ema, 6),
            "operation_latency_ema_ms": round(operation_latency_ema_ms, 3),
            "operation_consecutive_failures": int(operation_consecutive),
            "sla_penalty": round(sla_penalty, 6),
            "latency_pressure": round(latency_pressure, 6),
            "last_category": last_category,
            "samples": samples,
            "success_ratio": round(success_ratio, 6),
        }

    @staticmethod
    def _action_state_for(*, state: Dict[str, Any], action: str) -> Dict[str, Any]:
        action_rows = state.get("action_stats", {})
        if not isinstance(action_rows, dict):
            return {}
        clean_action = str(action or "").strip().lower()
        if not clean_action:
            return {}
        row = action_rows.get(clean_action, {})
        return dict(row) if isinstance(row, dict) else {}

    @classmethod
    def _operation_class(cls, action: str) -> str:
        clean_action = str(action or "").strip().lower()
        if not clean_action:
            return "default"
        return str(cls._ACTION_OPERATION_CLASS.get(clean_action, "default")).strip().lower() or "default"

    @staticmethod
    def _operation_state_for(*, state: Dict[str, Any], operation_class: str) -> Dict[str, Any]:
        operation_rows = state.get("operation_stats", {})
        if not isinstance(operation_rows, dict):
            return {}
        clean_operation = str(operation_class or "").strip().lower()
        if not clean_operation:
            return {}
        row = operation_rows.get(clean_operation, {})
        return dict(row) if isinstance(row, dict) else {}

    def _build_retry_contract(
        self,
        *,
        action: str,
        provider: str,
        operation_class: str,
        category: str,
        severity: float,
        route: Dict[str, Any],
        retry_hint: Dict[str, Any],
        state: Dict[str, Any],
        action_state: Dict[str, Any],
        health_pressure: float,
        effective_pressure: float,
        outage_pressure: float,
        outage_active: bool,
        cooldown_bias_pressure: float,
    ) -> Dict[str, Any]:
        clean_action = str(action or "").strip().lower()
        clean_provider = self._normalize_provider(str(provider)) or "default"
        clean_operation = str(operation_class or "").strip().lower() or "default"
        clean_category = str(category or "").strip().lower() or "unknown"
        hint_row = retry_hint if isinstance(retry_hint, dict) else {}
        route_row = route if isinstance(route, dict) else {}
        state_row = state if isinstance(state, dict) else {}
        action_row = action_state if isinstance(action_state, dict) else {}

        base_delay = self._coerce_float(hint_row.get("base_delay_s", 0.0), minimum=0.0, maximum=900.0, default=0.0)
        max_delay = self._coerce_float(hint_row.get("max_delay_s", 0.0), minimum=0.0, maximum=3_600.0, default=0.0)
        multiplier = self._coerce_float(hint_row.get("multiplier", 1.5), minimum=1.0, maximum=4.0, default=1.5)
        jitter = self._coerce_float(hint_row.get("jitter_s", 0.0), minimum=0.0, maximum=5.0, default=0.0)

        operation_factor = self._coerce_float(
            self._OPERATION_RETRY_FACTOR.get(clean_operation, self._OPERATION_RETRY_FACTOR["default"]),
            minimum=0.45,
            maximum=2.0,
            default=1.0,
        )
        provider_table = self._PROVIDER_COOLDOWN_BASE.get(clean_provider, self._PROVIDER_COOLDOWN_BASE["default"])
        provider_category_base = self._coerce_int(
            provider_table.get(clean_category, provider_table.get("unknown", 16)),
            minimum=4,
            maximum=self.max_cooldown_s,
            default=16,
        )
        min_delay = max(
            0.2,
            min(
                base_delay if base_delay > 0.0 else 0.6,
                (float(provider_category_base) * operation_factor) / 44.0,
            ),
        )
        max_delay_cap = max(
            max_delay if max_delay > 0.0 else 2.0,
            min(float(self.max_cooldown_s), max_delay + (base_delay * 1.2) + 1.0),
        )

        severity_value = self._coerce_float(severity, minimum=0.0, maximum=1.0, default=0.0)
        risk_score = min(
            1.0,
            max(
                0.0,
                (severity_value * 0.56)
                + (self._coerce_float(health_pressure, minimum=0.0, maximum=1.0, default=0.0) * 0.18)
                + (self._coerce_float(effective_pressure, minimum=0.0, maximum=1.0, default=0.0) * 0.14)
                + (self._coerce_float(outage_pressure, minimum=0.0, maximum=1.0, default=0.0) * 0.16)
                + (self._coerce_float(cooldown_bias_pressure, minimum=0.0, maximum=1.0, default=0.0) * 0.12),
            ),
        )
        if clean_category in {"auth", "rate_limited"}:
            risk_score = min(1.0, risk_score + 0.08)
        if clean_category == "non_retryable":
            risk_score = min(1.0, risk_score + 0.2)

        mode = "light_retry"
        if clean_category == "non_retryable":
            mode = "abort"
        elif outage_active or severity_value >= 0.82 or risk_score >= 0.82:
            mode = "stabilize"
        elif clean_category in {"auth", "rate_limited"} or severity_value >= 0.58:
            mode = "adaptive_backoff"
        elif severity_value >= 0.32:
            mode = "probe_then_backoff"

        attempt_base = {
            "healthcheck": 5,
            "read": 6,
            "write": 5,
            "mutate": 4,
            "auth": 4,
            "maintenance": 4,
            "default": 4,
        }.get(clean_operation, 4)
        attempt_penalty = int(round(risk_score * 2.6))
        max_attempts = attempt_base - attempt_penalty
        if clean_category in {"timeout", "transient"} and mode not in {"stabilize", "abort"}:
            max_attempts += 1
        if clean_category in {"auth", "rate_limited"}:
            max_attempts = min(max_attempts, 4)
        if outage_active:
            max_attempts = min(max_attempts, 3)
        if clean_category == "non_retryable":
            max_attempts = 1
        max_attempts = self._coerce_int(max_attempts, minimum=1, maximum=8, default=1)

        timeout_base = {
            "healthcheck": 18,
            "read": 24,
            "write": 34,
            "mutate": 42,
            "auth": 28,
            "maintenance": 26,
            "default": 30,
        }.get(clean_operation, 30)
        timeout_pressure = min(1.0, max(0.0, (risk_score * 0.7) + (outage_pressure * 0.3)))
        timeout_s = int(round(timeout_base * (1.0 + (timeout_pressure * 0.55))))
        timeout_s = self._coerce_int(timeout_s, minimum=12, maximum=180, default=timeout_base)

        provider_consecutive = self._coerce_int(
            action_row.get("consecutive_failures", state_row.get("consecutive_failures", 0)),
            minimum=0,
            maximum=1_000_000,
            default=0,
        )
        provider_failure_ema = self._coerce_float(
            action_row.get("failure_ema", state_row.get("failure_ema", 0.0)),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        provider_trend_ema = self._coerce_float(
            action_row.get("failure_trend_ema", state_row.get("failure_trend_ema", 0.0)),
            minimum=-1.0,
            maximum=1.0,
            default=0.0,
        )
        operation_state = self._operation_state_for(state=state_row, operation_class=clean_operation)
        operation_trend_ema = self._coerce_float(
            operation_state.get("failure_trend_ema", 0.0),
            minimum=-1.0,
            maximum=1.0,
            default=0.0,
        )
        cooldown_s = self._cooldown_seconds(
            provider=clean_provider,
            category=clean_category,
            operation_class=clean_operation,
            consecutive_failures=provider_consecutive,
            failure_ema=provider_failure_ema,
            trend_ema=provider_trend_ema,
            operation_trend_ema=operation_trend_ema,
            cooldown_bias=self._coerce_float(
                state_row.get("cooldown_bias", 1.0),
                minimum=self.cooldown_bias_min,
                maximum=self.cooldown_bias_max,
                default=1.0,
            ),
        )

        selected_health = self._coerce_float(
            route_row.get("selected_health_score", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        selected_effective = self._coerce_float(
            route_row.get("selected_effective_score", selected_health),
            minimum=0.0,
            maximum=1.0,
            default=selected_health,
        )
        fingerprint = {
            "provider": clean_provider,
            "operation_class": clean_operation,
            "category": clean_category,
            "mode": mode,
            "risk": round(risk_score, 4),
            "severity": round(severity_value, 4),
            "attempts": int(max_attempts),
        }
        contract_id = self._diagnostic_id(
            stage="retry_contract",
            action=clean_action,
            code=f"retry_{mode}",
            fingerprint=fingerprint,
        )
        return {
            "version": "1.0",
            "contract_id": contract_id,
            "action": clean_action,
            "provider": clean_provider,
            "operation_class": clean_operation,
            "category": clean_category,
            "mode": mode,
            "risk_score": round(risk_score, 6),
            "severity": round(severity_value, 6),
            "route_strategy": str(route_row.get("strategy", "")).strip().lower(),
            "mission_profile": str(route_row.get("mission_profile", "")).strip().lower(),
            "timing": {
                "min_delay_s": round(min_delay, 3),
                "base_delay_s": round(base_delay, 3),
                "max_delay_s": round(max_delay, 3),
                "max_delay_cap_s": round(max_delay_cap, 3),
                "multiplier": round(multiplier, 3),
                "jitter_s": round(jitter, 3),
            },
            "budget": {
                "max_attempts": int(max_attempts),
                "suggested_timeout_s": int(timeout_s),
                "cooldown_recommendation_s": int(cooldown_s),
            },
            "pressures": {
                "health": round(self._coerce_float(health_pressure, minimum=0.0, maximum=1.0, default=0.0), 6),
                "effective": round(self._coerce_float(effective_pressure, minimum=0.0, maximum=1.0, default=0.0), 6),
                "outage": round(self._coerce_float(outage_pressure, minimum=0.0, maximum=1.0, default=0.0), 6),
                "cooldown_bias": round(self._coerce_float(cooldown_bias_pressure, minimum=0.0, maximum=1.0, default=0.0), 6),
            },
            "selected_scores": {
                "health": round(selected_health, 6),
                "effective": round(selected_effective, 6),
            },
            "outage": {
                "active": bool(outage_active),
                "pressure": round(self._coerce_float(outage_pressure, minimum=0.0, maximum=1.0, default=0.0), 6),
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    def _negotiate_provider_contract(
        self,
        *,
        action: str,
        payload: Dict[str, Any],
        providers: List[str],
        explicit_provider: str,
    ) -> Dict[str, Any]:
        clean_action = str(action or "").strip().lower()
        normalized: List[str] = []
        for provider in providers:
            clean_provider = self._normalize_provider(str(provider))
            if clean_provider and clean_provider not in normalized:
                normalized.append(clean_provider)
        rules_raw = self._ACTION_PROVIDER_RULES.get(clean_action, {})
        rules = rules_raw if isinstance(rules_raw, dict) else {}
        allowed_rule = rules.get("allow", [])
        preferred_rule = rules.get("prefer", [])
        allowed = [
            self._normalize_provider(str(item))
            for item in (allowed_rule if isinstance(allowed_rule, list) else [])
            if self._normalize_provider(str(item))
        ]
        preferred = [
            self._normalize_provider(str(item))
            for item in (preferred_rule if isinstance(preferred_rule, list) else [])
            if self._normalize_provider(str(item))
        ]
        dropped: List[Dict[str, Any]] = []
        if allowed:
            filtered = []
            for provider in normalized:
                if provider in allowed:
                    filtered.append(provider)
                else:
                    dropped.append({"provider": provider, "reason": "unsupported_provider_for_action"})
            normalized = filtered
        if explicit_provider and explicit_provider not in {"", "auto"} and allowed and explicit_provider not in allowed:
            return {
                "status": "error",
                "action": clean_action,
                "operation_class": self._operation_class(clean_action),
                "message": (
                    f"Provider '{explicit_provider}' is not supported for action '{clean_action}'. "
                    f"Allowed: {', '.join(allowed)}."
                ),
                "requested_provider": explicit_provider,
                "allowed_providers": allowed,
                "preferred_providers": preferred,
                "providers": normalized,
                "dropped_providers": dropped,
            }
        if self.preflight_provider_contract_strict and not normalized:
            message = f"No providers satisfy contract for action '{clean_action}'."
            if allowed:
                message = (
                    f"No providers satisfy contract for action '{clean_action}'. "
                    f"Allowed: {', '.join(allowed)}."
                )
            return {
                "status": "error",
                "action": clean_action,
                "operation_class": self._operation_class(clean_action),
                "message": message,
                "requested_provider": explicit_provider,
                "allowed_providers": allowed,
                "preferred_providers": preferred,
                "providers": [],
                "dropped_providers": dropped,
            }
        return {
            "status": "success",
            "action": clean_action,
            "operation_class": self._operation_class(clean_action),
            "requested_provider": explicit_provider,
            "providers": normalized,
            "allowed_providers": allowed,
            "preferred_providers": preferred,
            "dropped_providers": dropped,
            "strict": bool(self.preflight_provider_contract_strict),
            "payload_provider": self._normalize_provider(str(payload.get("provider", "")).strip()),
        }

    def _negotiate_provider_capability_contract(
        self,
        *,
        action: str,
        payload: Dict[str, Any],
        providers: List[str],
        explicit_provider: str,
        metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        clean_action = str(action or "").strip().lower()
        operation_class = self._operation_class(clean_action)
        capability_hint = self._normalize_mission_capability(
            str(metadata.get("__external_capability", "")).strip() or self._action_domain(clean_action)
        )
        strict = bool(getattr(self, "preflight_provider_capability_contract_strict", True))
        runtime_enabled = bool(getattr(self, "preflight_provider_capability_runtime_enabled", True))
        runtime_caps_raw = metadata.get("external_provider_capabilities", {})
        runtime_caps = runtime_caps_raw if isinstance(runtime_caps_raw, dict) else {}
        runtime_caps_map: Dict[str, Dict[str, Any]] = {}
        if isinstance(runtime_caps, dict):
            for provider_name, row in runtime_caps.items():
                normalized_provider = self._normalize_provider(str(provider_name))
                if not normalized_provider or not isinstance(row, dict):
                    continue
                runtime_caps_map[normalized_provider] = dict(row)

        normalized: List[str] = []
        for provider in providers:
            clean_provider = self._normalize_provider(str(provider))
            if clean_provider and clean_provider not in normalized:
                normalized.append(clean_provider)
        dropped: List[Dict[str, Any]] = []
        allowed: List[str] = []

        def _drop(
            provider_name: str,
            *,
            reason: str,
            details: Dict[str, Any] | None = None,
        ) -> None:
            row: Dict[str, Any] = {
                "provider": provider_name,
                "reason": str(reason or "").strip().lower(),
                "source": "capability_contract",
            }
            if isinstance(details, dict) and details:
                row["details"] = details
            dropped.append(row)

        for provider_name in normalized:
            static_row = self._PROVIDER_CAPABILITY_CONTRACTS.get(provider_name, {})
            static_caps = (
                [
                    self._normalize_mission_capability(str(item))
                    for item in static_row.get("capabilities", [])
                    if self._normalize_mission_capability(str(item))
                ]
                if isinstance(static_row, dict)
                else []
            )
            static_ops = (
                [str(item).strip().lower() for item in static_row.get("operation_classes", []) if str(item).strip()]
                if isinstance(static_row, dict)
                else []
            )
            static_capability_ok = bool(
                capability_hint in {"general", "external"}
                or not static_caps
                or capability_hint in static_caps
                or "external" in static_caps
            )
            static_operation_ok = bool(not static_ops or operation_class in static_ops)
            if not static_capability_ok or not static_operation_ok:
                _drop(
                    provider_name,
                    reason="provider_capability_unsupported",
                    details={
                        "required_capability": capability_hint,
                        "required_operation_class": operation_class,
                        "supported_capabilities": static_caps[:12],
                        "supported_operation_classes": static_ops[:12],
                    },
                )
                continue

            if runtime_enabled:
                runtime_row_raw = runtime_caps_map.get(provider_name, {})
                runtime_row = runtime_row_raw if isinstance(runtime_row_raw, dict) else {}
                if runtime_row:
                    runtime_enabled_flag = self._coerce_bool(runtime_row.get("enabled", True), default=True)
                    if not runtime_enabled_flag:
                        _drop(
                            provider_name,
                            reason="provider_capability_disabled",
                            details={"required_capability": capability_hint},
                        )
                        continue
                    runtime_status = str(runtime_row.get("status", "")).strip().lower()
                    if runtime_status in {"down", "maintenance", "disabled"}:
                        _drop(
                            provider_name,
                            reason="provider_runtime_unavailable",
                            details={"status": runtime_status},
                        )
                        continue
                    runtime_caps_list = [
                        self._normalize_mission_capability(str(item))
                        for item in (
                            runtime_row.get("capabilities", [])
                            if isinstance(runtime_row.get("capabilities", []), list)
                            else []
                        )
                        if self._normalize_mission_capability(str(item))
                    ]
                    if (
                        runtime_caps_list
                        and capability_hint not in {"general", "external"}
                        and capability_hint not in runtime_caps_list
                        and "external" not in runtime_caps_list
                    ):
                        _drop(
                            provider_name,
                            reason="provider_capability_runtime_mismatch",
                            details={
                                "required_capability": capability_hint,
                                "runtime_capabilities": runtime_caps_list[:12],
                            },
                        )
                        continue
                    runtime_ops = [
                        str(item).strip().lower()
                        for item in (
                            runtime_row.get("operation_classes", [])
                            if isinstance(runtime_row.get("operation_classes", []), list)
                            else []
                        )
                        if str(item).strip()
                    ]
                    if runtime_ops and operation_class not in runtime_ops:
                        _drop(
                            provider_name,
                            reason="provider_operation_class_not_supported",
                            details={
                                "required_operation_class": operation_class,
                                "runtime_operation_classes": runtime_ops[:12],
                            },
                        )
                        continue
                    action_allow = [
                        str(item).strip().lower()
                        for item in (
                            runtime_row.get("action_allow", [])
                            if isinstance(runtime_row.get("action_allow", []), list)
                            else []
                        )
                        if str(item).strip()
                    ]
                    if action_allow and clean_action not in action_allow:
                        _drop(
                            provider_name,
                            reason="provider_action_not_allowed",
                            details={"runtime_action_allow": action_allow[:12]},
                        )
                        continue
                    action_deny = [
                        str(item).strip().lower()
                        for item in (
                            runtime_row.get("action_deny", [])
                            if isinstance(runtime_row.get("action_deny", []), list)
                            else []
                        )
                        if str(item).strip()
                    ]
                    if action_deny and clean_action in action_deny:
                        _drop(
                            provider_name,
                            reason="provider_action_blocked",
                            details={"runtime_action_deny": action_deny[:12]},
                        )
                        continue

            allowed.append(provider_name)

        reason_to_code = {
            "provider_capability_unsupported": "provider_capability_not_supported_for_action",
            "provider_capability_runtime_mismatch": "provider_runtime_capability_mismatch",
            "provider_capability_disabled": "provider_runtime_capability_disabled",
            "provider_runtime_unavailable": "provider_runtime_unavailable",
            "provider_operation_class_not_supported": "provider_runtime_operation_not_supported",
            "provider_action_not_allowed": "provider_runtime_action_not_allowed",
            "provider_action_blocked": "provider_runtime_action_blocked",
        }
        explicit_clean = self._normalize_provider(str(explicit_provider or "").strip())
        if explicit_clean and explicit_clean not in {"", "auto"} and explicit_clean not in allowed:
            explicit_reason = ""
            explicit_details: Dict[str, Any] = {}
            for row in dropped:
                provider_name = self._normalize_provider(str(row.get("provider", "")))
                if provider_name != explicit_clean:
                    continue
                explicit_reason = str(row.get("reason", "")).strip().lower()
                details_row = row.get("details", {})
                explicit_details = details_row if isinstance(details_row, dict) else {}
                break
            code = reason_to_code.get(explicit_reason, "provider_capability_contract_failed")
            message = (
                f"Provider '{explicit_clean}' capability contract blocked action '{clean_action}'."
                if explicit_reason
                else f"Provider '{explicit_clean}' is unavailable for action '{clean_action}'."
            )
            return {
                "status": "error",
                "action": clean_action,
                "operation_class": operation_class,
                "required_capability": capability_hint,
                "requested_provider": explicit_clean,
                "providers": allowed,
                "dropped_providers": dropped,
                "allowed_providers": allowed,
                "capability_contract_code": code,
                "capability_contract_reason": explicit_reason,
                "capability_contract_details": explicit_details,
                "strict": strict,
                "message": message,
            }

        if strict and not allowed:
            first_reason = ""
            for row in dropped:
                first_reason = str(row.get("reason", "")).strip().lower()
                if first_reason:
                    break
            return {
                "status": "error",
                "action": clean_action,
                "operation_class": operation_class,
                "required_capability": capability_hint,
                "requested_provider": explicit_clean,
                "providers": [],
                "dropped_providers": dropped,
                "allowed_providers": [],
                "capability_contract_code": reason_to_code.get(first_reason, "provider_capability_contract_failed"),
                "capability_contract_reason": first_reason,
                "strict": strict,
                "message": (
                    f"No providers satisfy capability contract for action '{clean_action}' "
                    f"(capability={capability_hint}, operation={operation_class})."
                ),
            }

        return {
            "status": "success",
            "action": clean_action,
            "operation_class": operation_class,
            "required_capability": capability_hint,
            "requested_provider": explicit_clean,
            "providers": allowed,
            "allowed_providers": allowed,
            "dropped_providers": dropped,
            "strict": strict,
            "runtime_enabled": runtime_enabled,
            "message": "",
        }

    @classmethod
    def _normalize_provider(cls, raw: str) -> str:
        text = str(raw or "").strip().lower()
        if not text:
            return ""
        if text in {"google", "gmail", "google_docs", "google_tasks", "google_calendar"}:
            return "google"
        if text in {"graph", "microsoft_graph", "microsoft_graph_mail", "microsoft_graph_todo", "microsoft_graph_calendar", "microsoft_graph_drive", "microsoft"}:
            return "graph"
        if text in {"smtp"}:
            return "smtp"
        if text in {"auto"}:
            return "auto"
        return text

    @staticmethod
    def _classify_failure_category(message: str) -> str:
        lowered = str(message or "").strip().lower()
        if not lowered:
            return "unknown"
        if "401" in lowered or "403" in lowered or "unauthorized" in lowered or "forbidden" in lowered or "token" in lowered:
            return "auth"
        if "rate limit" in lowered or "429" in lowered or "too many requests" in lowered:
            return "rate_limited"
        if "timeout" in lowered or "timed out" in lowered:
            return "timeout"
        if any(token in lowered for token in ("invalid", "missing required", "not allowed", "denied", "non-retryable")):
            return "non_retryable"
        if any(token in lowered for token in ("unavailable", "service busy", "connection", "reset by peer", "temporar", "try again")):
            return "transient"
        return "unknown"

    def _cooldown_seconds(
        self,
        *,
        provider: str,
        category: str,
        operation_class: str,
        consecutive_failures: int,
        failure_ema: float,
        trend_ema: float = 0.0,
        operation_trend_ema: float = 0.0,
        cooldown_bias: float = 1.0,
    ) -> int:
        clean_provider = self._normalize_provider(provider) or "default"
        table = self._PROVIDER_COOLDOWN_BASE.get(clean_provider, self._PROVIDER_COOLDOWN_BASE["default"])
        base = int(table.get(category, table.get("unknown", 18)))
        if category in {"non_retryable"}:
            return 0
        if consecutive_failures <= 1 and failure_ema < 0.6:
            return 0
        operation_factor = self._coerce_float(
            self._OPERATION_COOLDOWN_FACTOR.get(
                str(operation_class or "").strip().lower(),
                self._OPERATION_COOLDOWN_FACTOR["default"],
            ),
            minimum=0.4,
            maximum=2.2,
            default=1.0,
        )
        trend = self._coerce_float(trend_ema, minimum=-1.0, maximum=1.0, default=0.0)
        operation_trend = self._coerce_float(operation_trend_ema, minimum=-1.0, maximum=1.0, default=0.0)
        trend_factor = 1.0
        if trend > 0.0:
            trend_factor += trend * self.cooldown_trend_weight
        else:
            trend_factor -= min(0.5, abs(trend) * self.cooldown_recovery_discount)
        if operation_trend > 0.0:
            trend_factor += operation_trend * (self.cooldown_trend_weight * 0.72)
        else:
            trend_factor -= min(0.35, abs(operation_trend) * (self.cooldown_recovery_discount * 0.65))
        trend_factor = max(0.45, min(1.95, trend_factor))
        bias_factor = self._coerce_float(
            cooldown_bias,
            minimum=self.cooldown_bias_min,
            maximum=self.cooldown_bias_max,
            default=1.0,
        )
        factor = (
            (1.0 + max(0.0, failure_ema - 0.45) + min(1.6, consecutive_failures * 0.22))
            * operation_factor
            * trend_factor
            * bias_factor
        )
        cooldown_s = int(round(base * factor))
        return max(0, min(cooldown_s, self.max_cooldown_s))

    def _smooth_cooldown_bias(self, *, previous: float, target: float) -> float:
        prev = self._coerce_float(
            previous,
            minimum=self.cooldown_bias_min,
            maximum=self.cooldown_bias_max,
            default=1.0,
        )
        tgt = self._coerce_float(
            target,
            minimum=self.cooldown_bias_min,
            maximum=self.cooldown_bias_max,
            default=1.0,
        )
        decay = self._coerce_float(
            self.cooldown_bias_decay,
            minimum=0.3,
            maximum=0.99,
            default=0.78,
        )
        value = (prev * decay) + (tgt * (1.0 - decay))
        return self._coerce_float(
            value,
            minimum=self.cooldown_bias_min,
            maximum=self.cooldown_bias_max,
            default=1.0,
        )

    def _load(self) -> None:
        if not self.store_path.exists():
            return
        try:
            payload = json.loads(self.store_path.read_text(encoding="utf-8"))
        except Exception:
            return
        config_raw = payload.get("mission_policy_config", {}) if isinstance(payload, dict) else {}
        self._apply_mission_policy_runtime_config(config_raw, reset=False)
        mission_policy_raw = payload.get("mission_outage_policy", {}) if isinstance(payload, dict) else {}
        mission_policy = mission_policy_raw if isinstance(mission_policy_raw, dict) else {}
        mission_analysis_history = self._load_mission_analysis_history(
            payload.get("mission_analysis_history", []) if isinstance(payload, dict) else []
        )
        items = payload.get("items", []) if isinstance(payload, dict) else []
        if not isinstance(items, list):
            return
        loaded: Dict[str, Dict[str, Any]] = {}
        for row in items:
            if not isinstance(row, dict):
                continue
            provider = self._normalize_provider(str(row.get("provider", "")).strip())
            if not provider:
                continue
            loaded[provider] = {
                "provider": provider,
                "samples": self._coerce_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                "successes": self._coerce_int(row.get("successes", 0), minimum=0, maximum=10_000_000, default=0),
                "failures": self._coerce_int(row.get("failures", 0), minimum=0, maximum=10_000_000, default=0),
                "consecutive_failures": self._coerce_int(row.get("consecutive_failures", 0), minimum=0, maximum=10_000_000, default=0),
                "failure_ema": self._coerce_float(row.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "failure_trend_ema": self._coerce_float(row.get("failure_trend_ema", 0.0), minimum=-1.0, maximum=1.0, default=0.0),
                "availability_ema": self._coerce_float(row.get("availability_ema", 0.55), minimum=0.0, maximum=1.0, default=0.55),
                "latency_ema_ms": self._coerce_float(row.get("latency_ema_ms", 0.0), minimum=0.0, maximum=3_600_000.0, default=0.0),
                "outage_ema": self._coerce_float(row.get("outage_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "outage_streak": self._coerce_int(row.get("outage_streak", 0), minimum=0, maximum=10_000_000, default=0),
                "outage_active": self._coerce_bool(row.get("outage_active", False), default=False),
                "outage_since_at": str(row.get("outage_since_at", "")).strip(),
                "outage_policy_bias": self._coerce_float(
                    row.get("outage_policy_bias", 0.0),
                    minimum=self.outage_policy_bias_min,
                    maximum=self.outage_policy_bias_max,
                    default=0.0,
                ),
                "cooldown_bias": self._coerce_float(
                    row.get("cooldown_bias", 1.0),
                    minimum=self.cooldown_bias_min,
                    maximum=self.cooldown_bias_max,
                    default=1.0,
                ),
                "operation_cooldown_bias": self._load_operation_cooldown_bias(row.get("operation_cooldown_bias", {})),
                "profile_performance": self._load_profile_performance(
                    row.get("profile_performance", {})
                ),
                "last_status": str(row.get("last_status", "")).strip().lower(),
                "last_error": str(row.get("last_error", "")).strip(),
                "last_category": str(row.get("last_category", "")).strip().lower(),
                "last_action": str(row.get("last_action", "")).strip().lower(),
                "cooldown_until": str(row.get("cooldown_until", "")).strip(),
                "last_cooldown_s": self._coerce_int(row.get("last_cooldown_s", 0), minimum=0, maximum=self.max_cooldown_s, default=0),
                "last_success_at": str(row.get("last_success_at", "")).strip(),
                "last_failure_at": str(row.get("last_failure_at", "")).strip(),
                "category_counts": self._load_category_counts(row.get("category_counts", {})),
                "action_stats": self._load_action_stats(row.get("action_stats", {})),
                "operation_stats": self._load_operation_stats(row.get("operation_stats", {})),
                "updated_at": str(row.get("updated_at", "")).strip(),
            }
        with self._lock:
            self._provider_states = loaded
            self._mission_outage_policy = {
                "bias": self._coerce_float(
                    mission_policy.get("bias", 0.0),
                    minimum=self.mission_outage_bias_min,
                    maximum=self.mission_outage_bias_max,
                    default=0.0,
                ),
                "pressure_ema": self._coerce_float(mission_policy.get("pressure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "risk_ema": self._coerce_float(mission_policy.get("risk_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "quality_ema": self._coerce_float(mission_policy.get("quality_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "failed_ratio_ema": self._coerce_float(
                    mission_policy.get("failed_ratio_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "blocked_ratio_ema": self._coerce_float(
                    mission_policy.get("blocked_ratio_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "mode": str(mission_policy.get("mode", "stable")).strip().lower() or "stable",
                "profile": self._normalize_mission_outage_profile(str(mission_policy.get("profile", "balanced"))),
                "profile_confidence": self._coerce_float(
                    mission_policy.get("profile_confidence", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "profile_pressure_ema": self._coerce_float(
                    mission_policy.get("profile_pressure_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "profile_stability_ema": self._coerce_float(
                    mission_policy.get("profile_stability_ema", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "profile_switch_count": self._coerce_int(
                    mission_policy.get("profile_switch_count", 0),
                    minimum=0,
                    maximum=1_000_000,
                    default=0,
                ),
                "profile_last_switch_at": str(mission_policy.get("profile_last_switch_at", "")).strip(),
                "profile_last_reason": str(mission_policy.get("profile_last_reason", "")).strip(),
                "profile_history": self._load_mission_profile_history(mission_policy.get("profile_history", [])),
                "capability_bias": self._load_mission_capability_bias(mission_policy.get("capability_bias", {})),
                "updated_at": str(mission_policy.get("updated_at", "")).strip(),
                "last_reason": str(mission_policy.get("last_reason", "")).strip(),
            }
            self._mission_analysis_history = mission_analysis_history
            self._trim_locked()

    def _trim_locked(self) -> None:
        mission_policy = self._mission_outage_policy if isinstance(self._mission_outage_policy, dict) else {}
        mission_analysis_history = (
            [dict(row) for row in self._mission_analysis_history if isinstance(row, dict)]
            if isinstance(self._mission_analysis_history, list)
            else []
        )
        profile_history_raw = mission_policy.get("profile_history", [])
        capability_bias_raw = mission_policy.get("capability_bias", {})
        mission_policy["capability_bias"] = self._serialize_mission_capability_bias(capability_bias_raw)
        if isinstance(profile_history_raw, list) and len(profile_history_raw) > self.mission_outage_profile_history_limit:
            mission_policy["profile_history"] = self._load_mission_profile_history(profile_history_raw)
        self._mission_outage_policy = mission_policy
        if len(mission_analysis_history) > self.mission_analysis_history_limit:
            mission_analysis_history = mission_analysis_history[-self.mission_analysis_history_limit :]
        self._mission_analysis_history = mission_analysis_history
        if len(self._provider_states) <= self.max_providers:
            for provider, row in list(self._provider_states.items()):
                action_rows = row.get("action_stats", {}) if isinstance(row, dict) else {}
                operation_rows = row.get("operation_stats", {}) if isinstance(row, dict) else {}
                if isinstance(action_rows, dict) and len(action_rows) > self.max_action_stats_per_provider:
                    self._provider_states[provider]["action_stats"] = self._trim_stat_rows(
                        action_rows,
                        limit=self.max_action_stats_per_provider,
                    )
                if isinstance(operation_rows, dict) and len(operation_rows) > self.max_operation_stats_per_provider:
                    self._provider_states[provider]["operation_stats"] = self._trim_stat_rows(
                        operation_rows,
                        limit=self.max_operation_stats_per_provider,
                    )
                operation_bias = row.get("operation_cooldown_bias", {}) if isinstance(row, dict) else {}
                if isinstance(operation_bias, dict) and len(operation_bias) > self.max_operation_stats_per_provider:
                    ordered_bias = sorted(
                        operation_bias.items(),
                        key=lambda item: (
                            self._coerce_float(
                                item[1],
                                minimum=self.cooldown_bias_min,
                                maximum=self.cooldown_bias_max,
                                default=1.0,
                            ),
                            str(item[0]),
                        ),
                        reverse=True,
                    )
                    self._provider_states[provider]["operation_cooldown_bias"] = {
                        str(name).strip().lower(): round(
                            self._coerce_float(
                                value,
                                minimum=self.cooldown_bias_min,
                                maximum=self.cooldown_bias_max,
                                default=1.0,
                            ),
                            6,
                        )
                        for name, value in ordered_bias[: self.max_operation_stats_per_provider]
                        if str(name).strip()
                    }
                profile_perf = row.get("profile_performance", {}) if isinstance(row, dict) else {}
                if isinstance(profile_perf, dict):
                    self._provider_states[provider]["profile_performance"] = self._load_profile_performance(profile_perf)
            return
        rows = sorted(
            self._provider_states.values(),
            key=lambda row: (
                str(row.get("updated_at", "")),
                self._coerce_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                str(row.get("provider", "")),
            ),
            reverse=True,
        )
        trimmed_states: Dict[str, Dict[str, Any]] = {}
        for row in rows[: self.max_providers]:
            provider = str(row.get("provider", "")).strip()
            if not provider:
                continue
            payload = dict(row)
            action_rows = payload.get("action_stats", {})
            operation_rows = payload.get("operation_stats", {})
            if isinstance(action_rows, dict) and len(action_rows) > self.max_action_stats_per_provider:
                payload["action_stats"] = self._trim_stat_rows(action_rows, limit=self.max_action_stats_per_provider)
            if isinstance(operation_rows, dict) and len(operation_rows) > self.max_operation_stats_per_provider:
                payload["operation_stats"] = self._trim_stat_rows(
                    operation_rows,
                    limit=self.max_operation_stats_per_provider,
                )
            operation_bias = payload.get("operation_cooldown_bias", {})
            if isinstance(operation_bias, dict) and len(operation_bias) > self.max_operation_stats_per_provider:
                ordered_bias = sorted(
                    operation_bias.items(),
                    key=lambda item: (
                        self._coerce_float(
                            item[1],
                            minimum=self.cooldown_bias_min,
                            maximum=self.cooldown_bias_max,
                            default=1.0,
                        ),
                        str(item[0]),
                    ),
                    reverse=True,
                )
                payload["operation_cooldown_bias"] = {
                    str(name).strip().lower(): round(
                        self._coerce_float(
                            value,
                            minimum=self.cooldown_bias_min,
                            maximum=self.cooldown_bias_max,
                            default=1.0,
                        ),
                        6,
                    )
                    for name, value in ordered_bias[: self.max_operation_stats_per_provider]
                    if str(name).strip()
                }
            profile_perf = payload.get("profile_performance", {})
            if isinstance(profile_perf, dict):
                payload["profile_performance"] = self._load_profile_performance(profile_perf)
            trimmed_states[provider] = payload
        self._provider_states = trimmed_states

    def _maybe_save_locked(self, *, force: bool) -> None:
        now = time.monotonic()
        if not force:
            if self._updates_since_save < 18 and (now - self._last_save_monotonic) < 20.0:
                return
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "mission_policy_config": self._mission_policy_runtime_config_snapshot(),
            "mission_outage_policy": dict(self._mission_outage_policy),
            "mission_analysis_history": list(self._mission_analysis_history),
            "items": list(self._provider_states.values()),
        }
        try:
            self.store_path.parent.mkdir(parents=True, exist_ok=True)
            self.store_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
            self._updates_since_save = 0
            self._last_save_monotonic = now
        except Exception:
            pass

    def _load_category_counts(self, raw: Any) -> Dict[str, int]:
        payload = raw if isinstance(raw, dict) else {}
        out: Dict[str, int] = {}
        for key, value in payload.items():
            name = str(key or "").strip().lower()
            if not name:
                continue
            out[name] = self._coerce_int(value, minimum=0, maximum=10_000_000, default=0)
        return out

    def _load_action_stats(self, raw: Any) -> Dict[str, Dict[str, Any]]:
        payload = raw if isinstance(raw, dict) else {}
        rows: Dict[str, Dict[str, Any]] = {}
        for action_name, value in payload.items():
            clean_action = str(action_name or "").strip().lower()
            if not clean_action or not isinstance(value, dict):
                continue
            rows[clean_action] = {
                "samples": self._coerce_int(value.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                "successes": self._coerce_int(value.get("successes", 0), minimum=0, maximum=10_000_000, default=0),
                "failures": self._coerce_int(value.get("failures", 0), minimum=0, maximum=10_000_000, default=0),
                "consecutive_failures": self._coerce_int(
                    value.get("consecutive_failures", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                ),
                "failure_ema": self._coerce_float(value.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "failure_trend_ema": self._coerce_float(value.get("failure_trend_ema", 0.0), minimum=-1.0, maximum=1.0, default=0.0),
                "latency_ema_ms": self._coerce_float(value.get("latency_ema_ms", 0.0), minimum=0.0, maximum=3_600_000.0, default=0.0),
                "last_status": str(value.get("last_status", "")).strip().lower(),
                "last_category": str(value.get("last_category", "")).strip().lower(),
                "updated_at": str(value.get("updated_at", "")).strip(),
            }
        if len(rows) <= self.max_action_stats_per_provider:
            return rows
        ordered = sorted(
            rows.items(),
            key=lambda item: (
                str(item[1].get("updated_at", "")),
                self._coerce_int(item[1].get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                item[0],
            ),
            reverse=True,
        )
        return {name: dict(row) for name, row in ordered[: self.max_action_stats_per_provider]}

    def _load_operation_stats(self, raw: Any) -> Dict[str, Dict[str, Any]]:
        payload = raw if isinstance(raw, dict) else {}
        rows: Dict[str, Dict[str, Any]] = {}
        for operation_name, value in payload.items():
            clean_operation = str(operation_name or "").strip().lower()
            if not clean_operation or not isinstance(value, dict):
                continue
            rows[clean_operation] = {
                "samples": self._coerce_int(value.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                "successes": self._coerce_int(value.get("successes", 0), minimum=0, maximum=10_000_000, default=0),
                "failures": self._coerce_int(value.get("failures", 0), minimum=0, maximum=10_000_000, default=0),
                "consecutive_failures": self._coerce_int(
                    value.get("consecutive_failures", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                ),
                "failure_ema": self._coerce_float(value.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                "failure_trend_ema": self._coerce_float(value.get("failure_trend_ema", 0.0), minimum=-1.0, maximum=1.0, default=0.0),
                "latency_ema_ms": self._coerce_float(value.get("latency_ema_ms", 0.0), minimum=0.0, maximum=3_600_000.0, default=0.0),
                "last_status": str(value.get("last_status", "")).strip().lower(),
                "last_category": str(value.get("last_category", "")).strip().lower(),
                "updated_at": str(value.get("updated_at", "")).strip(),
            }
        if len(rows) <= self.max_operation_stats_per_provider:
            return rows
        ordered = sorted(
            rows.items(),
            key=lambda item: (
                str(item[1].get("updated_at", "")),
                self._coerce_int(item[1].get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                item[0],
            ),
            reverse=True,
        )
        return {name: dict(row) for name, row in ordered[: self.max_operation_stats_per_provider]}

    def _load_operation_cooldown_bias(self, raw: Any) -> Dict[str, float]:
        payload = raw if isinstance(raw, dict) else {}
        rows: Dict[str, float] = {}
        for operation_name, value in payload.items():
            clean_operation = str(operation_name or "").strip().lower()
            if not clean_operation:
                continue
            rows[clean_operation] = round(
                self._coerce_float(
                    value,
                    minimum=self.cooldown_bias_min,
                    maximum=self.cooldown_bias_max,
                    default=1.0,
                ),
                6,
            )
        if len(rows) <= self.max_operation_stats_per_provider:
            return rows
        ordered = sorted(rows.items(), key=lambda item: (float(item[1]), item[0]), reverse=True)
        return {name: float(value) for name, value in ordered[: self.max_operation_stats_per_provider]}

    def _load_profile_performance(self, raw: Any) -> Dict[str, Dict[str, Any]]:
        payload = raw if isinstance(raw, dict) else {}
        rows: Dict[str, Dict[str, Any]] = {}
        for profile_name, value in payload.items():
            clean_profile = self._normalize_mission_outage_profile(str(profile_name))
            if not isinstance(value, dict):
                continue
            rows[clean_profile] = {
                "samples": self._coerce_int(
                    value.get("samples", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                ),
                "successes": self._coerce_int(
                    value.get("successes", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                ),
                "failures": self._coerce_int(
                    value.get("failures", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                ),
                "success_rate": round(
                    self._coerce_float(
                        value.get("success_rate", 0.0),
                        minimum=0.0,
                        maximum=1.0,
                        default=0.0,
                    ),
                    6,
                ),
                "success_ema": round(
                    self._coerce_float(
                        value.get("success_ema", 0.5),
                        minimum=0.0,
                        maximum=1.0,
                        default=0.5,
                    ),
                    6,
                ),
                "updated_at": str(value.get("updated_at", "")).strip(),
            }
        if len(rows) <= 4:
            return rows
        ordered = sorted(
            rows.items(),
            key=lambda item: (
                str(item[1].get("updated_at", "")),
                self._coerce_int(
                    item[1].get("samples", 0),
                    minimum=0,
                    maximum=10_000_000,
                    default=0,
                ),
                item[0],
            ),
            reverse=True,
        )
        return {name: dict(row) for name, row in ordered[:4]}

    def _load_mission_profile_history(self, raw: Any) -> List[Dict[str, Any]]:
        rows = raw if isinstance(raw, list) else []
        loaded: List[Dict[str, Any]] = []
        for value in rows[-self.mission_outage_profile_history_limit :]:
            if not isinstance(value, dict):
                continue
            loaded.append(
                {
                    "at": str(value.get("at", "")).strip(),
                    "profile": self._normalize_mission_outage_profile(str(value.get("profile", "balanced"))),
                    "mode": str(value.get("mode", "stable")).strip().lower() or "stable",
                    "target_pressure": round(
                        self._coerce_float(value.get("target_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "risk": round(self._coerce_float(value.get("risk", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6),
                    "quality": round(
                        self._coerce_float(value.get("quality", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "reliability": round(
                        self._coerce_float(value.get("reliability", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "failed_ratio": round(
                        self._coerce_float(value.get("failed_ratio", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "blocked_ratio": round(
                        self._coerce_float(value.get("blocked_ratio", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        6,
                    ),
                    "reason": str(value.get("reason", "")).strip(),
                }
            )
        return loaded

    def _trim_stat_rows(self, rows: Dict[str, Dict[str, Any]], *, limit: int) -> Dict[str, Dict[str, Any]]:
        ordered = sorted(
            rows.items(),
            key=lambda item: (
                str(item[1].get("updated_at", "")) if isinstance(item[1], dict) else "",
                self._coerce_int(item[1].get("samples", 0), minimum=0, maximum=10_000_000, default=0)
                if isinstance(item[1], dict)
                else 0,
                item[0],
            ),
            reverse=True,
        )
        return {name: dict(data) for name, data in ordered[:limit] if isinstance(data, dict)}

    @staticmethod
    def _merge_dropped_provider_rows(*groups: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        merged: List[Dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()
        for group in groups:
            if not isinstance(group, list):
                continue
            for row in group:
                if not isinstance(row, dict):
                    continue
                provider = str(row.get("provider", "")).strip().lower()
                if not provider:
                    continue
                reason = str(row.get("reason", "")).strip().lower()
                source = str(row.get("source", "")).strip().lower()
                key = (provider, reason, source)
                if key in seen:
                    continue
                seen.add(key)
                merged.append({"provider": provider, "reason": reason, "source": source})
        return merged

    @staticmethod
    def _to_timestamp(value: str) -> float:
        text = str(value or "").strip()
        if not text:
            return 0.0
        normalized = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except Exception:
            return 0.0
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).timestamp()

    @staticmethod
    def _coerce_int(value: object, *, minimum: int, maximum: int, default: int) -> int:
        try:
            parsed = int(value)
        except Exception:
            parsed = default
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _coerce_float(value: object, *, minimum: float, maximum: float, default: float) -> float:
        try:
            parsed = float(value)
        except Exception:
            parsed = default
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _coerce_bool(value: object, *, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        text = str(value or "").strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
        return bool(default)

    @staticmethod
    def _env_flag(name: str, *, default: bool = False) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return default
        return ExternalReliabilityOrchestrator._coerce_bool(raw, default=default)
