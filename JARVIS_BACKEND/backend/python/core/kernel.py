import asyncio
import os
import re
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from .approval_gate import ApprovalGate
from .circuit_breaker import ActionCircuitBreaker
from .contracts import ActionResult, ExecutionPlan, GoalRecord, GoalRequest, PlanStep
from .desktop_anchor_memory import DesktopAnchorMemory
from .desktop_state import DesktopState
from .episodic_memory import EpisodicMemory
from .execution_strategy import ExecutionStrategyController
from .executor import Executor
from .external_reliability import ExternalReliabilityOrchestrator
from .goal_manager import GoalManager
from .macro_manager import MacroManager
from .mission_control import MissionControl
from .oauth_token_store import OAuthTokenStore
from .planner import Planner
from .policy_bandit import MissionPolicyBandit
from .recovery import RecoveryManager
from .rollback_manager import RollbackManager
from .runtime_memory import RuntimeMemory
from .schedule_manager import SCHEDULE_METADATA_KEY, ScheduleManager
from .telemetry import Telemetry
from .tool_registry import ToolRegistry
from .trigger_manager import TriggerManager
from .verifier import Verifier
from backend.python.policies.policy_guard import PolicyGuard
from backend.python.tools.route_handlers import register_tools
from backend.python.utils.logger import Logger


class AgentKernel:
    """
    Planner -> Policy -> Executor -> Verifier -> Recovery runtime.
    """

    _TEMPLATE_TOKEN_EXACT_RE = re.compile(r"^\{\{\s*([a-zA-Z0-9_.-]+)\s*\}\}$")

    def __init__(self) -> None:
        self.log = Logger("AgentKernel").get_logger()
        self.telemetry = Telemetry(max_events=int(os.getenv("JARVIS_TELEMETRY_MAX_EVENTS", "5000")))
        self.goal_manager = GoalManager(
            store_path=os.getenv("JARVIS_GOAL_STORE", "data/goals.json"),
            max_records=self._coerce_int(
                os.getenv("JARVIS_GOAL_MAX_RECORDS", "5000"),
                minimum=100,
                maximum=100_000,
                default=5000,
            ),
        )
        self.registry = ToolRegistry()
        register_tools(self.registry)

        self.planner = Planner()
        self.planner.set_tool_catalog(set(self.registry.list_tools().keys()))
        self.policy = PolicyGuard()
        self.policy.set_runtime_actions(set(self.registry.list_tools().keys()))
        self.approval_gate = ApprovalGate(
            ttl_s=int(os.getenv("JARVIS_APPROVAL_TTL_S", "300")),
            max_records=int(os.getenv("JARVIS_APPROVAL_MAX_RECORDS", "2048")),
        )
        self.verifier = Verifier()
        self.recovery = RecoveryManager()
        self.runtime_memory = RuntimeMemory(
            max_items=int(os.getenv("JARVIS_RUNTIME_MEMORY_MAX", "120")),
            store_path=os.getenv("JARVIS_RUNTIME_MEMORY_STORE", "data/runtime_memory.jsonl"),
        )
        self.desktop_state = DesktopState(
            max_items=int(os.getenv("JARVIS_DESKTOP_STATE_MAX", "800")),
            store_path=os.getenv("JARVIS_DESKTOP_STATE_STORE", "data/desktop_state.jsonl"),
        )
        self.desktop_anchor_memory = DesktopAnchorMemory(
            max_entries=int(os.getenv("JARVIS_DESKTOP_ANCHOR_MEMORY_MAX", "6000")),
            store_path=os.getenv("JARVIS_DESKTOP_ANCHOR_MEMORY_STORE", "data/desktop_anchor_memory.json"),
            quarantine_ttl_s=int(os.getenv("JARVIS_DESKTOP_ANCHOR_QUARANTINE_TTL_S", "1200")),
            quarantine_max_entries=int(os.getenv("JARVIS_DESKTOP_ANCHOR_QUARANTINE_MAX", "3000")),
        )
        self.episodic_memory = EpisodicMemory(
            max_items=int(os.getenv("JARVIS_EPISODIC_MEMORY_MAX", "5000")),
            store_path=os.getenv("JARVIS_EPISODIC_MEMORY_STORE", "data/episodic_memory.jsonl"),
            embedding_dim=int(os.getenv("JARVIS_EPISODIC_EMBEDDING_DIM", "256")),
        )
        self.macro_manager = MacroManager(
            store_path=os.getenv("JARVIS_MACRO_STORE", "data/macros.json"),
            max_records=int(os.getenv("JARVIS_MACRO_MAX_RECORDS", "1500")),
        )
        self.schedule_manager = ScheduleManager(
            store_path=os.getenv("JARVIS_SCHEDULE_STORE", "data/schedules.json"),
            max_records=int(os.getenv("JARVIS_SCHEDULE_MAX_RECORDS", "2000")),
        )
        self.trigger_manager = TriggerManager(
            store_path=os.getenv("JARVIS_TRIGGER_STORE", "data/triggers.json"),
            max_records=int(os.getenv("JARVIS_TRIGGER_MAX_RECORDS", "2000")),
        )
        self.mission_control = MissionControl(
            store_path=os.getenv("JARVIS_MISSION_STORE", "data/missions.json"),
            max_records=int(os.getenv("JARVIS_MISSION_MAX_RECORDS", "5000")),
            max_checkpoints=int(os.getenv("JARVIS_MISSION_MAX_CHECKPOINTS", "1200")),
        )
        self.rollback_manager = RollbackManager(
            store_path=os.getenv("JARVIS_ROLLBACK_STORE", "data/rollback_journal.json"),
            backup_dir=os.getenv("JARVIS_ROLLBACK_BACKUP_DIR", "data/rollback_backups"),
            max_entries=int(os.getenv("JARVIS_ROLLBACK_MAX_ENTRIES", "20000")),
        )
        self.action_circuit_breaker = ActionCircuitBreaker(
            failure_threshold=self._coerce_int(
                os.getenv("JARVIS_CIRCUIT_BREAKER_FAILURE_THRESHOLD", "3"),
                minimum=1,
                maximum=20,
                default=3,
            ),
            cooldown_s=self._coerce_int(
                os.getenv("JARVIS_CIRCUIT_BREAKER_COOLDOWN_S", "45"),
                minimum=5,
                maximum=3600,
                default=45,
            ),
            max_cooldown_s=self._coerce_int(
                os.getenv("JARVIS_CIRCUIT_BREAKER_MAX_COOLDOWN_S", "900"),
                minimum=15,
                maximum=7200,
                default=900,
            ),
            max_states=self._coerce_int(
                os.getenv("JARVIS_CIRCUIT_BREAKER_MAX_STATES", "2000"),
                minimum=20,
                maximum=100000,
                default=2000,
            ),
        )
        self.oauth_store = OAuthTokenStore.shared(store_path=os.getenv("JARVIS_OAUTH_STORE", "data/oauth_tokens.json"))
        self.external_reliability = ExternalReliabilityOrchestrator(
            store_path=os.getenv("JARVIS_EXTERNAL_RELIABILITY_STORE", "data/external_reliability.json"),
            max_providers=self._coerce_int(
                os.getenv("JARVIS_EXTERNAL_RELIABILITY_MAX_PROVIDERS", "1200"),
                minimum=60,
                maximum=20000,
                default=1200,
            ),
        )
        self.policy_bandit = MissionPolicyBandit(
            store_path=os.getenv("JARVIS_POLICY_BANDIT_STORE", "data/policy_bandit.json"),
            max_task_classes=self._coerce_int(
                os.getenv("JARVIS_POLICY_BANDIT_MAX_TASK_CLASSES", "1200"),
                minimum=40,
                maximum=50_000,
                default=1200,
            ),
            max_profiles_per_class=self._coerce_int(
                os.getenv("JARVIS_POLICY_BANDIT_MAX_PROFILES_PER_CLASS", "24"),
                minimum=2,
                maximum=200,
                default=24,
            ),
        )
        self.execution_strategy = ExecutionStrategyController(
            store_path=os.getenv("JARVIS_EXECUTION_STRATEGY_STORE", "data/execution_strategy.json"),
            max_task_classes=self._coerce_int(
                os.getenv("JARVIS_EXECUTION_STRATEGY_MAX_TASK_CLASSES", "1400"),
                minimum=40,
                maximum=50_000,
                default=1400,
            ),
        )
        self.executor = Executor(
            registry=self.registry,
            policy_guard=self.policy,
            verifier=self.verifier,
            recovery=self.recovery,
            telemetry=self.telemetry,
            approval_gate=self.approval_gate,
            rollback_manager=self.rollback_manager,
            circuit_breaker=self.action_circuit_breaker,
            desktop_state=self.desktop_state,
            desktop_anchor_memory=self.desktop_anchor_memory,
            external_reliability=self.external_reliability,
        )
        self.max_replans = max(0, int(os.getenv("JARVIS_MAX_REPLANS", "2")))
        self.replan_delay_base_s = self._coerce_float(
            os.getenv("JARVIS_REPLAN_DELAY_BASE_S", "0"),
            minimum=0.0,
            maximum=10.0,
            default=0.0,
        )
        self.replan_allow_blocked = self._env_flag("JARVIS_REPLAN_ALLOW_BLOCKED", default=False)
        self.replan_allow_non_retryable = self._env_flag("JARVIS_REPLAN_ALLOW_NON_RETRYABLE", default=False)
        self.replan_escalate_recovery_profile = self._env_flag("JARVIS_REPLAN_ESCALATE_RECOVERY_PROFILE", default=True)
        self.replan_escalate_verification = self._env_flag("JARVIS_REPLAN_ESCALATE_VERIFICATION", default=True)
        self.replan_escalate_policy_profile = self._env_flag("JARVIS_REPLAN_ESCALATE_POLICY_PROFILE", default=True)
        self.runtime_policy_adaptation_enabled = self._env_flag(
            "JARVIS_RUNTIME_POLICY_ADAPTATION_ENABLED",
            default=True,
        )
        self.runtime_policy_auto_upgrade = self._env_flag(
            "JARVIS_RUNTIME_POLICY_AUTO_UPGRADE",
            default=False,
        )
        self.runtime_policy_external_pressure_threshold = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_EXTERNAL_PRESSURE_THRESHOLD", "0.48"),
            minimum=0.1,
            maximum=0.95,
            default=0.48,
        )
        self.runtime_policy_quality_floor = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_QUALITY_FLOOR", "0.56"),
            minimum=0.1,
            maximum=0.95,
            default=0.56,
        )
        self.runtime_policy_trend_feedback_enabled = self._env_flag(
            "JARVIS_RUNTIME_POLICY_TREND_FEEDBACK_ENABLED",
            default=True,
        )
        self.runtime_policy_trend_refresh_s = self._coerce_int(
            os.getenv("JARVIS_RUNTIME_POLICY_TREND_REFRESH_S", "45"),
            minimum=5,
            maximum=1800,
            default=45,
        )
        self.runtime_policy_trend_limit = self._coerce_int(
            os.getenv("JARVIS_RUNTIME_POLICY_TREND_LIMIT", "220"),
            minimum=40,
            maximum=2000,
            default=220,
        )
        self.runtime_policy_trend_weight = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_TREND_WEIGHT", "0.35"),
            minimum=0.0,
            maximum=1.0,
            default=0.35,
        )
        self.runtime_policy_trend_relief_weight = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_TREND_RELIEF_WEIGHT", "0.24"),
            minimum=0.0,
            maximum=1.0,
            default=0.24,
        )
        self.runtime_policy_mission_drift_enabled = self._env_flag(
            "JARVIS_RUNTIME_POLICY_MISSION_DRIFT_ENABLED",
            default=True,
        )
        self.runtime_policy_mission_drift_weight = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_MISSION_DRIFT_WEIGHT", "0.32"),
            minimum=0.0,
            maximum=1.0,
            default=0.32,
        )
        self.runtime_policy_mission_drift_relief_weight = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_MISSION_DRIFT_RELIEF_WEIGHT", "0.18"),
            minimum=0.0,
            maximum=1.0,
            default=0.18,
        )
        self.runtime_policy_mission_drift_severe_threshold = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_MISSION_DRIFT_SEVERE_THRESHOLD", "0.66"),
            minimum=0.1,
            maximum=1.0,
            default=0.66,
        )
        self.runtime_policy_provider_policy_relief_enabled = self._env_flag(
            "JARVIS_RUNTIME_POLICY_PROVIDER_POLICY_RELIEF_ENABLED",
            default=True,
        )
        self.runtime_policy_provider_policy_relief_gain = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_PROVIDER_POLICY_RELIEF_GAIN", "0.12"),
            minimum=0.0,
            maximum=0.5,
            default=0.12,
        )
        self.runtime_policy_signal_smoothing_enabled = self._env_flag(
            "JARVIS_RUNTIME_POLICY_SIGNAL_SMOOTHING_ENABLED",
            default=True,
        )
        self.runtime_policy_signal_ema_alpha = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_SIGNAL_EMA_ALPHA", "0.42"),
            minimum=0.05,
            maximum=1.0,
            default=0.42,
        )
        self.runtime_policy_signal_stale_reset_s = self._coerce_int(
            os.getenv("JARVIS_RUNTIME_POLICY_SIGNAL_STALE_RESET_S", "1200"),
            minimum=30,
            maximum=86_400,
            default=1200,
        )
        self.runtime_policy_signal_state_max_scopes = self._coerce_int(
            os.getenv("JARVIS_RUNTIME_POLICY_SIGNAL_STATE_MAX_SCOPES", "3000"),
            minimum=2,
            maximum=100_000,
            default=3000,
        )
        self.runtime_policy_hysteresis_external_margin = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_HYSTERESIS_EXTERNAL_MARGIN", "0.05"),
            minimum=0.0,
            maximum=0.4,
            default=0.05,
        )
        self.runtime_policy_hysteresis_quality_margin = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_HYSTERESIS_QUALITY_MARGIN", "0.04"),
            minimum=0.0,
            maximum=0.4,
            default=0.04,
        )
        self.runtime_policy_hysteresis_confirm_margin = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_HYSTERESIS_CONFIRM_MARGIN", "0.05"),
            minimum=0.0,
            maximum=0.4,
            default=0.05,
        )
        self.runtime_policy_hysteresis_trend_margin = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_HYSTERESIS_TREND_MARGIN", "0.06"),
            minimum=0.0,
            maximum=0.4,
            default=0.06,
        )
        self.runtime_policy_contract_guardrail_enabled = self._env_flag(
            "JARVIS_RUNTIME_POLICY_CONTRACT_GUARDRAIL_ENABLED",
            default=True,
        )
        self.runtime_policy_contract_pressure_threshold = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_CONTRACT_PRESSURE_THRESHOLD", "0.38"),
            minimum=0.05,
            maximum=0.95,
            default=0.38,
        )
        self.runtime_policy_contract_severe_threshold = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_CONTRACT_SEVERE_THRESHOLD", "0.62"),
            minimum=0.1,
            maximum=1.0,
            default=0.62,
        )
        self.runtime_policy_remediation_feedback_enabled = self._env_flag(
            "JARVIS_RUNTIME_POLICY_REMEDIATION_FEEDBACK_ENABLED",
            default=True,
        )
        self.runtime_policy_remediation_hard_floor = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_REMEDIATION_HARD_FLOOR", "0.34"),
            minimum=0.0,
            maximum=1.0,
            default=0.34,
        )
        self.runtime_policy_remediation_relief_floor = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_REMEDIATION_RELIEF_FLOOR", "0.74"),
            minimum=0.0,
            maximum=1.0,
            default=0.74,
        )
        self.runtime_policy_remediation_min_samples = self._coerce_int(
            os.getenv("JARVIS_RUNTIME_POLICY_REMEDIATION_MIN_SAMPLES", "2"),
            minimum=0,
            maximum=100,
            default=2,
        )
        self.runtime_policy_repair_memory_limit = self._coerce_int(
            os.getenv("JARVIS_RUNTIME_POLICY_REPAIR_MEMORY_LIMIT", "8"),
            minimum=1,
            maximum=40,
            default=8,
        )
        self.runtime_policy_verification_pressure_enabled = self._env_flag(
            "JARVIS_RUNTIME_POLICY_VERIFICATION_PRESSURE_ENABLED",
            default=True,
        )
        self.runtime_policy_verification_pressure_threshold = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_VERIFICATION_PRESSURE_THRESHOLD", "0.36"),
            minimum=0.05,
            maximum=0.95,
            default=0.36,
        )
        self.runtime_policy_verification_pressure_severe_threshold = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_VERIFICATION_PRESSURE_SEVERE_THRESHOLD", "0.62"),
            minimum=0.1,
            maximum=1.0,
            default=0.62,
        )
        self.runtime_policy_telemetry_feedback_enabled = self._env_flag(
            "JARVIS_RUNTIME_POLICY_TELEMETRY_FEEDBACK_ENABLED",
            default=True,
        )
        self.runtime_policy_telemetry_feedback_min_events = self._coerce_int(
            os.getenv("JARVIS_RUNTIME_POLICY_TELEMETRY_FEEDBACK_MIN_EVENTS", "28"),
            minimum=1,
            maximum=5000,
            default=28,
        )
        self.runtime_policy_telemetry_feedback_limit = self._coerce_int(
            os.getenv("JARVIS_RUNTIME_POLICY_TELEMETRY_FEEDBACK_LIMIT", "900"),
            minimum=20,
            maximum=10_000,
            default=900,
        )
        self.runtime_policy_telemetry_feedback_decay = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_TELEMETRY_FEEDBACK_DECAY", "0.72"),
            minimum=0.3,
            maximum=0.99,
            default=0.72,
        )
        self.runtime_policy_telemetry_feedback_event_rate_scale = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_TELEMETRY_FEEDBACK_EVENT_RATE_SCALE", "8.0"),
            minimum=0.5,
            maximum=50.0,
            default=8.0,
        )
        self.runtime_policy_telemetry_feedback_failure_weight = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_POLICY_TELEMETRY_FEEDBACK_FAILURE_WEIGHT", "0.74"),
            minimum=0.05,
            maximum=0.95,
            default=0.74,
        )
        self.runtime_external_route_adaptation_enabled = self._env_flag(
            "JARVIS_RUNTIME_EXTERNAL_ROUTE_ADAPTATION_ENABLED",
            default=True,
        )
        self.runtime_external_route_entropy_force_enabled = self._env_flag(
            "JARVIS_RUNTIME_EXTERNAL_ROUTE_ENTROPY_FORCE_ENABLED",
            default=True,
        )
        self.runtime_external_route_severe_profile = self._normalize_external_route_profile(
            os.getenv("JARVIS_RUNTIME_EXTERNAL_ROUTE_SEVERE_PROFILE", "defensive")
        )
        self.runtime_external_route_moderate_profile = self._normalize_external_route_profile(
            os.getenv("JARVIS_RUNTIME_EXTERNAL_ROUTE_MODERATE_PROFILE", "cautious")
        )
        self.runtime_external_route_stable_profile = self._normalize_external_route_profile(
            os.getenv("JARVIS_RUNTIME_EXTERNAL_ROUTE_STABLE_PROFILE", "balanced")
        )
        self.runtime_external_route_throughput_profile = self._normalize_external_route_profile(
            os.getenv("JARVIS_RUNTIME_EXTERNAL_ROUTE_THROUGHPUT_PROFILE", "throughput")
        )
        self.runtime_external_route_throughput_quality_floor = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_EXTERNAL_ROUTE_THROUGHPUT_QUALITY_FLOOR", "0.88"),
            minimum=0.5,
            maximum=0.99,
            default=0.88,
        )
        self.runtime_external_route_probe_severe = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_EXTERNAL_ROUTE_PROBE_SEVERE", "0.96"),
            minimum=0.0,
            maximum=1.0,
            default=0.96,
        )
        self.runtime_external_route_probe_moderate = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_EXTERNAL_ROUTE_PROBE_MODERATE", "0.72"),
            minimum=0.0,
            maximum=1.0,
            default=0.72,
        )
        self.runtime_external_route_probe_stable = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_EXTERNAL_ROUTE_PROBE_STABLE", "0.28"),
            minimum=0.0,
            maximum=1.0,
            default=0.28,
        )
        self.runtime_external_remediation_budget_enabled = self._env_flag(
            "JARVIS_RUNTIME_EXTERNAL_REMEDIATION_BUDGET_ENABLED",
            default=True,
        )
        self.runtime_external_remediation_actions_severe = self._coerce_int(
            os.getenv("JARVIS_RUNTIME_EXTERNAL_REMEDIATION_ACTIONS_SEVERE", "5"),
            minimum=1,
            maximum=8,
            default=5,
        )
        self.runtime_external_remediation_actions_moderate = self._coerce_int(
            os.getenv("JARVIS_RUNTIME_EXTERNAL_REMEDIATION_ACTIONS_MODERATE", "3"),
            minimum=1,
            maximum=8,
            default=3,
        )
        self.runtime_external_remediation_actions_stable = self._coerce_int(
            os.getenv("JARVIS_RUNTIME_EXTERNAL_REMEDIATION_ACTIONS_STABLE", "2"),
            minimum=1,
            maximum=8,
            default=2,
        )
        self.runtime_external_remediation_total_severe = self._coerce_int(
            os.getenv("JARVIS_RUNTIME_EXTERNAL_REMEDIATION_TOTAL_SEVERE", "12"),
            minimum=2,
            maximum=24,
            default=12,
        )
        self.runtime_external_remediation_total_moderate = self._coerce_int(
            os.getenv("JARVIS_RUNTIME_EXTERNAL_REMEDIATION_TOTAL_MODERATE", "8"),
            minimum=2,
            maximum=24,
            default=8,
        )
        self.runtime_external_remediation_total_stable = self._coerce_int(
            os.getenv("JARVIS_RUNTIME_EXTERNAL_REMEDIATION_TOTAL_STABLE", "6"),
            minimum=2,
            maximum=24,
            default=6,
        )
        self.runtime_external_contract_risk_floor = self._coerce_float(
            os.getenv("JARVIS_RUNTIME_EXTERNAL_CONTRACT_RISK_FLOOR", "0.14"),
            minimum=0.0,
            maximum=1.0,
            default=0.14,
        )
        self.oauth_maintenance_enabled = self._env_flag("JARVIS_OAUTH_MAINTENANCE_ENABLED", default=True)
        self.oauth_maintenance_interval_s = self._coerce_int(
            os.getenv("JARVIS_OAUTH_MAINTENANCE_INTERVAL_S", "90"),
            minimum=5,
            maximum=3600,
            default=90,
        )
        self.oauth_refresh_window_s = self._coerce_int(
            os.getenv("JARVIS_OAUTH_REFRESH_WINDOW_S", "300"),
            minimum=0,
            maximum=86400 * 7,
            default=300,
        )
        self.auto_mission_recovery_enabled = self._env_flag("JARVIS_MISSION_AUTO_RECOVER_ENABLED", default=True)
        self.auto_mission_recovery_allow_blocked = self._env_flag("JARVIS_MISSION_AUTO_RECOVER_ALLOW_BLOCKED", default=False)
        self.auto_mission_recovery_allow_unknown = self._env_flag("JARVIS_MISSION_AUTO_RECOVER_ALLOW_UNKNOWN", default=False)
        self.auto_mission_recovery_poll_s = self._coerce_int(
            os.getenv("JARVIS_MISSION_AUTO_RECOVER_POLL_S", "45"),
            minimum=5,
            maximum=3600,
            default=45,
        )
        self.auto_mission_recovery_max_resumes = self._coerce_int(
            os.getenv("JARVIS_MISSION_AUTO_RECOVER_MAX_RESUMES", "3"),
            minimum=0,
            maximum=100,
            default=3,
        )
        self.auto_mission_recovery_base_delay_s = self._coerce_int(
            os.getenv("JARVIS_MISSION_AUTO_RECOVER_BASE_DELAY_S", "20"),
            minimum=0,
            maximum=3600,
            default=20,
        )
        self.auto_mission_recovery_max_delay_s = self._coerce_int(
            os.getenv("JARVIS_MISSION_AUTO_RECOVER_MAX_DELAY_S", "600"),
            minimum=5,
            maximum=7200,
            default=600,
        )
        self.auto_mission_recovery_profile_escalate = self._env_flag(
            "JARVIS_MISSION_AUTO_RECOVER_PROFILE_ESCALATE",
            default=True,
        )
        self.auto_rollback_enabled = self._env_flag("JARVIS_AUTO_ROLLBACK_ENABLED", default=True)
        self.auto_rollback_allow_blocked = self._env_flag("JARVIS_AUTO_ROLLBACK_ALLOW_BLOCKED", default=False)
        self.auto_rollback_dry_run = self._env_flag("JARVIS_AUTO_ROLLBACK_DRY_RUN", default=False)
        self.auto_rollback_default_policy = (
            str(os.getenv("JARVIS_ROLLBACK_POLICY", "on_failure")).strip().lower() or "on_failure"
        )
        self.autonomy_auto_tune_enabled = self._env_flag("JARVIS_AUTONOMY_AUTO_TUNE_ENABLED", default=False)
        self.autonomy_auto_tune_interval_s = self._coerce_int(
            os.getenv("JARVIS_AUTONOMY_AUTO_TUNE_INTERVAL_S", "120"),
            minimum=15,
            maximum=3600,
            default=120,
        )
        self.external_reliability_analysis_auto_emit_enabled = self._env_flag(
            "JARVIS_EXTERNAL_RELIABILITY_ANALYSIS_AUTO_EMIT_ENABLED",
            default=True,
        )
        self.external_reliability_analysis_auto_emit_interval_s = self._coerce_int(
            os.getenv("JARVIS_EXTERNAL_RELIABILITY_ANALYSIS_AUTO_EMIT_INTERVAL_S", "180"),
            minimum=20,
            maximum=7200,
            default=180,
        )
        self.external_reliability_analysis_provider_limit = self._coerce_int(
            os.getenv("JARVIS_EXTERNAL_RELIABILITY_ANALYSIS_PROVIDER_LIMIT", "260"),
            minimum=20,
            maximum=5000,
            default=260,
        )
        self.external_reliability_analysis_history_limit = self._coerce_int(
            os.getenv("JARVIS_EXTERNAL_RELIABILITY_ANALYSIS_HISTORY_LIMIT", "40"),
            minimum=8,
            maximum=400,
            default=40,
        )
        self.external_reliability_mission_history_limit = self._coerce_int(
            os.getenv("JARVIS_EXTERNAL_RELIABILITY_MISSION_HISTORY_LIMIT", "240"),
            minimum=20,
            maximum=5000,
            default=240,
        )
        self.external_reliability_mission_history_window = self._coerce_int(
            os.getenv("JARVIS_EXTERNAL_RELIABILITY_MISSION_HISTORY_WINDOW", "36"),
            minimum=4,
            maximum=1200,
            default=36,
        )
        self.external_reliability_provider_policy_autotune_enabled = self._env_flag(
            "JARVIS_EXTERNAL_RELIABILITY_PROVIDER_POLICY_AUTOTUNE_ENABLED",
            default=True,
        )
        self.external_reliability_provider_policy_autotune_interval_s = self._coerce_int(
            os.getenv("JARVIS_EXTERNAL_RELIABILITY_PROVIDER_POLICY_AUTOTUNE_INTERVAL_S", "120"),
            minimum=15,
            maximum=7200,
            default=120,
        )
        self.external_reliability_provider_policy_autotune_dry_run = self._env_flag(
            "JARVIS_EXTERNAL_RELIABILITY_PROVIDER_POLICY_AUTOTUNE_DRY_RUN",
            default=False,
        )

        self._running = False
        self._worker: Optional[asyncio.Task] = None
        self._background_tasks: Set[asyncio.Task[Any]] = set()
        self._pending_auto_resume_missions: Set[str] = set()
        self._last_oauth_maintenance_monotonic = 0.0
        self._last_mission_recovery_monotonic = 0.0
        self._last_autonomy_tune_monotonic = 0.0
        self._last_external_reliability_analysis_monotonic = 0.0
        self._last_external_provider_policy_autotune_monotonic = 0.0
        self._last_mission_trend_feedback_monotonic = 0.0
        self._last_autonomy_tune: Dict[str, Any] = {
            "status": "idle",
            "last_run_at": "",
            "changed": False,
            "target_profile": "",
            "reason": "",
        }
        self._last_mission_trend_feedback: Dict[str, Any] = {
            "status": "idle",
            "mode": "stable",
            "trend_pressure": 0.0,
            "last_updated_at": "",
        }
        self._last_external_reliability_analysis: Dict[str, Any] = {
            "status": "idle",
            "generated_at": "",
            "volatility_mode": "",
            "volatility_index": 0.0,
            "at_risk_count": 0,
            "provider_count": 0,
            "drift_mode": "",
            "drift_score": 0.0,
            "provider_policy_changed": False,
            "provider_policy_updated_count": 0,
        }
        self._last_runtime_policy_telemetry_feedback: Dict[str, Any] = {
            "status": "idle",
            "mode": "stable",
            "pressure": 0.0,
            "failure_ratio": 0.0,
            "event_rate_pressure": 0.0,
            "sample_count": 0,
            "updated_at": "",
        }
        self._runtime_policy_signal_state: Dict[str, Dict[str, Any]] = {}
        self._last_oauth_maintenance: Dict[str, Any] = {
            "status": "idle",
            "last_run_at": "",
            "candidate_count": 0,
            "refreshed_count": 0,
            "error_count": 0,
        }
        self._trigger_metadata_key = "__jarvis_trigger_id"
        self._goal_metadata_key = "__jarvis_goal_id"
        self._mission_metadata_key = "__jarvis_mission_id"
        self._policy_bandit_task_class_key = "__jarvis_policy_bandit_task_class"
        self._policy_bandit_profile_key = "__jarvis_policy_bandit_profile"

    async def start(self) -> None:
        if self._running:
            return
        recovery = self.goal_manager.recovery_summary()
        if int(recovery.get("requeued_count", 0)) > 0 or int(recovery.get("recovered_running_count", 0)) > 0:
            self.telemetry.emit("goal.recovered", recovery)
            self.log.info(f"Recovered goal queue state: {recovery}")
        self._running = True
        self._worker = asyncio.create_task(self._loop(), name="agent-kernel-loop")
        self.log.info("Agent kernel started.")

    async def stop(self) -> None:
        self._running = False
        for task in list(self._background_tasks):
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*list(self._background_tasks), return_exceptions=True)
            self._background_tasks.clear()
        self._pending_auto_resume_missions.clear()
        self._runtime_policy_signal_state.clear()
        if self._worker:
            self._worker.cancel()
            try:
                await self._worker
            except asyncio.CancelledError:
                pass
        self.log.info("Agent kernel stopped.")

    async def submit_goal(self, text: str, source: str = "user", metadata: Optional[Dict[str, object]] = None) -> str:
        requested_metadata = dict(metadata) if isinstance(metadata, dict) else {}
        explicit_profile = str(requested_metadata.get("policy_profile", "")).strip().lower()
        task_class = self._infer_policy_task_class(text=text, source=source)
        requested_metadata[self._policy_bandit_task_class_key] = task_class
        bandit_selected_profile = ""
        bandit_selection: Dict[str, Any] = {}

        candidate_profiles = self._policy_bandit_candidate_profiles(source_name=source)
        if not explicit_profile and candidate_profiles:
            bandit_selection = self.policy_bandit.choose_profile(
                task_class=task_class,
                candidate_profiles=candidate_profiles,
                metadata={"source": source},
            )
            if isinstance(bandit_selection, dict):
                selected = str(bandit_selection.get("selected_profile", "")).strip().lower()
                if selected:
                    requested_metadata["policy_profile"] = selected
                    requested_metadata[self._policy_bandit_profile_key] = selected
                    bandit_selected_profile = selected

        effective_metadata = self.policy.decorate_metadata_with_defaults(source_name=source, metadata=requested_metadata)
        if self._policy_bandit_task_class_key not in effective_metadata:
            effective_metadata[self._policy_bandit_task_class_key] = task_class
        if self._policy_bandit_profile_key not in effective_metadata:
            resolved_profile = str(effective_metadata.get("policy_profile", "")).strip().lower()
            if resolved_profile:
                effective_metadata[self._policy_bandit_profile_key] = resolved_profile
        if "recovery_profile" not in effective_metadata:
            policy_profile = str(effective_metadata.get("policy_profile", "")).strip().lower()
            default_recovery = self._default_recovery_profile(policy_profile)
            if default_recovery:
                effective_metadata["recovery_profile"] = default_recovery
        execution_strategy = self.execution_strategy.recommend(
            task_class=task_class,
            source_name=source,
            metadata=effective_metadata,
        )
        strategy_applied = self._apply_execution_strategy_recommendation(
            metadata=effective_metadata,
            recommendation=execution_strategy,
        )
        if strategy_applied:
            self.telemetry.emit(
                "execution_strategy.applied",
                {
                    "source": source,
                    "task_class": task_class,
                    "mode": str(execution_strategy.get("mode", "")),
                    "confidence": float(execution_strategy.get("confidence", 0.0) or 0.0),
                    "overrides": dict(strategy_applied),
                },
            )
        budget = self._resolve_goal_budget(source_name=source, metadata=effective_metadata)
        effective_metadata["max_runtime_s"] = budget["max_runtime_s"]
        effective_metadata["max_steps"] = budget["max_steps"]
        requested_mission_id = str(effective_metadata.get(self._mission_metadata_key, "")).strip()
        request = GoalRequest(text=text, source=source, metadata=effective_metadata)
        goal = GoalRecord(goal_id=str(uuid.uuid4()), request=request)

        mission = self.mission_control.create_for_goal(
            goal_id=goal.goal_id,
            text=text,
            source=source,
            metadata=effective_metadata,
            mission_id=requested_mission_id,
        )
        effective_metadata[self._mission_metadata_key] = mission.mission_id
        request.metadata = effective_metadata

        await self.goal_manager.enqueue(goal)
        self.telemetry.emit(
            "goal.enqueued",
            {
                "goal_id": goal.goal_id,
                "source": source,
                "mission_id": mission.mission_id,
                "policy_profile": str(effective_metadata.get("policy_profile", "")).strip().lower(),
                "policy_task_class": task_class,
                "policy_bandit_profile": bandit_selected_profile,
            },
        )
        if bandit_selected_profile:
            self.telemetry.emit(
                "policy.bandit_selected",
                {
                    "goal_id": goal.goal_id,
                    "source": source,
                    "task_class": task_class,
                    "selected_profile": bandit_selected_profile,
                    "candidates": bandit_selection.get("candidates", []) if isinstance(bandit_selection, dict) else [],
                },
            )
        return goal.goal_id

    def get_goal(self, goal_id: str) -> Optional[GoalRecord]:
        return self.goal_manager.get(goal_id)

    def list_goals(self, *, status: Optional[str] = None, limit: int = 100) -> Dict[str, object]:
        bounded_limit = max(1, min(int(limit), 1000))
        normalized_status = str(status or "").strip().lower()

        rows: list[Dict[str, object]] = []
        for goal in self.goal_manager.all_goals().values():
            goal_status = str(goal.status.value if hasattr(goal.status, "value") else goal.status).strip().lower()
            if normalized_status and goal_status != normalized_status:
                continue

            results = goal.results if isinstance(goal.results, list) else []
            actions = [
                str(item.action).strip()
                for item in results
                if hasattr(item, "action") and str(item.action).strip()
            ]

            row: Dict[str, object] = {
                "goal_id": goal.goal_id,
                "status": goal_status,
                "source": str(goal.request.source or "").strip(),
                "text": str(goal.request.text or "").strip(),
                "created_at": str(goal.request.created_at or ""),
                "started_at": str(goal.started_at or ""),
                "completed_at": str(goal.completed_at or ""),
                "failure_reason": str(goal.failure_reason or ""),
                "plan_id": str(goal.plan.plan_id if goal.plan else ""),
                "result_count": len(results),
                "actions": actions,
                "mission_id": self.mission_control.mission_for_goal(goal.goal_id),
            }
            rows.append(row)

        rows.sort(
            key=lambda item: (
                str(item.get("created_at", "")),
                str(item.get("goal_id", "")),
            ),
            reverse=True,
        )
        items = rows[:bounded_limit]
        return {"items": items, "count": len(items), "total": len(rows)}

    def cancel_goal(self, goal_id: str, reason: str = "Cancelled by user request.") -> Dict[str, object]:
        ok, message, goal = self.goal_manager.request_cancel(goal_id, reason=reason)
        payload: Dict[str, object] = {"status": "success" if ok else "error", "message": message}
        if goal:
            payload["goal"] = {
                "goal_id": goal.goal_id,
                "status": goal.status.value,
                "failure_reason": goal.failure_reason or "",
            }
        if ok:
            self.telemetry.emit("goal.cancel_requested", {"goal_id": goal_id, "reason": reason})
        return payload

    def explain_goal(self, goal_id: str, *, include_memory_hints: bool = True) -> Dict[str, Any]:
        clean_goal_id = str(goal_id or "").strip()
        if not clean_goal_id:
            return {"status": "error", "message": "goal id is required"}

        goal = self.goal_manager.get(clean_goal_id)
        if goal is None:
            return {"status": "error", "message": "Goal not found"}

        status_text = str(goal.status.value if hasattr(goal.status, "value") else goal.status).strip().lower()
        results = goal.results if isinstance(goal.results, list) else []

        result_counts: Counter[str] = Counter()
        action_counts: Counter[str] = Counter()
        failed_actions: Counter[str] = Counter()
        failure_rows: list[Dict[str, Any]] = []
        for row in results:
            if not isinstance(row, ActionResult):
                continue
            row_status = str(row.status or "").strip().lower() or "unknown"
            action = str(row.action or "").strip() or "unknown"
            result_counts[row_status] += 1
            action_counts[action] += 1
            if row_status in {"failed", "blocked"}:
                failed_actions[action] += 1
                failure_rows.append(
                    {
                        "action": action,
                        "status": row_status,
                        "attempt": int(row.attempt or 1),
                        "error": str(row.error or "").strip(),
                        "completed_at": str(row.completed_at or ""),
                    }
                )

        failure_rows.sort(
            key=lambda item: (str(item.get("completed_at", "")), int(item.get("attempt", 0))),
            reverse=True,
        )

        mission_id = self.mission_control.mission_for_goal(clean_goal_id)
        mission = self.mission_control.get(mission_id) if mission_id else None
        mission_diagnostics = self.mission_control.diagnostics(mission_id, hotspot_limit=6) if mission_id else {}
        mission_resume = self.mission_control.resume_preview(mission_id) if mission_id else {}
        rollback_ready = self.rollback_manager.list_entries(status="ready", goal_id=clean_goal_id, limit=30)

        plan_context = goal.plan.context if goal.plan and isinstance(goal.plan.context, dict) else {}
        planner_mode = str(plan_context.get("planner_mode", "")).strip()
        planner_provider = str(plan_context.get("planner_provider", "")).strip()
        policy_profile = str(plan_context.get("policy_profile", "") or goal.request.metadata.get("policy_profile", "")).strip()
        recovery_profile = str(goal.request.metadata.get("recovery_profile", "")).strip()

        recommendations: list[str] = []
        if status_text in {"failed", "blocked"}:
            recommendations.append("Inspect failed actions and mission diagnostics before rerunning.")
            if isinstance(mission_resume, dict) and str(mission_resume.get("status", "")).strip().lower() == "success":
                recommendations.append("Resume the mission from the stored resume cursor instead of restarting from scratch.")
        if int(rollback_ready.get("count", 0) or 0) > 0:
            recommendations.append("Rollback entries are available for this goal; run a dry-run rollback first if needed.")
        if not recommendations and status_text == "completed":
            recommendations.append("Goal completed successfully. Consider saving it as a macro for reuse.")
        if not recommendations:
            recommendations.append("Goal is still in progress; monitor telemetry and mission timeline for drift.")

        memory_hints: Dict[str, Any] = {}
        if include_memory_hints:
            memory_hints = {
                "runtime": self.runtime_memory.search(goal.request.text, limit=5),
                "episodic": self.episodic_memory.search(
                    goal.request.text,
                    limit=6,
                    exclude_goal_ids=[clean_goal_id],
                ),
                "strategy": self.episodic_memory.strategy(goal.request.text, limit=8),
            }

        return {
            "status": "success",
            "goal_id": clean_goal_id,
            "goal": {
                "status": status_text,
                "source": str(goal.request.source or "").strip(),
                "text": str(goal.request.text or "").strip(),
                "created_at": str(goal.request.created_at or ""),
                "started_at": str(goal.started_at or ""),
                "completed_at": str(goal.completed_at or ""),
                "failure_reason": str(goal.failure_reason or ""),
                "result_count": len(results),
            },
            "plan": {
                "plan_id": str(goal.plan.plan_id if goal.plan else ""),
                "intent": str(goal.plan.intent if goal.plan else ""),
                "step_count": len(goal.plan.steps) if goal.plan and isinstance(goal.plan.steps, list) else 0,
                "planner_mode": planner_mode,
                "planner_provider": planner_provider,
                "policy_profile": policy_profile,
                "recovery_profile": recovery_profile,
            },
            "results": {
                "status_counts": dict(result_counts),
                "action_counts": dict(action_counts),
                "failed_action_counts": dict(failed_actions),
                "recent_failures": failure_rows[:8],
            },
            "mission": {
                "mission_id": mission_id,
                "record": mission if isinstance(mission, dict) else None,
                "diagnostics": mission_diagnostics if isinstance(mission_diagnostics, dict) else {},
                "resume_preview": mission_resume if isinstance(mission_resume, dict) else {},
            },
            "rollback": rollback_ready,
            "memory_hints": memory_hints,
            "recommendations": recommendations,
        }

    def autonomy_report(self, *, limit_recent_goals: int = 250) -> Dict[str, Any]:
        bounded_limit = max(20, min(int(limit_recent_goals), 2000))
        all_goals = list(self.goal_manager.all_goals().values())
        all_goals.sort(key=lambda item: str(item.request.created_at or ""), reverse=True)
        recent_goals = all_goals[:bounded_limit]

        status_counts: Counter[str] = Counter()
        action_failure_counts: Counter[str] = Counter()
        action_run_counts: Counter[str] = Counter()
        remediation_action_counts: Counter[str] = Counter()
        remediation_attempted = 0
        remediation_success = 0
        for goal in recent_goals:
            status_text = str(goal.status.value if hasattr(goal.status, "value") else goal.status).strip().lower() or "unknown"
            status_counts[status_text] += 1
            rows = goal.results if isinstance(goal.results, list) else []
            for row in rows:
                if not isinstance(row, ActionResult):
                    continue
                action = str(row.action or "").strip() or "unknown"
                row_status = str(row.status or "").strip().lower() or "unknown"
                action_run_counts[action] += 1
                if row_status in {"failed", "blocked"}:
                    action_failure_counts[action] += 1
                evidence = row.evidence if isinstance(row.evidence, dict) else {}
                remediation = evidence.get("external_remediation", {})
                if isinstance(remediation, dict):
                    actions = remediation.get("actions", [])
                    action_rows = actions if isinstance(actions, list) else []
                    for item in action_rows:
                        if not isinstance(item, dict):
                            continue
                        rem_action = str(item.get("action", "")).strip().lower()
                        rem_status = str(item.get("status", "")).strip().lower()
                        if rem_action:
                            remediation_action_counts[rem_action] += 1
                        if rem_status not in {"skipped", ""}:
                            remediation_attempted += 1
                        if rem_status == "success":
                            remediation_success += 1

        completed = int(status_counts.get("completed", 0))
        failed = int(status_counts.get("failed", 0))
        blocked = int(status_counts.get("blocked", 0))
        cancelled = int(status_counts.get("cancelled", 0))
        terminal = max(1, completed + failed + blocked + cancelled)

        failure_pressure = (failed + blocked + cancelled) / terminal
        success_rate = completed / terminal

        breaker_snapshot = self.action_circuit_breaker.snapshot(limit=500)
        breaker_items = breaker_snapshot.get("items", [])
        if not isinstance(breaker_items, list):
            breaker_items = []
        open_breakers = [
            item
            for item in breaker_items
            if isinstance(item, dict) and str(item.get("open_until", "")).strip()
        ]
        open_breaker_count = len(open_breakers)
        total_breakers = max(1, int(breaker_snapshot.get("total", len(breaker_items)) or len(breaker_items) or 1))
        open_breaker_pressure = open_breaker_count / total_breakers

        pending_approvals = self.approval_gate.pending_count()
        approval_pressure = min(1.0, pending_approvals / 25.0)

        pending_schedules = self.schedule_manager.pending_count()
        active_triggers = self.trigger_manager.active_count()
        running_missions = int(self.list_missions(status="running", limit=500).get("count", 0) or 0)
        external_reliability = self.external_reliability.snapshot(limit=300)
        external_items = external_reliability.get("items", []) if isinstance(external_reliability, dict) else []
        external_degraded = []
        for row in external_items if isinstance(external_items, list) else []:
            if not isinstance(row, dict):
                continue
            failure_ema = self._coerce_float(row.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            if failure_ema >= 0.42:
                external_degraded.append(row)
        guardrail_snapshot = self.policy.guardrail_snapshot(limit=500, min_samples=1)
        guardrail_items = guardrail_snapshot.get("items", []) if isinstance(guardrail_snapshot, dict) else []
        unstable_guardrail_actions: list[Dict[str, Any]] = []
        critical_guardrail_actions: list[Dict[str, Any]] = []
        for row in guardrail_items if isinstance(guardrail_items, list) else []:
            if not isinstance(row, dict):
                continue
            unstable_score = self._coerce_float(row.get("unstable_score", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            if unstable_score >= float(self.policy.guardrails_warn_unstable):
                unstable_guardrail_actions.append(row)
            if unstable_score >= float(self.policy.guardrails_block_unstable_high):
                critical_guardrail_actions.append(row)

        runtime_hints = self.runtime_memory.recent_hints(limit=120)
        episodic_stats = self.episodic_memory.stats()
        anchor_memory = self.desktop_anchor_memory.snapshot(limit=260)
        anchor_items = anchor_memory.get("items", []) if isinstance(anchor_memory, dict) else []
        token_inventory = self.oauth_store.list(limit=500)
        token_count = int(token_inventory.get("count", 0) or 0)
        policy_bandit = self.policy_bandit.snapshot(limit=260)
        bandit_items = policy_bandit.get("items", []) if isinstance(policy_bandit, dict) else []
        execution_strategy = self.execution_strategy.snapshot(limit=260)
        execution_strategy_items = execution_strategy.get("items", []) if isinstance(execution_strategy, dict) else []

        reliability_score = 100.0 * (1.0 - ((failure_pressure * 0.56) + (open_breaker_pressure * 0.31) + (approval_pressure * 0.13)))
        reliability_score = max(0.0, min(100.0, reliability_score))

        automation_depth = min(1.0, (pending_schedules + active_triggers + running_missions) / 24.0)
        memory_health = min(1.0, (len(runtime_hints) / 120.0) * 0.35 + (int(episodic_stats.get("count", 0) or 0) / 4000.0) * 0.65)
        integration_health = 1.0 if token_count > 0 else 0.45
        autonomy_score = 100.0 * (
            (success_rate * 0.43)
            + ((1.0 - open_breaker_pressure) * 0.23)
            + (memory_health * 0.16)
            + (integration_health * 0.12)
            + ((1.0 - approval_pressure) * 0.06)
        )
        autonomy_score = max(0.0, min(100.0, autonomy_score))

        if autonomy_score >= 82 and reliability_score >= 78:
            tier = "high"
        elif autonomy_score >= 62 and reliability_score >= 55:
            tier = "medium"
        else:
            tier = "developing"

        recovery_profiles = self.recovery.list_profiles()
        current_recovery_profile = str(recovery_profiles.get("default_profile", "")).strip().lower()
        if open_breaker_count > 0 or failure_pressure >= 0.45:
            recommended_recovery_profile = "safe"
        elif success_rate >= 0.75 and failure_pressure <= 0.18 and open_breaker_count == 0:
            recommended_recovery_profile = "aggressive"
        else:
            recommended_recovery_profile = "balanced"

        recommendations: list[str] = []
        if open_breaker_count > 0:
            recommendations.append("Circuit breakers are open; stabilize with safe recovery profile and rerun failed missions.")
        if failure_pressure >= 0.35:
            recommendations.append("Recent failure pressure is high; review failed_action hotspots and tighten verification.")
        if pending_approvals >= 8:
            recommendations.append("Approval queue is building up; pre-approve safe actions or switch to less restrictive profile where appropriate.")
        if token_count == 0:
            recommendations.append("No OAuth tokens are available; connect providers to unlock external integrations.")
        if external_degraded:
            recommendations.append("External connector reliability is degraded; review provider cooldowns and run token maintenance.")
        if critical_guardrail_actions:
            recommendations.append(
                "Adaptive guardrails marked unstable high-risk actions; reduce autonomy level or harden UI/external verification before retry."
            )
        elif unstable_guardrail_actions:
            recommendations.append("Adaptive guardrails report unstable actions; monitor hotspots and keep verification strictness at least standard.")
        if not recommendations:
            recommendations.append("Backend autonomy is stable. Continue monitoring hotspots and mission diagnostics.")

        action_hotspots = []
        for action, failures in action_failure_counts.most_common(10):
            total_runs = int(action_run_counts.get(action, 0))
            action_hotspots.append(
                {
                    "action": action,
                    "failures": int(failures),
                    "runs": total_runs,
                    "failure_rate": round((failures / max(1, total_runs)), 4),
                }
            )

        return {
            "status": "success",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "window_recent_goals": len(recent_goals),
            "goal_status_counts": dict(status_counts),
            "pressures": {
                "failure_pressure": round(failure_pressure, 6),
                "open_breaker_pressure": round(open_breaker_pressure, 6),
                "approval_pressure": round(approval_pressure, 6),
            },
            "scores": {
                "reliability": round(reliability_score, 2),
                "autonomy": round(autonomy_score, 2),
                "tier": tier,
            },
            "automation": {
                "pending_approvals": pending_approvals,
                "pending_schedules": pending_schedules,
                "active_triggers": active_triggers,
                "running_missions": running_missions,
                "pending_auto_resumes": self.pending_auto_resume_count(),
            },
            "memory": {
                "runtime_hint_count": len(runtime_hints),
                "episodic": episodic_stats,
                "desktop_anchor_memory": {
                    "tracked_anchors": int(anchor_memory.get("total", 0) or 0) if isinstance(anchor_memory, dict) else 0,
                    "quarantine_count": int(anchor_memory.get("quarantine_count", 0) or 0)
                    if isinstance(anchor_memory, dict)
                    else 0,
                    "top_anchor_actions": [
                        str(item.get("action", ""))
                        for item in anchor_items[:10]
                        if isinstance(item, dict) and str(item.get("action", "")).strip()
                    ],
                    "top_quarantined_actions": [
                        str(item.get("action", ""))
                        for item in (
                            anchor_memory.get("quarantine_items", [])
                            if isinstance(anchor_memory.get("quarantine_items", []), list)
                            else []
                        )[:10]
                        if isinstance(item, dict) and str(item.get("action", "")).strip()
                    ],
                },
            },
            "integrations": {
                "oauth_token_count": token_count,
                "oauth_maintenance": self.oauth_maintenance_status(),
                "external_reliability": {
                    "tracked_providers": int(external_reliability.get("total", 0) or 0) if isinstance(external_reliability, dict) else 0,
                    "degraded_providers": len(external_degraded),
                    "mission_outage_mode": str(
                        external_reliability.get("mission_outage_policy", {}).get("mode", "")
                        if isinstance(external_reliability.get("mission_outage_policy", {}), dict)
                        else ""
                    ).strip().lower(),
                    "mission_outage_bias": self._coerce_float(
                        external_reliability.get("mission_outage_policy", {}).get("bias", 0.0)
                        if isinstance(external_reliability.get("mission_outage_policy", {}), dict)
                        else 0.0,
                        minimum=-1.0,
                        maximum=1.0,
                        default=0.0,
                    ),
                    "top_degraded_providers": [
                        str(item.get("provider", ""))
                        for item in external_degraded[:10]
                        if isinstance(item, dict) and str(item.get("provider", "")).strip()
                    ],
                    "remediation_attempted": remediation_attempted,
                    "remediation_success_rate": round(
                        (float(remediation_success) / max(1.0, float(remediation_attempted))),
                        4,
                    ),
                    "top_remediation_actions": [
                        {"action": str(name), "count": int(count)}
                        for name, count in remediation_action_counts.most_common(10)
                    ],
                },
                "policy_bandit": {
                    "tracked_task_classes": int(policy_bandit.get("tracked_task_classes", 0) or 0)
                    if isinstance(policy_bandit, dict)
                    else 0,
                    "top_profiles": [
                        {
                            "task_class": str(item.get("task_class", "")),
                            "profile": str(item.get("profile", "")),
                            "reward_mean": self._coerce_float(item.get("reward_mean", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                            "pulls": self._coerce_int(item.get("pulls", 0), minimum=0, maximum=10_000_000, default=0),
                        }
                        for item in bandit_items[:10]
                        if isinstance(item, dict) and str(item.get("profile", "")).strip()
                    ],
                },
                "execution_strategy": {
                    "tracked_task_classes": int(execution_strategy.get("tracked_task_classes", 0) or 0)
                    if isinstance(execution_strategy, dict)
                    else 0,
                    "global_parallel_cap": int(
                        execution_strategy.get("config", {}).get("global_parallel_cap", 0)
                    )
                    if isinstance(execution_strategy, dict)
                    and isinstance(execution_strategy.get("config", {}), dict)
                    else 0,
                    "top_modes": [
                        {
                            "task_class": str(item.get("task_class", "")),
                            "mode": str(item.get("mode", "")),
                            "pulls": self._coerce_int(item.get("pulls", 0), minimum=0, maximum=10_000_000, default=0),
                        }
                        for item in execution_strategy_items[:10]
                        if isinstance(item, dict) and str(item.get("mode", "")).strip()
                    ],
                },
            },
            "circuit_breakers": {
                "open_count": open_breaker_count,
                "total_count": int(breaker_snapshot.get("total", 0) or 0),
                "open_actions": [str(item.get("action", "")) for item in open_breakers[:20] if isinstance(item, dict)],
            },
            "policy_guardrails": {
                "enabled": bool(guardrail_snapshot.get("enabled", False)) if isinstance(guardrail_snapshot, dict) else False,
                "tracked_count": int(guardrail_snapshot.get("total", 0) or 0) if isinstance(guardrail_snapshot, dict) else 0,
                "unstable_count": len(unstable_guardrail_actions),
                "critical_count": len(critical_guardrail_actions),
                "top_unstable_actions": [
                    str(item.get("action", ""))
                    for item in unstable_guardrail_actions[:20]
                    if isinstance(item, dict) and str(item.get("action", "")).strip()
                ],
            },
            "recovery": {
                "current_profile": current_recovery_profile,
                "recommended_profile": recommended_recovery_profile,
                "profiles": recovery_profiles,
            },
            "action_hotspots": action_hotspots,
            "recommendations": recommendations,
            "last_tune": dict(self._last_autonomy_tune),
            "last_external_reliability_analysis": dict(self._last_external_reliability_analysis),
            "last_runtime_policy_telemetry_feedback": dict(self._last_runtime_policy_telemetry_feedback),
        }

    def runtime_diagnostics_bundle(self, *, limit: int = 200) -> Dict[str, Any]:
        bounded = max(20, min(int(limit), 5000))
        queue_snapshot = self.queue_diagnostics(limit=min(bounded, 1200), include_terminal=False)
        guardrail_snapshot = self.policy.guardrail_snapshot(limit=min(bounded, 500), min_samples=1)
        circuit_snapshot = self.action_circuit_breaker.snapshot(limit=min(bounded, 500))
        external_snapshot = self.external_reliability.snapshot(limit=min(bounded, 500))
        mission_summary: Dict[str, Any] = {"status": "unavailable"}
        risk_snapshot: Dict[str, Any] = {"status": "unavailable"}
        try:
            risk_runtime = getattr(self.policy.risk_engine, "runtime_snapshot", None)
            if callable(risk_runtime):
                risk_snapshot = risk_runtime(limit=min(bounded, 500))
        except Exception as exc:  # noqa: BLE001
            risk_snapshot = {"status": "error", "message": str(exc)}
        try:
            mission_summary = self._summarize_mission_trends(limit=min(bounded, 240))
        except Exception as exc:  # noqa: BLE001
            mission_summary = {"status": "error", "message": str(exc)}

        model_snapshot: Dict[str, Any] = {"status": "unavailable"}
        try:
            model_router = getattr(self.planner, "model_router", None)
            registry = getattr(model_router, "registry", None)
            runtime_snapshot = getattr(registry, "runtime_snapshot", None)
            if callable(runtime_snapshot):
                model_snapshot = runtime_snapshot(limit=min(bounded, 500))
        except Exception as exc:  # noqa: BLE001
            model_snapshot = {"status": "error", "message": str(exc)}

        queue_length = int(queue_snapshot.get("queue_length", 0) or 0) if isinstance(queue_snapshot, dict) else 0
        orphaned_pending = int(queue_snapshot.get("orphaned_pending_count", 0) or 0) if isinstance(queue_snapshot, dict) else 0
        unstable_guardrails = int(guardrail_snapshot.get("count", 0) or 0) if isinstance(guardrail_snapshot, dict) else 0
        open_breakers = int(circuit_snapshot.get("open_count", 0) or 0) if isinstance(circuit_snapshot, dict) else 0
        degraded_providers = 0
        if isinstance(external_snapshot, dict):
            rows = external_snapshot.get("items", [])
            if isinstance(rows, list):
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    failure_ema = self._coerce_float(row.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
                    if failure_ema >= 0.45 or bool(row.get("outage_active", False)) or bool(row.get("cooldown_active", False)):
                        degraded_providers += 1
        mission_policy = external_snapshot.get("mission_outage_policy", {}) if isinstance(external_snapshot, dict) else {}
        mission_policy_pressure = self._coerce_float(
            mission_policy.get("pressure_ema", 0.0) if isinstance(mission_policy, dict) else 0.0,
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        mission_policy_blocked = self._coerce_float(
            mission_policy.get("blocked_ratio_ema", 0.0) if isinstance(mission_policy, dict) else 0.0,
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        mission_trend = mission_summary.get("trend", {}) if isinstance(mission_summary, dict) else {}
        mission_trend_pressure = self._coerce_float(
            mission_trend.get("pressure", 0.0) if isinstance(mission_trend, dict) else 0.0,
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        contract_pressure = self._coerce_float(
            (mission_policy_pressure * 0.56) + (mission_policy_blocked * 0.28) + (mission_trend_pressure * 0.16),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )

        readiness = 1.0
        readiness -= min(0.35, queue_length / 600.0)
        readiness -= min(0.2, orphaned_pending / 80.0)
        readiness -= min(0.2, open_breakers / 18.0)
        readiness -= min(0.15, degraded_providers / 12.0)
        readiness -= min(0.1, unstable_guardrails / 250.0)
        readiness -= min(0.14, contract_pressure * 0.22)
        readiness = max(0.0, min(1.0, readiness))
        level = "healthy"
        if readiness < 0.7:
            level = "degraded"
        if readiness < 0.45:
            level = "critical"

        pressure = self._coerce_float(
            (min(1.0, queue_length / 480.0) * 0.3)
            + (min(1.0, open_breakers / 12.0) * 0.24)
            + (min(1.0, degraded_providers / 8.0) * 0.22)
            + (contract_pressure * 0.24),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        alerts: list[Dict[str, Any]] = []
        if queue_length >= 180:
            alerts.append(
                {
                    "severity": "high",
                    "code": "queue_backlog_high",
                    "message": f"Action queue backlog is elevated ({queue_length} pending steps).",
                    "hint": "Prioritize critical missions and reduce low-value scheduled jobs.",
                }
            )
        if open_breakers > 0:
            alerts.append(
                {
                    "severity": "medium" if open_breakers < 4 else "high",
                    "code": "circuit_breakers_open",
                    "message": f"{open_breakers} action circuit breaker(s) are open.",
                    "hint": "Review breaker diagnostics and trigger targeted recovery before retries.",
                }
            )
        if contract_pressure >= 0.5:
            alerts.append(
                {
                    "severity": "high" if contract_pressure >= 0.7 else "medium",
                    "code": "external_contract_pressure",
                    "message": "External provider contract pressure is elevated.",
                    "hint": "Run connector preflight remediation and tighten verification strictness.",
                }
            )
        if unstable_guardrails >= 24:
            alerts.append(
                {
                    "severity": "medium",
                    "code": "guardrail_instability",
                    "message": f"Adaptive guardrails report {unstable_guardrails} unstable actions.",
                    "hint": "Quarantine unstable anchors and enforce strict verification for affected actions.",
                }
            )
        recommendations: list[str] = []
        if pressure >= 0.58:
            recommendations.append("Switch to stability-oriented recovery profile and reduce concurrency until pressure drops.")
        if contract_pressure >= 0.46:
            recommendations.append("Increase connector preflight strictness and enforce provider cooldown windows.")
        if degraded_providers > 0 and open_breakers > 0:
            recommendations.append("Correlated provider and breaker instability detected; route high-risk actions through fallback providers.")
        if not recommendations:
            recommendations.append("Runtime is stable; continue autonomous execution with current policy profile.")

        return {
            "status": "success",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "readiness": {"score": round(readiness, 6), "level": level},
            "pressure": {
                "score": round(pressure, 6),
                "contract": round(contract_pressure, 6),
                "mission_trend": round(mission_trend_pressure, 6),
                "mission_policy": round(mission_policy_pressure, 6),
            },
            "alerts": alerts,
            "recommendations": recommendations,
            "queue": queue_snapshot,
            "guardrails": guardrail_snapshot,
            "risk_runtime": risk_snapshot,
            "model_runtime": model_snapshot,
            "circuit_breakers": circuit_snapshot,
            "external_reliability": external_snapshot,
            "mission_trends": mission_summary,
        }

    def autonomy_tune(self, *, dry_run: bool = False, reason: str = "manual") -> Dict[str, Any]:
        report = self.autonomy_report(limit_recent_goals=400)
        mission_summary = self._summarize_mission_trends(limit=220)
        recovery_section = report.get("recovery", {})
        if not isinstance(recovery_section, dict):
            recovery_section = {}
        current_profile = str(recovery_section.get("current_profile", "")).strip().lower()
        target_profile = str(recovery_section.get("recommended_profile", "")).strip().lower()
        telemetry_feedback = self._runtime_policy_telemetry_feedback(force=True)
        telemetry_mode = str(telemetry_feedback.get("mode", "")).strip().lower() if isinstance(telemetry_feedback, dict) else ""
        telemetry_pressure = self._coerce_float(
            telemetry_feedback.get("pressure", 0.0) if isinstance(telemetry_feedback, dict) else 0.0,
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        telemetry_guardrail_applied = False
        if telemetry_mode == "severe" or telemetry_pressure >= 0.72:
            if target_profile in {"aggressive", "balanced"}:
                target_profile = "safe"
                telemetry_guardrail_applied = True
        elif telemetry_mode == "moderate" or telemetry_pressure >= 0.52:
            if target_profile == "aggressive":
                target_profile = "balanced"
                telemetry_guardrail_applied = True
        clean_reason = str(reason or "").strip() or "manual"

        changed = False
        message = "No recovery profile change required."
        status = "success"

        if target_profile and target_profile != current_profile:
            if dry_run:
                message = f"Would switch recovery profile from {current_profile or 'unknown'} to {target_profile}."
            else:
                ok, update_message, selected = self.recovery.set_default_profile(target_profile)
                status = "success" if ok else "error"
                message = update_message
                changed = bool(ok and selected != current_profile)
                target_profile = selected
                if ok:
                    self.telemetry.emit(
                        "autonomy.tuned",
                        {
                            "reason": clean_reason,
                            "changed": changed,
                            "from_profile": current_profile,
                            "to_profile": target_profile,
                        },
                    )

        policy_tuning = self.policy.tune_from_operational_signals(
            autonomy_report=report,
            mission_summary=mission_summary,
            dry_run=dry_run,
            reason=clean_reason,
        )
        if isinstance(policy_tuning, dict) and bool(policy_tuning.get("changed", False)):
            if not dry_run:
                changed = True
            self.telemetry.emit(
                "policy.autotuned",
                {
                    "reason": clean_reason,
                    "dry_run": bool(dry_run),
                    "mode": str(policy_tuning.get("mode", "")),
                    "changed": bool(policy_tuning.get("changed", False)),
                },
            )

        bandit_tuning = self.policy_bandit.tune_from_operational_signals(
            autonomy_report=report,
            mission_summary=mission_summary,
            dry_run=dry_run,
            reason=clean_reason,
        )
        if isinstance(bandit_tuning, dict) and bool(bandit_tuning.get("changed", False)):
            if not dry_run:
                changed = True
            self.telemetry.emit(
                "policy.bandit_autotuned",
                {
                    "reason": clean_reason,
                    "dry_run": bool(dry_run),
                    "mode": str(bandit_tuning.get("mode", "")),
                    "changed": bool(bandit_tuning.get("changed", False)),
                },
            )

        execution_strategy_tuning = self.execution_strategy.tune_from_operational_signals(
            autonomy_report=report,
            mission_summary=mission_summary,
            dry_run=dry_run,
            reason=clean_reason,
        )
        if isinstance(execution_strategy_tuning, dict) and bool(execution_strategy_tuning.get("changed", False)):
            if not dry_run:
                changed = True
            self.telemetry.emit(
                "execution_strategy.autotuned",
                {
                    "reason": clean_reason,
                    "dry_run": bool(dry_run),
                    "mode": str(execution_strategy_tuning.get("mode", "")),
                    "changed": bool(execution_strategy_tuning.get("changed", False)),
                },
            )

        external_reliability_tuning = self.external_reliability.tune_from_operational_signals(
            autonomy_report=report,
            mission_summary=mission_summary,
            dry_run=dry_run,
            reason=clean_reason,
        )
        if isinstance(external_reliability_tuning, dict) and bool(external_reliability_tuning.get("changed", False)):
            if not dry_run:
                changed = True
            self.telemetry.emit(
                "external_reliability.autotuned",
                {
                    "reason": clean_reason,
                    "dry_run": bool(dry_run),
                    "mode": str(external_reliability_tuning.get("mode", "")),
                    "changed": bool(external_reliability_tuning.get("changed", False)),
                },
            )

        mission_reliability_analysis = self.external_reliability_mission_analysis(
            provider_limit=self._coerce_int(
                getattr(self, "external_reliability_analysis_provider_limit", 260),
                minimum=20,
                maximum=5000,
                default=260,
            ),
            history_limit=self._coerce_int(
                getattr(self, "external_reliability_analysis_history_limit", 40),
                minimum=8,
                maximum=400,
                default=40,
            ),
            record=True,
        )
        provider_policy_tuning = (
            mission_reliability_analysis.get("provider_policy_tuning", {})
            if isinstance(mission_reliability_analysis, dict)
            and isinstance(mission_reliability_analysis.get("provider_policy_tuning", {}), dict)
            else {}
        )
        if bool(provider_policy_tuning.get("changed", False)):
            if not dry_run:
                changed = True

        row = {
            "status": status,
            "last_run_at": datetime.now(timezone.utc).isoformat(),
            "changed": changed,
            "target_profile": target_profile,
            "reason": clean_reason,
            "dry_run": bool(dry_run),
            "policy_mode": str(policy_tuning.get("mode", "")) if isinstance(policy_tuning, dict) else "",
            "policy_changed": bool(policy_tuning.get("changed", False)) if isinstance(policy_tuning, dict) else False,
            "policy_bandit_mode": str(bandit_tuning.get("mode", "")) if isinstance(bandit_tuning, dict) else "",
            "policy_bandit_changed": bool(bandit_tuning.get("changed", False)) if isinstance(bandit_tuning, dict) else False,
            "execution_strategy_mode": str(execution_strategy_tuning.get("mode", "")) if isinstance(execution_strategy_tuning, dict) else "",
            "execution_strategy_changed": bool(execution_strategy_tuning.get("changed", False))
            if isinstance(execution_strategy_tuning, dict)
            else False,
            "external_reliability_mode": str(external_reliability_tuning.get("mode", ""))
            if isinstance(external_reliability_tuning, dict)
            else "",
            "external_reliability_changed": bool(external_reliability_tuning.get("changed", False))
            if isinstance(external_reliability_tuning, dict)
            else False,
            "external_provider_policy_changed": bool(provider_policy_tuning.get("changed", False)),
            "external_provider_policy_updated_count": self._coerce_int(
                provider_policy_tuning.get("updated_count", 0),
                minimum=0,
                maximum=100_000,
                default=0,
            ),
            "telemetry_mode": telemetry_mode,
            "telemetry_pressure": round(telemetry_pressure, 6),
            "telemetry_guardrail_applied": bool(telemetry_guardrail_applied),
        }
        self._last_autonomy_tune = row

        return {
            **row,
            "message": message,
            "current_profile": current_profile,
            "report": report,
            "mission_summary": mission_summary,
            "policy_tuning": policy_tuning,
            "policy_bandit_tuning": bandit_tuning,
            "execution_strategy_tuning": execution_strategy_tuning,
            "external_reliability_tuning": external_reliability_tuning,
            "external_reliability_analysis": mission_reliability_analysis,
            "external_provider_policy_tuning": provider_policy_tuning,
            "telemetry_feedback": telemetry_feedback,
        }

    async def preview_plan(
        self,
        *,
        text: str,
        source: str = "desktop-ui",
        metadata: Optional[Dict[str, object]] = None,
    ) -> Dict[str, Any]:
        clean_text = str(text or "").strip()
        if not clean_text:
            raise ValueError("text is required")

        effective_metadata = self.policy.decorate_metadata_with_defaults(source_name=source, metadata=metadata)
        request = GoalRequest(text=clean_text, source=source, metadata=effective_metadata)
        transient_goal = GoalRecord(goal_id=str(uuid.uuid4()), request=request)
        lexical_hints = self.runtime_memory.search(clean_text, limit=5)
        episodic_hints = self.episodic_memory.search(clean_text, limit=6)
        episodic_strategy = self.episodic_memory.strategy(clean_text, limit=10)
        context: Dict[str, object] = {
            "source": source,
            "replan_attempt": 0,
            "recent_goal_hints": self.runtime_memory.recent_hints(limit=8),
            "retrieved_memory_hints": lexical_hints,
            "retrieved_episodic_hints": episodic_hints,
            "retrieved_hybrid_hints": self._merge_memory_hints(lexical_hints, episodic_hints, limit=10),
            "retrieved_episodic_strategy": episodic_strategy,
            "desktop_state_hints": self.desktop_state.hints(limit=6),
            "desktop_anchor_hints": self.desktop_anchor_memory.hints(query=clean_text, limit=6),
        }
        context.update(self._planner_reliability_context())
        failure_clusters = self._external_failure_clusters(
            goal_text=clean_text,
            context=context,
            limit=self._coerce_int(
                getattr(self, "runtime_policy_failure_cluster_limit", 8),
                minimum=1,
                maximum=40,
                default=8,
            ),
        )
        if failure_clusters:
            context["external_failure_clusters"] = failure_clusters
        plan = await self.planner.build_plan(transient_goal, context=context)

        risk_rank = {"unknown": 0, "low": 1, "medium": 2, "high": 3}
        top_risk = "low"
        steps: list[Dict[str, Any]] = []
        for index, step in enumerate(plan.steps, start=1):
            definition = self.registry.get(step.action)
            step_risk = str(getattr(definition, "risk", "unknown") or "unknown")
            if risk_rank.get(step_risk, 0) > risk_rank.get(top_risk, 0):
                top_risk = step_risk
            steps.append(
                {
                    "index": index,
                    "step_id": step.step_id,
                    "action": step.action,
                    "args": step.args,
                    "depends_on": step.depends_on,
                    "risk": step_risk,
                    "requires_confirmation": bool(getattr(definition, "requires_confirmation", False)),
                    "description": str(getattr(definition, "description", "") or ""),
                    "verify": step.verify,
                }
            )

        suggested_macros = self.macro_manager.list(query=clean_text, limit=3)
        diagnostics = self._analyze_plan_readiness(plan)

        return {
            "status": "success",
            "plan": {
                "plan_id": plan.plan_id,
                "goal_id": plan.goal_id,
                "intent": plan.intent,
                "created_at": plan.created_at,
                "planner_mode": plan.context.get("planner_mode", ""),
                "planner_provider": plan.context.get("planner_provider", ""),
                "planner_model": plan.context.get("planner_model", ""),
                "step_count": len(steps),
                "risk": top_risk,
                "steps": steps,
            },
            "diagnostics": diagnostics,
            "suggested_macros": suggested_macros,
        }

    def list_macros(self, *, query: str = "", limit: int = 100) -> Dict[str, object]:
        rows = self.macro_manager.list(query=query, limit=limit)
        return {"items": rows, "count": len(rows), "query": str(query or "")}

    def get_macro(self, macro_id: str) -> Optional[Dict[str, object]]:
        record = self.macro_manager.get(macro_id)
        return record.to_dict() if record else None

    async def run_macro(
        self,
        macro_id: str,
        source: str = "desktop-ui",
        metadata: Optional[Dict[str, object]] = None,
    ) -> Dict[str, object]:
        record = self.macro_manager.get(macro_id)
        if not record:
            return {"status": "error", "message": "Macro not found"}
        used = self.macro_manager.mark_used(macro_id)
        self.telemetry.emit(
            "macro.run_requested",
            {
                "macro_id": macro_id,
                "source": source,
                "usage_count": used.usage_count if used else record.usage_count,
            },
        )
        enriched_metadata = dict(metadata) if isinstance(metadata, dict) else {}
        enriched_metadata["macro_id"] = macro_id
        goal_id = await self.submit_goal(text=record.text, source=source, metadata=enriched_metadata)
        self.telemetry.emit(
            "macro.run_enqueued",
            {
                "macro_id": macro_id,
                "goal_id": goal_id,
                "source": source,
            },
        )
        return {"status": "success", "goal_id": goal_id, "macro": record.to_dict()}

    def schedule_goal(
        self,
        *,
        text: str,
        run_at: str,
        source: str = "desktop-schedule",
        metadata: Optional[Dict[str, object]] = None,
        max_attempts: int = 3,
        retry_delay_s: int = 60,
        repeat_interval_s: int = 0,
    ) -> Dict[str, object]:
        record = self.schedule_manager.create(
            text=text,
            run_at=run_at,
            source=source,
            metadata=metadata if isinstance(metadata, dict) else {},
            max_attempts=max_attempts,
            retry_delay_s=retry_delay_s,
            repeat_interval_s=repeat_interval_s,
        )
        self.telemetry.emit(
            "schedule.created",
            {
                "schedule_id": record.schedule_id,
                "run_at": record.run_at,
                "source": record.source,
                "repeat_interval_s": record.repeat_interval_s,
            },
        )
        return record.to_dict()

    def get_schedule(self, schedule_id: str) -> Optional[Dict[str, object]]:
        record = self.schedule_manager.get(schedule_id)
        return record.to_dict() if record else None

    def list_schedules(self, *, status: str | None = None, limit: int = 200) -> Dict[str, object]:
        rows = self.schedule_manager.list(status=status, limit=limit)
        return {"items": rows, "count": len(rows)}

    def cancel_schedule(self, schedule_id: str) -> Dict[str, object]:
        ok, message, record = self.schedule_manager.cancel(schedule_id)
        payload: Dict[str, object] = {"status": "success" if ok else "error", "message": message}
        if record:
            payload["schedule"] = record.to_dict()
            self.telemetry.emit(
                "schedule.cancelled",
                {
                    "schedule_id": record.schedule_id,
                    "status": record.status,
                },
            )
        return payload

    def pause_schedule(self, schedule_id: str) -> Dict[str, object]:
        ok, message, record = self.schedule_manager.pause(schedule_id)
        payload: Dict[str, object] = {"status": "success" if ok else "error", "message": message}
        if record:
            payload["schedule"] = record.to_dict()
            self.telemetry.emit(
                "schedule.paused",
                {
                    "schedule_id": record.schedule_id,
                    "status": record.status,
                },
            )
        return payload

    def resume_schedule(self, schedule_id: str) -> Dict[str, object]:
        ok, message, record = self.schedule_manager.resume(schedule_id)
        payload: Dict[str, object] = {"status": "success" if ok else "error", "message": message}
        if record:
            payload["schedule"] = record.to_dict()
            self.telemetry.emit(
                "schedule.resumed",
                {
                    "schedule_id": record.schedule_id,
                    "status": record.status,
                },
            )
        return payload

    def run_schedule_now(self, schedule_id: str) -> Dict[str, object]:
        ok, message, record = self.schedule_manager.run_now(schedule_id)
        payload: Dict[str, object] = {"status": "success" if ok else "error", "message": message}
        if record:
            payload["schedule"] = record.to_dict()
            self.telemetry.emit(
                "schedule.run_now",
                {
                    "schedule_id": record.schedule_id,
                    "status": record.status,
                    "next_run_at": record.next_run_at,
                },
            )
        return payload

    def create_trigger(
        self,
        *,
        text: str,
        interval_s: int,
        start_at: str = "",
        source: str = "desktop-trigger",
        metadata: Optional[Dict[str, object]] = None,
    ) -> Dict[str, object]:
        record = self.trigger_manager.create(
            text=text,
            interval_s=interval_s,
            start_at=start_at,
            source=source,
            metadata=metadata if isinstance(metadata, dict) else {},
        )
        self.telemetry.emit(
            "trigger.created",
            {"trigger_id": record.trigger_id, "interval_s": record.interval_s, "source": record.source},
        )
        return record.to_dict()

    def list_triggers(self, *, status: str | None = None, limit: int = 200) -> Dict[str, object]:
        rows = self.trigger_manager.list(status=status, limit=limit)
        return {"items": rows, "count": len(rows)}

    def get_trigger(self, trigger_id: str) -> Optional[Dict[str, object]]:
        record = self.trigger_manager.get(trigger_id)
        return record.to_dict() if record else None

    def pause_trigger(self, trigger_id: str) -> Dict[str, object]:
        ok, message, record = self.trigger_manager.pause(trigger_id)
        payload: Dict[str, object] = {"status": "success" if ok else "error", "message": message}
        if record:
            payload["trigger"] = record.to_dict()
            self.telemetry.emit(
                "trigger.paused",
                {
                    "trigger_id": record.trigger_id,
                    "status": record.status,
                },
            )
        return payload

    def resume_trigger(self, trigger_id: str) -> Dict[str, object]:
        ok, message, record = self.trigger_manager.resume(trigger_id)
        payload: Dict[str, object] = {"status": "success" if ok else "error", "message": message}
        if record:
            payload["trigger"] = record.to_dict()
            self.telemetry.emit(
                "trigger.resumed",
                {
                    "trigger_id": record.trigger_id,
                    "status": record.status,
                },
            )
        return payload

    def run_trigger_now(self, trigger_id: str) -> Dict[str, object]:
        ok, message, record = self.trigger_manager.run_now(trigger_id)
        payload: Dict[str, object] = {"status": "success" if ok else "error", "message": message}
        if record:
            payload["trigger"] = record.to_dict()
            self.telemetry.emit(
                "trigger.run_now",
                {
                    "trigger_id": record.trigger_id,
                    "status": record.status,
                    "next_run_at": record.next_run_at,
                },
            )
        return payload

    def cancel_trigger(self, trigger_id: str) -> Dict[str, object]:
        ok, message, record = self.trigger_manager.cancel(trigger_id)
        payload: Dict[str, object] = {"status": "success" if ok else "error", "message": message}
        if record:
            payload["trigger"] = record.to_dict()
            self.telemetry.emit(
                "trigger.cancelled",
                {
                    "trigger_id": record.trigger_id,
                    "status": record.status,
                },
            )
        return payload

    def list_missions(self, *, status: str = "", limit: int = 100) -> Dict[str, Any]:
        return self.mission_control.list(status=status, limit=limit)

    def get_mission(self, mission_id: str) -> Optional[Dict[str, Any]]:
        return self.mission_control.get(mission_id)

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
        return self.mission_control.timeline(
            mission_id,
            limit=limit,
            event=event,
            step_id=step_id,
            status=status,
            descending=descending,
        )

    def mission_resume_preview(self, mission_id: str) -> Dict[str, Any]:
        return self.mission_control.resume_preview(mission_id)

    def mission_diagnostics(self, mission_id: str, *, hotspot_limit: int = 8) -> Dict[str, Any]:
        return self.mission_control.diagnostics(mission_id, hotspot_limit=hotspot_limit)

    def queue_diagnostics(
        self,
        *,
        limit: int = 200,
        include_terminal: bool = False,
        status: str = "",
        source: str = "",
        mission_id: str = "",
    ) -> Dict[str, Any]:
        snapshot_fn = getattr(self.goal_manager, "queue_snapshot", None)
        if not callable(snapshot_fn):
            return {"status": "error", "message": "queue diagnostics are unavailable"}
        payload = snapshot_fn(
            limit=limit,
            include_terminal=include_terminal,
            status=status,
            source=source,
            mission_id=mission_id,
            mission_lookup=self.mission_control.mission_for_goal,
        )
        if not isinstance(payload, dict):
            return {"status": "error", "message": "queue diagnostics payload is invalid"}
        payload["policy"] = {
            "priority_enabled": bool(getattr(self.goal_manager, "_priority_dequeue_enabled", False)),
            "starvation_window_s": float(getattr(self.goal_manager, "_priority_starvation_window_s", 0.0) or 0.0),
            "default_source_priority": int(getattr(self.goal_manager, "_default_source_priority", 0) or 0),
            "queue_deadline_enforced": bool(getattr(self.goal_manager, "_queue_deadline_enforced", False)),
            "default_max_queue_wait_s": float(getattr(self.goal_manager, "_default_max_queue_wait_s", 0.0) or 0.0),
        }
        return payload

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
        bounded_priority = self._coerce_int(priority, minimum=-20, maximum=20, default=-3)
        clean_reason = str(reason or "").strip() or "manual"
        reprioritize = getattr(self.goal_manager, "reprioritize", None)
        if not callable(reprioritize):
            return {"status": "error", "message": "goal reprioritization is unavailable"}
        promoted = bool(
            reprioritize(
                clean_goal_id,
                priority=bounded_priority,
                reason=clean_reason,
                move_front=True,
                stronger_only=False,
            )
        )
        goal = self.goal_manager.get(clean_goal_id)
        if not promoted:
            if goal is None:
                return {"status": "error", "message": "Goal not found", "goal_id": clean_goal_id}
            status_text = str(goal.status.value if hasattr(goal.status, "value") else goal.status).strip().lower()
            return {
                "status": "error",
                "message": f"Goal cannot be prioritized from status '{status_text}'.",
                "goal_id": clean_goal_id,
                "goal_status": status_text,
            }
        mission_id = self.mission_control.mission_for_goal(clean_goal_id)
        goal_status = ""
        if goal is not None:
            goal_status = str(goal.status.value if hasattr(goal.status, "value") else goal.status).strip().lower()
        self.telemetry.emit(
            "queue.goal_prioritized",
            {
                "goal_id": clean_goal_id,
                "mission_id": mission_id,
                "priority": bounded_priority,
                "reason": clean_reason,
                "goal_status": goal_status,
            },
        )
        return {
            "status": "success",
            "goal_id": clean_goal_id,
            "mission_id": mission_id,
            "priority": bounded_priority,
            "reason": clean_reason,
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
        mission = self.mission_control.get(clean_mission_id)
        if not mission:
            return {"status": "error", "message": "Mission not found", "mission_id": clean_mission_id}

        bounded_priority = self._coerce_int(priority, minimum=-20, maximum=20, default=-4)
        clean_reason = str(reason or "").strip() or "manual"
        queue_payload = self.queue_diagnostics(limit=5000, include_terminal=False)
        rows = queue_payload.get("items", []) if isinstance(queue_payload, dict) else []
        if not isinstance(rows, list):
            rows = []
        target_rows = [
            row
            for row in rows
            if isinstance(row, dict)
            and str(row.get("mission_id", "")).strip() == clean_mission_id
            and int(row.get("queue_index", -1)) >= 0
        ]
        if not target_rows:
            return {
                "status": "error",
                "message": "Mission has no queued goals to prioritize.",
                "mission_id": clean_mission_id,
            }

        reprioritize = getattr(self.goal_manager, "reprioritize", None)
        if not callable(reprioritize):
            return {"status": "error", "message": "goal reprioritization is unavailable"}

        ordered_targets = sorted(target_rows, key=lambda row: int(row.get("queue_index", 0)))
        promoted_goal_ids: List[str] = []
        for row in reversed(ordered_targets):
            goal_id = str(row.get("goal_id", "")).strip()
            if not goal_id:
                continue
            changed = bool(
                reprioritize(
                    goal_id,
                    priority=bounded_priority,
                    reason=f"{clean_reason}:mission_priority",
                    move_front=True,
                    stronger_only=False,
                )
            )
            if changed:
                promoted_goal_ids.append(goal_id)

        demoted_goal_ids: List[str] = []
        if bool(demote_others):
            demote_priority = self._coerce_int(bounded_priority + 3, minimum=-20, maximum=20, default=3)
            for row in rows:
                if not isinstance(row, dict):
                    continue
                row_goal_id = str(row.get("goal_id", "")).strip()
                row_mission_id = str(row.get("mission_id", "")).strip()
                if not row_goal_id or row_mission_id == clean_mission_id:
                    continue
                if int(row.get("queue_index", -1)) < 0:
                    continue
                changed = bool(
                    reprioritize(
                        row_goal_id,
                        priority=demote_priority,
                        reason=f"{clean_reason}:demote_for_mission:{clean_mission_id}",
                        move_front=False,
                        stronger_only=False,
                    )
                )
                if changed:
                    demoted_goal_ids.append(row_goal_id)

        promoted_goal_ids = list(dict.fromkeys(promoted_goal_ids))
        demoted_goal_ids = list(dict.fromkeys(demoted_goal_ids))
        self.telemetry.emit(
            "queue.mission_prioritized",
            {
                "mission_id": clean_mission_id,
                "priority": bounded_priority,
                "reason": clean_reason,
                "promoted_count": len(promoted_goal_ids),
                "demoted_count": len(demoted_goal_ids),
                "demote_others": bool(demote_others),
            },
        )
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

    def list_recovery_profiles(self) -> Dict[str, Any]:
        return self.recovery.list_profiles()

    def set_default_recovery_profile(self, profile_name: str) -> Dict[str, Any]:
        ok, message, selected = self.recovery.set_default_profile(profile_name)
        payload: Dict[str, Any] = {
            "status": "success" if ok else "error",
            "message": message,
            "default_profile": selected,
        }
        payload["profiles"] = self.recovery.list_profiles()
        if ok:
            self.telemetry.emit(
                "recovery.profile_updated",
                {
                    "default_profile": selected,
                },
            )
        return payload

    async def resume_mission(
        self,
        mission_id: str,
        *,
        source: str = "desktop-mission",
        metadata: Optional[Dict[str, object]] = None,
    ) -> Dict[str, Any]:
        resume_payload = self.mission_control.build_resume_payload(mission_id)
        if resume_payload.get("status") != "success":
            return resume_payload

        mission = resume_payload.get("mission", {})
        if not isinstance(mission, dict):
            return {"status": "error", "message": "mission payload is invalid"}

        requested_metadata = dict(metadata) if isinstance(metadata, dict) else {}
        requested_metadata[self._mission_metadata_key] = mission_id
        requested_metadata["resume_from_mission_id"] = mission_id
        requested_metadata["resume_plan"] = resume_payload.get("resume_plan", {})
        requested_metadata["resume_completed_step_ids"] = resume_payload.get("completed_step_ids", [])
        requested_metadata["resume_cursor"] = resume_payload.get("resume_cursor", {})
        if "policy_profile" not in requested_metadata:
            source_meta = mission.get("metadata", {})
            if isinstance(source_meta, dict):
                inherited_profile = str(source_meta.get("policy_profile", "")).strip()
                if inherited_profile:
                    requested_metadata["policy_profile"] = inherited_profile
                inherited_recovery_profile = str(source_meta.get("recovery_profile", "")).strip().lower()
                if inherited_recovery_profile and "recovery_profile" not in requested_metadata:
                    requested_metadata["recovery_profile"] = inherited_recovery_profile

        goal_text = str(mission.get("text", "")).strip()
        if not goal_text:
            return {"status": "error", "message": "mission text is missing"}

        new_goal_id = await self.submit_goal(text=goal_text, source=source, metadata=requested_metadata)
        self.mission_control.mark_resumed(mission_id, new_goal_id=new_goal_id)
        self.telemetry.emit(
            "mission.resumed",
            {
                "mission_id": mission_id,
                "goal_id": new_goal_id,
                "remaining_steps": int(resume_payload.get("remaining_steps", 0)),
            },
        )
        return {
            "status": "success",
            "mission_id": mission_id,
            "goal_id": new_goal_id,
            "remaining_steps": int(resume_payload.get("remaining_steps", 0)),
            "completed_step_ids": resume_payload.get("completed_step_ids", []),
        }

    def cancel_mission(self, mission_id: str, *, reason: str = "Cancelled by user request.") -> Dict[str, Any]:
        clean_mission_id = str(mission_id or "").strip()
        if not clean_mission_id:
            return {"status": "error", "message": "mission id is required"}

        mission = self.mission_control.get(clean_mission_id)
        if not mission:
            return {"status": "error", "message": "Mission not found"}

        mission_status = str(mission.get("status", "")).strip().lower()
        cancel_reason = str(reason or "").strip() or "Cancelled by user request."
        self._pending_auto_resume_missions.discard(clean_mission_id)

        if mission_status in {"completed", "cancelled"}:
            return {
                "status": "error",
                "message": f"Mission is already {mission_status}.",
                "mission_id": clean_mission_id,
            }

        active_goal_id = str(mission.get("active_goal_id", "")).strip() or str(mission.get("latest_goal_id", "")).strip()
        if mission_status == "running" and active_goal_id:
            cancelled = self.cancel_goal(active_goal_id, reason=cancel_reason)
            if str(cancelled.get("status", "")).strip().lower() != "success":
                return {
                    "status": "error",
                    "message": str(cancelled.get("message", "Failed to cancel running mission goal.")),
                    "mission_id": clean_mission_id,
                    "goal_id": active_goal_id,
                }

            self.mission_control.mark_finished(clean_mission_id, status="cancelled", error=cancel_reason)
            mission_row = self.mission_control.get(clean_mission_id) or mission
            self.telemetry.emit(
                "mission.cancel_requested",
                {
                    "mission_id": clean_mission_id,
                    "goal_id": active_goal_id,
                    "reason": cancel_reason,
                },
            )
            return {
                "status": "success",
                "message": "Mission cancellation requested.",
                "mission_id": clean_mission_id,
                "goal_id": active_goal_id,
                "mission": mission_row,
            }

        self.mission_control.mark_finished(clean_mission_id, status="cancelled", error=cancel_reason)
        mission_row = self.mission_control.get(clean_mission_id) or mission
        self.telemetry.emit(
            "mission.cancelled",
            {
                "mission_id": clean_mission_id,
                "goal_id": active_goal_id,
                "reason": cancel_reason,
                "previous_status": mission_status,
            },
        )
        return {
            "status": "success",
            "message": "Mission marked as cancelled.",
            "mission_id": clean_mission_id,
            "goal_id": active_goal_id,
            "mission": mission_row,
        }

    def list_rollbacks(self, *, status: str = "", goal_id: str = "", limit: int = 200) -> Dict[str, Any]:
        return self.rollback_manager.list_entries(status=status, goal_id=goal_id, limit=limit)

    def get_rollback(self, rollback_id: str) -> Optional[Dict[str, Any]]:
        return self.rollback_manager.get_entry(rollback_id)

    def run_rollback(self, rollback_id: str, *, dry_run: bool = False) -> Dict[str, Any]:
        result = self.rollback_manager.rollback_entry(rollback_id, dry_run=dry_run)
        self.telemetry.emit(
            "rollback.executed",
            {
                "rollback_id": rollback_id,
                "status": result.get("status", "error"),
                "dry_run": bool(dry_run),
            },
        )
        return result

    def run_goal_rollback(self, goal_id: str, *, dry_run: bool = False) -> Dict[str, Any]:
        result = self.rollback_manager.rollback_goal(goal_id, dry_run=dry_run)
        self.telemetry.emit(
            "rollback.goal_executed",
            {
                "goal_id": goal_id,
                "status": result.get("status", "error"),
                "dry_run": bool(dry_run),
                "rolled_back": int(result.get("rolled_back", 0) or 0),
                "failed": int(result.get("failed", 0) or 0),
            },
        )
        return result

    async def maintain_oauth_tokens(
        self,
        *,
        refresh_window_s: Optional[int] = None,
        provider: str = "",
        account_id: str = "",
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        window_s = self.oauth_refresh_window_s if refresh_window_s is None else refresh_window_s
        payload = await asyncio.to_thread(
            self.oauth_store.maintain,
            refresh_window_s=self._coerce_int(window_s, minimum=0, maximum=86400 * 7, default=self.oauth_refresh_window_s),
            provider=str(provider or "").strip().lower(),
            account_id=str(account_id or "").strip().lower(),
            dry_run=bool(dry_run),
        )
        self._record_oauth_maintenance(payload)
        self.telemetry.emit(
            "oauth.maintenance",
            {
                "status": payload.get("status", "error"),
                "dry_run": bool(dry_run),
                "candidate_count": int(payload.get("candidate_count", 0) or 0),
                "refreshed_count": int(payload.get("refreshed_count", 0) or 0),
                "error_count": int(payload.get("error_count", 0) or 0),
                "provider_filter": payload.get("provider_filter", ""),
            },
        )
        return payload

    def oauth_maintenance_status(self) -> Dict[str, Any]:
        return dict(self._last_oauth_maintenance)

    def external_reliability_status(self, *, provider: str = "", limit: int = 200) -> Dict[str, Any]:
        return self.external_reliability.snapshot(provider=provider, limit=limit)

    def external_reliability_mission_analysis(
        self,
        *,
        provider_limit: int = 260,
        history_limit: int = 40,
        record: bool = True,
    ) -> Dict[str, Any]:
        provider_cap = self._coerce_int(provider_limit, minimum=20, maximum=5000, default=260)
        history_cap = self._coerce_int(history_limit, minimum=8, maximum=400, default=40)
        record_requested = self._coerce_bool(record, default=True)
        snapshot = self.external_reliability.snapshot(limit=provider_cap)
        mission_policy_raw = snapshot.get("mission_outage_policy", {}) if isinstance(snapshot, dict) else {}
        mission_policy = mission_policy_raw if isinstance(mission_policy_raw, dict) else {}
        items_raw = snapshot.get("items", []) if isinstance(snapshot, dict) else []
        items = [dict(row) for row in items_raw if isinstance(row, dict)] if isinstance(items_raw, list) else []

        now_utc = datetime.now(timezone.utc)
        provider_health: list[Dict[str, Any]] = []
        for row in items:
            provider = str(row.get("provider", "")).strip().lower()
            if not provider:
                continue
            cooldown_until_text = str(row.get("cooldown_until", "")).strip()
            cooldown_until = self._parse_iso_utc(cooldown_until_text) if cooldown_until_text else None
            cooldown_active = bool(cooldown_until is not None and cooldown_until > now_utc)
            retry_after_s = (
                max(0.0, (cooldown_until - now_utc).total_seconds())
                if cooldown_until is not None and cooldown_active
                else 0.0
            )
            provider_health.append(
                {
                    "provider": provider,
                    "cooldown_active": cooldown_active,
                    "retry_after_s": round(retry_after_s, 3),
                    "health_score": self._coerce_float(row.get("health_score", 0.5), minimum=0.0, maximum=1.0, default=0.5),
                    "failure_ema": self._coerce_float(row.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                    "failure_trend_ema": self._coerce_float(
                        row.get("failure_trend_ema", 0.0),
                        minimum=-1.0,
                        maximum=1.0,
                        default=0.0,
                    ),
                    "outage_active": bool(row.get("outage_active", False)),
                    "outage_ema": self._coerce_float(row.get("outage_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                    "mission_profile_alignment": self._coerce_float(
                        row.get("mission_profile_alignment", 0.0),
                        minimum=-1.0,
                        maximum=1.0,
                        default=0.0,
                    ),
                    "samples": self._coerce_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                }
            )

        trend = self._external_reliability_trend_summary(
            provider_health=provider_health,
            mission_policy=mission_policy,
        )

        profile_history_raw = mission_policy.get("profile_history", []) if isinstance(mission_policy, dict) else []
        history_rows = [dict(row) for row in profile_history_raw if isinstance(row, dict)] if isinstance(profile_history_raw, list) else []
        history_rows = history_rows[-history_cap:]
        profile_transitions = 0
        mode_transitions = 0
        pressure_deltas: list[float] = []
        previous_profile = ""
        previous_mode = ""
        previous_pressure: Optional[float] = None
        for row in history_rows:
            profile_name = str(row.get("profile", "")).strip().lower()
            mode_name = str(row.get("mode", "")).strip().lower()
            pressure = self._coerce_float(row.get("target_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            if previous_profile and profile_name and profile_name != previous_profile:
                profile_transitions += 1
            if previous_mode and mode_name and mode_name != previous_mode:
                mode_transitions += 1
            if previous_pressure is not None:
                pressure_deltas.append(abs(pressure - previous_pressure))
            previous_profile = profile_name or previous_profile
            previous_mode = mode_name or previous_mode
            previous_pressure = pressure
        avg_pressure_delta = (
            sum(pressure_deltas) / float(len(pressure_deltas))
            if pressure_deltas
            else 0.0
        )
        transition_pressure = (
            float(profile_transitions + mode_transitions) / max(1.0, float(len(history_rows)))
            if history_rows
            else 0.0
        )
        volatility_index = self._coerce_float(
            (avg_pressure_delta * 0.54) + (transition_pressure * 0.46),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        volatility_mode = "stable"
        if volatility_index >= 0.68:
            volatility_mode = "surging"
        elif volatility_index >= 0.44:
            volatility_mode = "elevated"
        elif volatility_index <= 0.16:
            volatility_mode = "calm"

        at_risk_rows: list[Dict[str, Any]] = []
        for row in provider_health:
            health_score = self._coerce_float(row.get("health_score", 0.5), minimum=0.0, maximum=1.0, default=0.5)
            failure_ema = self._coerce_float(row.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            outage_ema = self._coerce_float(row.get("outage_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            at_risk = bool(
                health_score <= 0.42
                or failure_ema >= 0.58
                or outage_ema >= 0.62
                or bool(row.get("cooldown_active", False))
                or bool(row.get("outage_active", False))
            )
            if not at_risk:
                continue
            at_risk_rows.append(
                {
                    "provider": str(row.get("provider", "")).strip().lower(),
                    "health_score": round(health_score, 6),
                    "failure_ema": round(failure_ema, 6),
                    "outage_ema": round(outage_ema, 6),
                    "cooldown_active": bool(row.get("cooldown_active", False)),
                    "outage_active": bool(row.get("outage_active", False)),
                    "retry_after_s": round(
                        self._coerce_float(row.get("retry_after_s", 0.0), minimum=0.0, maximum=86_400.0, default=0.0),
                        3,
                    ),
                }
            )
        at_risk_rows.sort(
            key=lambda row: (
                int(row.get("outage_active", False)),
                int(row.get("cooldown_active", False)),
                -(float(row.get("failure_ema", 0.0) or 0.0)),
                float(row.get("health_score", 1.0) or 1.0),
                str(row.get("provider", "")),
            ),
            reverse=True,
        )
        provider_count = len(provider_health)
        at_risk_count = len(at_risk_rows)
        at_risk_ratio = (
            float(at_risk_count) / float(provider_count)
            if provider_count > 0
            else 0.0
        )
        trend_pressure = self._coerce_float(
            trend.get("trend_pressure", 0.0) if isinstance(trend, dict) else 0.0,
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        recommendations: list[Dict[str, Any]] = []
        if volatility_mode in {"surging", "elevated"}:
            recommendations.append(
                {
                    "priority": 1,
                    "type": "autotune",
                    "summary": "Mission outage profile is volatile; run autonomy_tune and bias toward stability profile.",
                    "tool_action": {"action": "autonomy_tune", "args": {"dry_run": False, "reason": "external_reliability_mission_analysis"}},
                }
            )
        if at_risk_ratio >= 0.4 or trend_pressure >= 0.58:
            recommendations.append(
                {
                    "priority": 2,
                    "type": "connector_readiness",
                    "summary": "Multiple providers are degraded; run connector status + token maintenance before heavy external batches.",
                    "tool_actions": [
                        {"action": "external_connector_status", "args": {}},
                        {"action": "oauth_token_maintain", "args": {"provider": "auto", "refresh_window_s": 900, "dry_run": False}},
                    ],
                }
            )
        if not recommendations:
            recommendations.append(
                {
                    "priority": 3,
                    "type": "observe",
                    "summary": "External reliability is stable; continue current profile and monitor trend deltas.",
                }
            )

        result = {
            "status": "success",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "provider_count": int(provider_count),
            "mission_policy": {
                "mode": str(mission_policy.get("mode", "stable")).strip().lower() or "stable",
                "profile": str(mission_policy.get("profile", "balanced")).strip().lower() or "balanced",
                "bias": round(self._coerce_float(mission_policy.get("bias", 0.0), minimum=-1.0, maximum=1.0, default=0.0), 6),
                "pressure_ema": round(self._coerce_float(mission_policy.get("pressure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6),
                "risk_ema": round(self._coerce_float(mission_policy.get("risk_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6),
                "quality_ema": round(self._coerce_float(mission_policy.get("quality_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6),
                "failed_ratio_ema": round(self._coerce_float(mission_policy.get("failed_ratio_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6),
                "blocked_ratio_ema": round(self._coerce_float(mission_policy.get("blocked_ratio_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0), 6),
            },
            "profile_history_analysis": {
                "history_count": int(len(history_rows)),
                "history_limit": int(history_cap),
                "profile_transitions": int(profile_transitions),
                "mode_transitions": int(mode_transitions),
                "avg_pressure_delta": round(avg_pressure_delta, 6),
                "transition_pressure": round(transition_pressure, 6),
                "volatility_index": round(volatility_index, 6),
                "volatility_mode": volatility_mode,
            },
            "provider_risk_analysis": {
                "at_risk_count": int(at_risk_count),
                "at_risk_ratio": round(at_risk_ratio, 6),
                "top_at_risk": at_risk_rows[:12],
            },
            "trend": trend if isinstance(trend, dict) else {},
            "recommendations": recommendations[:6],
        }
        history_payload: Dict[str, Any] = {"status": "skip", "recorded": False}
        provider_policy_tuning: Dict[str, Any] = {"status": "skip", "changed": False}
        history_snapshot: Dict[str, Any] = {"status": "skip"}
        if record_requested:
            record_fn = getattr(self.external_reliability, "record_mission_analysis", None)
            if callable(record_fn):
                history_payload = record_fn(
                    analysis=result,
                    reason="kernel_external_reliability_mission_analysis",
                    dry_run=False,
                )
            now_monotonic = time.monotonic()
            autotune_interval_s = float(
                self._coerce_int(
                    getattr(self, "external_reliability_provider_policy_autotune_interval_s", 120),
                    minimum=15,
                    maximum=7200,
                    default=120,
                )
            )
            autotune_enabled = bool(getattr(self, "external_reliability_provider_policy_autotune_enabled", True))
            last_autotune_monotonic = self._coerce_float(
                getattr(self, "_last_external_provider_policy_autotune_monotonic", 0.0),
                minimum=0.0,
                maximum=10_000_000_000.0,
                default=0.0,
            )
            if autotune_enabled and (
                (now_monotonic - last_autotune_monotonic) >= autotune_interval_s
            ):
                self._last_external_provider_policy_autotune_monotonic = now_monotonic
                tune_fn = getattr(self.external_reliability, "tune_provider_policy_from_mission_analysis", None)
                if callable(tune_fn):
                    provider_policy_tuning = tune_fn(
                        analysis=result,
                        dry_run=bool(getattr(self, "external_reliability_provider_policy_autotune_dry_run", False)),
                        reason="kernel_external_reliability_mission_analysis",
                    )
                if callable(tune_fn) and isinstance(provider_policy_tuning, dict) and hasattr(self, "telemetry"):
                    self.telemetry.emit(
                        "external_reliability.provider_policy_autotune",
                        {
                            "status": str(provider_policy_tuning.get("status", "")).strip().lower() or "unknown",
                            "changed": bool(provider_policy_tuning.get("changed", False)),
                            "updated_count": self._coerce_int(
                                provider_policy_tuning.get("updated_count", 0),
                                minimum=0,
                                maximum=100_000,
                                default=0,
                            ),
                            "dry_run": bool(provider_policy_tuning.get("dry_run", False)),
                            "mission_mode": str(provider_policy_tuning.get("mission_mode", "")).strip().lower(),
                            "mission_profile": str(provider_policy_tuning.get("mission_profile", "")).strip().lower(),
                        },
                    )
            history_fn = getattr(self.external_reliability, "mission_analysis_history", None)
            if callable(history_fn):
                history_snapshot = history_fn(
                    limit=self._coerce_int(
                        getattr(self, "external_reliability_mission_history_limit", 240),
                        minimum=20,
                        maximum=5000,
                        default=240,
                    ),
                    window=self._coerce_int(
                        getattr(self, "external_reliability_mission_history_window", 36),
                        minimum=4,
                        maximum=1200,
                        default=36,
                    ),
                )

        if isinstance(history_payload, dict) and history_payload:
            result["mission_history_record"] = {
                "status": str(history_payload.get("status", "")).strip().lower() or "unknown",
                "recorded": bool(history_payload.get("recorded", False)),
                "delta_score": self._coerce_float(
                    history_payload.get("delta_score", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "elapsed_s": self._coerce_float(
                    history_payload.get("elapsed_s", 0.0),
                    minimum=0.0,
                    maximum=86_400.0,
                    default=0.0,
                ),
            }
            drift = history_payload.get("drift", {})
            if isinstance(drift, dict) and drift:
                result["mission_history_drift"] = {
                    "mode": str(drift.get("mode", "")).strip().lower(),
                    "drift_score": self._coerce_float(
                        drift.get("drift_score", 0.0),
                        minimum=0.0,
                        maximum=1.0,
                        default=0.0,
                    ),
                    "switch_pressure": self._coerce_float(
                        drift.get("switch_pressure", 0.0),
                        minimum=0.0,
                        maximum=1.0,
                        default=0.0,
                    ),
                }
        if isinstance(provider_policy_tuning, dict) and provider_policy_tuning:
            result["provider_policy_tuning"] = {
                "status": str(provider_policy_tuning.get("status", "")).strip().lower() or "unknown",
                "changed": bool(provider_policy_tuning.get("changed", False)),
                "updated_count": self._coerce_int(
                    provider_policy_tuning.get("updated_count", 0),
                    minimum=0,
                    maximum=100_000,
                    default=0,
                ),
                "mission_mode": str(provider_policy_tuning.get("mission_mode", "")).strip().lower(),
                "mission_profile": str(provider_policy_tuning.get("mission_profile", "")).strip().lower(),
                "dry_run": bool(provider_policy_tuning.get("dry_run", False)),
            }
        if isinstance(history_snapshot, dict) and str(history_snapshot.get("status", "")).strip().lower() == "success":
            diagnostics = history_snapshot.get("diagnostics", {})
            diagnostics_row = diagnostics if isinstance(diagnostics, dict) else {}
            result["mission_history"] = {
                "count": self._coerce_int(history_snapshot.get("count", 0), minimum=0, maximum=100_000, default=0),
                "total": self._coerce_int(history_snapshot.get("total", 0), minimum=0, maximum=100_000, default=0),
                "diagnostics": {
                    "mode": str(diagnostics_row.get("mode", "")).strip().lower(),
                    "drift_score": self._coerce_float(
                        diagnostics_row.get("drift_score", 0.0),
                        minimum=0.0,
                        maximum=1.0,
                        default=0.0,
                    ),
                },
            }
        return result

    def reset_external_reliability(self, *, provider: str = "") -> Dict[str, Any]:
        return self.external_reliability.reset(provider=provider)

    def external_reliability_mission_history(self, *, limit: int = 240, window: int = 36) -> Dict[str, Any]:
        history_fn = getattr(self.external_reliability, "mission_analysis_history", None)
        if callable(history_fn):
            return history_fn(limit=limit, window=window)
        return {
            "status": "error",
            "message": "Mission analysis history is unavailable.",
            "count": 0,
            "total": 0,
            "limit": int(limit),
            "window": int(window),
            "items": [],
            "diagnostics": {"status": "error", "mode": "unavailable", "drift_score": 0.0},
        }

    def external_reliability_mission_policy_status(
        self,
        *,
        provider_limit: int = 16,
        history_limit: int = 24,
        history_window: int = 36,
    ) -> Dict[str, Any]:
        status_fn = getattr(self.external_reliability, "mission_policy_status", None)
        if callable(status_fn):
            payload = status_fn(
                provider_limit=provider_limit,
                history_limit=history_limit,
                history_window=history_window,
            )
        else:
            payload = {
                "status": "error",
                "message": "Mission policy status is unavailable.",
                "provider_biases": [],
                "history": {"count": 0, "total": 0, "items": [], "diagnostics": {}},
                "policy": {},
                "config": {},
            }

        analysis = self.external_reliability_mission_analysis(
            provider_limit=max(80, self._coerce_int(provider_limit, minimum=1, maximum=5000, default=16) * 4),
            history_limit=max(8, self._coerce_int(history_limit, minimum=1, maximum=400, default=24)),
            record=False,
        )
        now_monotonic = time.monotonic()
        interval_s = float(
            self._coerce_int(
                getattr(self, "external_reliability_provider_policy_autotune_interval_s", 120),
                minimum=15,
                maximum=7200,
                default=120,
            )
        )
        last_autotune_monotonic = self._coerce_float(
            getattr(self, "_last_external_provider_policy_autotune_monotonic", 0.0),
            minimum=0.0,
            maximum=10_000_000_000.0,
            default=0.0,
        )
        elapsed_s = (now_monotonic - last_autotune_monotonic) if last_autotune_monotonic > 0 else interval_s
        cooldown_remaining_s = max(0.0, interval_s - elapsed_s)
        payload["provider_policy_autotune"] = {
            "enabled": bool(getattr(self, "external_reliability_provider_policy_autotune_enabled", True)),
            "interval_s": round(interval_s, 6),
            "dry_run": bool(getattr(self, "external_reliability_provider_policy_autotune_dry_run", False)),
            "cooldown_remaining_s": round(cooldown_remaining_s, 6),
            "last_run_age_s": round(max(0.0, elapsed_s), 6),
        }
        payload["analysis"] = {
            "generated_at": str(analysis.get("generated_at", "")).strip(),
            "mission_policy": analysis.get("mission_policy", {}) if isinstance(analysis.get("mission_policy", {}), dict) else {},
            "profile_history_analysis": (
                analysis.get("profile_history_analysis", {})
                if isinstance(analysis.get("profile_history_analysis", {}), dict)
                else {}
            ),
            "provider_risk_analysis": (
                analysis.get("provider_risk_analysis", {})
                if isinstance(analysis.get("provider_risk_analysis", {}), dict)
                else {}
            ),
            "trend": analysis.get("trend", {}) if isinstance(analysis.get("trend", {}), dict) else {},
            "mission_history_drift": (
                analysis.get("mission_history_drift", {})
                if isinstance(analysis.get("mission_history_drift", {}), dict)
                else {}
            ),
            "provider_policy_tuning": (
                analysis.get("provider_policy_tuning", {})
                if isinstance(analysis.get("provider_policy_tuning", {}), dict)
                else {}
            ),
            "recommendations": [
                dict(item)
                for item in (analysis.get("recommendations", []) if isinstance(analysis.get("recommendations", []), list) else [])
                if isinstance(item, dict)
            ][:6],
        }
        return payload

    def external_reliability_mission_policy_configure(
        self,
        *,
        config: Dict[str, Any] | None = None,
        persist_now: bool = True,
        provider_limit: int = 16,
        history_limit: int = 24,
        history_window: int = 36,
    ) -> Dict[str, Any]:
        update_fn = getattr(self.external_reliability, "update_mission_policy_config", None)
        if not callable(update_fn):
            return {"status": "error", "message": "Mission policy configuration is unavailable."}
        payload = update_fn(config=config, persist_now=bool(persist_now))
        payload["status_snapshot"] = self.external_reliability_mission_policy_status(
            provider_limit=provider_limit,
            history_limit=history_limit,
            history_window=history_window,
        )
        return payload

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
        tune_fn = getattr(self.external_reliability, "tune_from_operational_signals", None)
        if not callable(tune_fn):
            return {"status": "error", "message": "Mission policy tuning is unavailable."}

        clean_reason = str(reason or "").strip() or "manual"
        report = self.autonomy_report(limit_recent_goals=400)
        mission_summary = self._summarize_mission_trends(limit=220)
        tune_payload = tune_fn(
            autonomy_report=report,
            mission_summary=mission_summary,
            dry_run=bool(dry_run),
            reason=f"kernel:{clean_reason}",
        )

        analysis = self.external_reliability_mission_analysis(
            provider_limit=provider_limit,
            history_limit=history_limit,
            record=False,
        )
        history_record: Dict[str, Any] = {"status": "skip", "recorded": False}
        record_fn = getattr(self.external_reliability, "record_mission_analysis", None)
        if bool(record_analysis) and callable(record_fn):
            history_record = record_fn(
                analysis=analysis,
                reason=f"kernel_mission_policy_tune:{clean_reason}",
                dry_run=bool(dry_run),
            )

        provider_policy_tuning: Dict[str, Any] = {"status": "skip", "changed": False}
        tune_provider_fn = getattr(self.external_reliability, "tune_provider_policy_from_mission_analysis", None)
        if bool(tune_provider_policies) and callable(tune_provider_fn):
            provider_policy_tuning = tune_provider_fn(
                analysis=analysis,
                dry_run=bool(dry_run),
                reason=f"kernel_mission_policy_tune:{clean_reason}",
            )
            self._last_external_provider_policy_autotune_monotonic = time.monotonic()
            if hasattr(self, "telemetry") and isinstance(provider_policy_tuning, dict):
                self.telemetry.emit(
                    "external_reliability.mission_policy_manual_tune",
                    {
                        "status": str(provider_policy_tuning.get("status", "")).strip().lower() or "unknown",
                        "changed": bool(provider_policy_tuning.get("changed", False)),
                        "updated_count": self._coerce_int(
                            provider_policy_tuning.get("updated_count", 0),
                            minimum=0,
                            maximum=100_000,
                            default=0,
                        ),
                        "dry_run": bool(dry_run),
                        "reason": clean_reason,
                    },
                )

        status_snapshot = self.external_reliability_mission_policy_status(
            provider_limit=min(32, max(8, self._coerce_int(provider_limit, minimum=1, maximum=5000, default=260) // 12)),
            history_limit=max(8, min(40, self._coerce_int(history_limit, minimum=1, maximum=400, default=40))),
            history_window=36,
        )
        return {
            "status": "success",
            "dry_run": bool(dry_run),
            "reason": clean_reason,
            "record_analysis": bool(record_analysis),
            "tune_provider_policies": bool(tune_provider_policies),
            "tune": tune_payload if isinstance(tune_payload, dict) else {},
            "mission_summary": mission_summary if isinstance(mission_summary, dict) else {},
            "report_summary": {
                "scores": report.get("scores", {}) if isinstance(report.get("scores", {}), dict) else {},
                "pressures": report.get("pressures", {}) if isinstance(report.get("pressures", {}), dict) else {},
                "risk": report.get("risk", {}) if isinstance(report.get("risk", {}), dict) else {},
                "quality": report.get("quality", {}) if isinstance(report.get("quality", {}), dict) else {},
                "recovery": report.get("recovery", {}) if isinstance(report.get("recovery", {}), dict) else {},
                "action_hotspots": [
                    dict(item)
                    for item in (report.get("action_hotspots", []) if isinstance(report.get("action_hotspots", []), list) else [])
                    if isinstance(item, dict)
                ][:8],
            },
            "analysis": analysis if isinstance(analysis, dict) else {},
            "history_record": history_record if isinstance(history_record, dict) else {},
            "provider_policy_tuning": provider_policy_tuning if isinstance(provider_policy_tuning, dict) else {},
            "changed": bool(
                (
                    isinstance(tune_payload, dict)
                    and bool(tune_payload.get("changed", False))
                )
                or (
                    isinstance(provider_policy_tuning, dict)
                    and bool(provider_policy_tuning.get("changed", False))
                )
            ),
            "status_snapshot": status_snapshot if isinstance(status_snapshot, dict) else {},
        }

    def reset_external_reliability_mission_policy(
        self,
        *,
        reset_history: bool = False,
        reset_provider_biases: bool = False,
    ) -> Dict[str, Any]:
        reset_fn = getattr(self.external_reliability, "reset_mission_policy", None)
        if not callable(reset_fn):
            return {"status": "error", "message": "Mission policy reset is unavailable."}
        payload = reset_fn(
            reset_history=bool(reset_history),
            reset_provider_biases=bool(reset_provider_biases),
        )
        payload["status_snapshot"] = self.external_reliability_mission_policy_status(
            provider_limit=12,
            history_limit=12,
            history_window=24,
        )
        return payload

    def desktop_anchor_memory_status(self, *, action: str = "", query: str = "", limit: int = 200) -> Dict[str, Any]:
        return self.desktop_anchor_memory.snapshot(action=action, query=query, limit=limit)

    def reset_desktop_anchor_memory(self, *, action: str = "", query: str = "") -> Dict[str, Any]:
        return self.desktop_anchor_memory.reset(action=action, query=query)

    def desktop_anchor_quarantine_status(self, *, action: str = "", query: str = "", limit: int = 200) -> Dict[str, Any]:
        return self.desktop_anchor_memory.quarantine_snapshot(action=action, query=query, limit=limit)

    def reset_desktop_anchor_quarantine(self, *, key: str = "", action: str = "", query: str = "") -> Dict[str, Any]:
        return self.desktop_anchor_memory.clear_quarantine(key=key, action=action, query=query)

    def policy_bandit_status(self, *, task_class: str = "", limit: int = 200) -> Dict[str, Any]:
        return self.policy_bandit.snapshot(task_class=task_class, limit=limit)

    def reset_policy_bandit(self, *, task_class: str = "") -> Dict[str, Any]:
        return self.policy_bandit.reset(task_class=task_class)

    def execution_strategy_status(self, *, task_class: str = "", limit: int = 200) -> Dict[str, Any]:
        return self.execution_strategy.snapshot(task_class=task_class, limit=limit)

    def reset_execution_strategy(self, *, task_class: str = "") -> Dict[str, Any]:
        return self.execution_strategy.reset(task_class=task_class)

    def pending_auto_resume_count(self) -> int:
        return len(self._pending_auto_resume_missions)

    async def run_forever(self) -> None:
        if not self._running:
            await self.start()
        while self._running:
            await asyncio.sleep(0.2)

    async def _loop(self) -> None:
        while self._running:
            await self._run_periodic_oauth_maintenance()
            await self._run_periodic_mission_recovery()
            await self._run_periodic_autonomy_tune()
            await self._run_periodic_external_reliability_analysis()
            await self._dispatch_due_triggers()
            await self._dispatch_due_schedules()

            goal = await self.goal_manager.dequeue(timeout_s=0.5)
            if goal is None:
                continue
            self.goal_manager.mark_running(goal)
            goal_metadata = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
            mission_id = str(goal_metadata.get(self._mission_metadata_key, "")).strip()
            if not mission_id:
                mission_id = self.mission_control.mission_for_goal(goal.goal_id)
                if mission_id:
                    goal_metadata[self._mission_metadata_key] = mission_id
                    goal.request.metadata = goal_metadata
            if mission_id:
                self.mission_control.bind_goal(mission_id, goal.goal_id)

            self.telemetry.emit(
                "goal.started",
                {
                    "goal_id": goal.goal_id,
                    "mission_id": mission_id,
                },
            )

            try:
                context: Dict[str, object] = {"source": goal.request.source}
                if mission_id:
                    context["mission_id"] = mission_id
                all_results: list[ActionResult] = []
                failed: ActionResult | None = None
                budget = self._resolve_goal_budget(source_name=goal.request.source, metadata=goal.request.metadata)
                runtime_budget_s = int(budget["max_runtime_s"])
                goal_started_monotonic = time.monotonic()
                deadline_monotonic = goal_started_monotonic + float(runtime_budget_s)
                step_budget = int(budget["max_steps"])
                executed_steps = 0
                success_steps = 0
                failed_steps = 0
                blocked_steps = 0
                planned_steps_hint = 0
                step_lookup: Dict[str, PlanStep] = {}
                interrupt_state: Dict[str, str] = {"reason": ""}
                resume_plan_payload = self._extract_resume_plan(goal.request.metadata)
                resume_plan_used = False
                replan_category_counts: Dict[str, int] = {}
                replan_policy = self._resolve_replan_policy(source_name=goal.request.source, metadata=goal.request.metadata)
                effective_max_replans = int(replan_policy.get("max_replans", self.max_replans))
                context["replan_policy"] = {
                    "max_replans": effective_max_replans,
                    "allow_blocked": bool(replan_policy.get("allow_blocked", False)),
                    "allow_non_retryable": bool(replan_policy.get("allow_non_retryable", False)),
                    "category_limits": dict(replan_policy.get("category_limits", {})),
                    "delay_base_s": float(replan_policy.get("delay_base_s", 0.0)),
                }

                metadata_ref = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
                metadata_ref["max_runtime_s"] = runtime_budget_s
                metadata_ref["max_steps"] = budget["max_steps"]
                metadata_ref[self._goal_metadata_key] = goal.goal_id
                if mission_id and self._mission_metadata_key not in metadata_ref:
                    metadata_ref[self._mission_metadata_key] = mission_id
                goal.request.metadata = metadata_ref
                self.goal_manager.sync(goal)
                context["goal_budget"] = {"max_runtime_s": runtime_budget_s, "max_steps": step_budget}

                def interrupt_check(goal_id: str = goal.goal_id) -> bool:
                    nonlocal executed_steps
                    if self.goal_manager.is_cancel_requested(goal_id):
                        interrupt_state["reason"] = self.goal_manager.cancel_reason(goal_id)
                        return True
                    if time.monotonic() >= deadline_monotonic:
                        interrupt_state["reason"] = f"Goal runtime budget exceeded ({runtime_budget_s}s)."
                        return True
                    if step_budget > 0 and executed_steps >= step_budget:
                        interrupt_state["reason"] = f"Goal step budget exceeded ({step_budget} steps)."
                        return True
                    return False

                def interrupt_reason_provider(goal_id: str = goal.goal_id) -> str:
                    reason = str(interrupt_state.get("reason", "")).strip()
                    if reason:
                        return reason
                    return self.goal_manager.cancel_reason(goal_id)

                def on_step_result(result: ActionResult) -> None:
                    nonlocal executed_steps, success_steps, failed_steps, blocked_steps
                    executed_steps += 1
                    step_status = str(result.status or "").strip().lower() or "unknown"
                    if step_status == "success":
                        success_steps += 1
                    elif step_status == "blocked":
                        blocked_steps += 1
                    elif step_status != "skipped":
                        failed_steps += 1

                    elapsed_s = max(0.0, time.monotonic() - goal_started_monotonic)
                    throughput_steps_per_s = (executed_steps / elapsed_s) if elapsed_s > 0 else 0.0
                    inferred_total_steps = max(planned_steps_hint, executed_steps)
                    remaining_steps = max(0, inferred_total_steps - executed_steps)
                    eta_s = (remaining_steps / throughput_steps_per_s) if throughput_steps_per_s > 0 else None

                    payload: Dict[str, object] = {
                        "goal_id": goal.goal_id,
                        "status": "running",
                        "step_status": step_status,
                        "action": result.action,
                        "attempt": max(1, int(result.attempt)),
                        "duration_ms": max(0, int(result.duration_ms)),
                        "completed_steps": executed_steps,
                        "success_steps": success_steps,
                        "failed_steps": failed_steps,
                        "blocked_steps": blocked_steps,
                        "planned_steps": planned_steps_hint,
                        "max_steps": step_budget,
                        "inferred_total_steps": inferred_total_steps,
                        "elapsed_s": round(elapsed_s, 3),
                        "throughput_steps_per_s": round(throughput_steps_per_s, 4),
                        "throughput_steps_per_min": round(throughput_steps_per_s * 60.0, 2),
                        "eta_s": round(eta_s, 3) if eta_s is not None else None,
                        "runtime_budget_s": int(runtime_budget_s),
                    }
                    if result.error:
                        payload["error"] = result.error
                    self.telemetry.emit("goal.progress", payload)
                    try:
                        self.policy.record_action_outcome(
                            action=result.action,
                            status=step_status,
                            source=goal.request.source,
                            metadata=goal.request.metadata if isinstance(goal.request.metadata, dict) else {},
                            evidence=result.evidence if isinstance(result.evidence, dict) else {},
                            error=result.error or "",
                        )
                    except Exception as exc:  # noqa: BLE001
                        self.telemetry.emit(
                            "policy.guardrail_record_error",
                            {
                                "goal_id": goal.goal_id,
                                "action": result.action,
                                "status": step_status,
                                "message": str(exc),
                            },
                        )

                    output = result.output if isinstance(result.output, dict) else {}
                    if mission_id:
                        step_id = ""
                        step_args: Dict[str, Any] = {}
                        if isinstance(result.evidence, dict):
                            step_id = str(result.evidence.get("step_id", "")).strip()
                        if step_id and step_id in step_lookup:
                            source_step = step_lookup.get(step_id)
                            if source_step and isinstance(source_step.args, dict):
                                step_args = source_step.args
                        self.mission_control.checkpoint_step_finished(
                            mission_id,
                            result,
                            goal_id=goal.goal_id,
                            plan_id=goal.plan.plan_id if goal.plan else "",
                            step_args=step_args,
                        )
                    state_row: Dict[str, Any] = {}
                    if isinstance(result.evidence, dict):
                        cached_state = result.evidence.get("desktop_state")
                        if isinstance(cached_state, dict):
                            state_row = {
                                "state_hash": str(cached_state.get("state_hash", "")),
                                "changed_paths": cached_state.get("changed_paths", []),
                            }
                    if not state_row:
                        try:
                            state_row = self.desktop_state.observe(
                                action=result.action,
                                output=output,
                                goal_id=goal.goal_id,
                                plan_id=goal.plan.plan_id if goal.plan else "",
                                step_id="",
                                source=goal.request.source,
                            )
                        except Exception:
                            state_row = {}

                    changed_paths = state_row.get("changed_paths", []) if isinstance(state_row, dict) else []
                    if isinstance(changed_paths, list) and changed_paths:
                        self.telemetry.emit(
                            "desktop_state.updated",
                            {
                                "goal_id": goal.goal_id,
                                "action": result.action,
                                "state_hash": state_row.get("state_hash", ""),
                                "changed_paths": changed_paths[:12],
                            },
                        )

                for attempt in range(effective_max_replans + 1):
                    if interrupt_check():
                        failed = ActionResult(
                            action="goal_budget",
                            status="blocked",
                            error=interrupt_reason_provider(),
                            output={
                                "status": "error",
                                "interrupted": True,
                                "reason": "budget_exhausted",
                                "message": interrupt_reason_provider(),
                            },
                        )
                        all_results.append(failed)
                        break

                    context["replan_attempt"] = attempt
                    execution_feedback = self._summarize_execution_feedback(all_results[-240:])
                    if execution_feedback:
                        context["execution_feedback"] = execution_feedback
                        context["execution_quality_score"] = execution_feedback.get("quality_score", 0.0)
                    else:
                        context.pop("execution_feedback", None)
                        context.pop("execution_quality_score", None)

                    if mission_id and attempt > 0:
                        mission_diag = self.mission_control.diagnostics(mission_id, hotspot_limit=6)
                        mission_feedback = self._compact_mission_feedback(mission_diag)
                        if mission_feedback:
                            context["mission_feedback"] = mission_feedback
                    mission_trend_feedback = self._runtime_mission_trend_feedback(force=attempt == 0)
                    if mission_trend_feedback:
                        context["mission_trend_feedback"] = mission_trend_feedback
                    telemetry_feedback = self._runtime_policy_telemetry_feedback(force=attempt == 0)
                    if telemetry_feedback:
                        context["telemetry_feedback"] = telemetry_feedback

                    metadata_ref = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
                    runtime_overrides: Dict[str, str] = {}
                    if bool(getattr(self, "runtime_policy_adaptation_enabled", True)):
                        runtime_overrides = self._derive_runtime_adaptive_overrides(
                            metadata=metadata_ref,
                            context=context,
                            attempt=attempt,
                        )
                    replan_overrides: Dict[str, str] = {}
                    if attempt > 0:
                        replan_overrides = self._derive_replan_overrides(
                            metadata=metadata_ref,
                            context=context,
                            policy=replan_policy,
                            replan_attempt=attempt,
                        )
                    combined_overrides = dict(runtime_overrides)
                    combined_overrides.update(replan_overrides)
                    if combined_overrides:
                        metadata_ref.update(combined_overrides)
                        goal.request.metadata = metadata_ref
                        self.goal_manager.sync(goal)
                        context.update({str(key): value for key, value in combined_overrides.items()})
                        event_name = "goal.replan_runtime_adjusted" if attempt > 0 else "goal.runtime_policy_adjusted"
                        self.telemetry.emit(
                            event_name,
                            {
                                "goal_id": goal.goal_id,
                                "attempt": attempt,
                                "overrides": dict(combined_overrides),
                                "runtime_overrides": dict(runtime_overrides),
                                "replan_overrides": dict(replan_overrides),
                                "mission_trend_feedback": context.get("mission_trend_feedback", {}),
                            },
                        )
                    lexical_hints = self.runtime_memory.search(goal.request.text, limit=5)
                    episodic_hints = self.episodic_memory.search(goal.request.text, limit=6)
                    episodic_strategy = self.episodic_memory.strategy(goal.request.text, limit=10)
                    context["recent_goal_hints"] = self.runtime_memory.recent_hints(limit=8)
                    context["retrieved_memory_hints"] = lexical_hints
                    context["retrieved_episodic_hints"] = episodic_hints
                    context["retrieved_hybrid_hints"] = self._merge_memory_hints(lexical_hints, episodic_hints, limit=10)
                    context["retrieved_episodic_strategy"] = episodic_strategy
                    context["desktop_state_hints"] = self.desktop_state.hints(limit=6)
                    context["desktop_anchor_hints"] = self.desktop_anchor_memory.hints(query=goal.request.text, limit=8)
                    context.update(self._planner_reliability_context())
                    repair_memory_hints = self._repair_memory_hints(
                        goal_text=goal.request.text,
                        context=context,
                        limit=self._coerce_int(
                            getattr(self, "runtime_policy_repair_memory_limit", 8),
                            minimum=1,
                            maximum=40,
                            default=8,
                        ),
                    )
                    if repair_memory_hints:
                        context["repair_memory_hints"] = repair_memory_hints
                    else:
                        context.pop("repair_memory_hints", None)
                    failure_clusters = self._external_failure_clusters(
                        goal_text=goal.request.text,
                        context=context,
                        limit=self._coerce_int(
                            getattr(self, "runtime_policy_failure_cluster_limit", 8),
                            minimum=1,
                            maximum=40,
                            default=8,
                        ),
                    )
                    if failure_clusters:
                        context["external_failure_clusters"] = failure_clusters
                    else:
                        context.pop("external_failure_clusters", None)
                    contract_guardrail_signal = self._contract_guardrail_signal(context)
                    if contract_guardrail_signal:
                        context["external_contract_guardrail"] = contract_guardrail_signal
                    else:
                        context.pop("external_contract_guardrail", None)
                    guardrail_snapshot = self.policy.guardrail_snapshot(limit=18, min_samples=max(1, self.policy.guardrails_min_samples // 2))
                    guardrail_items = guardrail_snapshot.get("items", []) if isinstance(guardrail_snapshot, dict) else []
                    if isinstance(guardrail_items, list) and guardrail_items:
                        context["action_guardrails"] = guardrail_items[:18]
                        context["action_guardrail_thresholds"] = (
                            guardrail_snapshot.get("thresholds", {}) if isinstance(guardrail_snapshot, dict) else {}
                        )
                    else:
                        context.pop("action_guardrails", None)
                        context.pop("action_guardrail_thresholds", None)
                    if attempt == 0 and resume_plan_payload and not resume_plan_used:
                        resume_plan = self._deserialize_execution_plan(goal_id=goal.goal_id, raw_plan=resume_plan_payload)
                        if resume_plan is not None and resume_plan.steps:
                            plan = resume_plan
                            resume_plan_used = True
                            context["resume_plan_used"] = True
                            self.telemetry.emit(
                                "mission.resume_plan_loaded",
                                {
                                    "goal_id": goal.goal_id,
                                    "mission_id": mission_id,
                                    "steps": len(plan.steps),
                                },
                            )
                        else:
                            plan = await self.planner.build_plan(goal, context=context)
                    else:
                        plan = await self.planner.build_plan(goal, context=context)
                    goal.plan = plan
                    if mission_id:
                        self.mission_control.set_plan(mission_id, plan)
                    runtime_hints = self._apply_plan_runtime_hints(
                        runtime_budget_s=runtime_budget_s,
                        step_budget=step_budget,
                        plan_context=plan.context if isinstance(plan.context, dict) else {},
                    )
                    if int(runtime_hints["max_runtime_s"]) < runtime_budget_s:
                        runtime_budget_s = int(runtime_hints["max_runtime_s"])
                        deadline_monotonic = goal_started_monotonic + float(runtime_budget_s)
                        metadata_ref = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
                        metadata_ref["max_runtime_s"] = runtime_budget_s
                        goal.request.metadata = metadata_ref
                    if int(runtime_hints["max_steps"]) < step_budget:
                        step_budget = int(runtime_hints["max_steps"])
                        metadata_ref = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
                        metadata_ref["max_steps"] = step_budget
                        goal.request.metadata = metadata_ref
                    hinted_strictness = str(runtime_hints.get("verification_strictness", "")).strip().lower()
                    if hinted_strictness in {"off", "standard", "strict"}:
                        metadata_ref = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
                        metadata_ref["verification_strictness"] = hinted_strictness
                        goal.request.metadata = metadata_ref
                        context["verification_strictness"] = hinted_strictness
                    hinted_deadline = str(runtime_hints.get("deadline_at", "")).strip()
                    if hinted_deadline:
                        metadata_ref = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
                        metadata_ref["deadline_at"] = hinted_deadline
                        goal.request.metadata = metadata_ref
                        context["deadline_at"] = hinted_deadline
                    guardrail_guidance = self.policy.recommend_runtime_overrides_for_actions(
                        actions=[step.action for step in plan.steps if isinstance(step, PlanStep)],
                        source_name=goal.request.source,
                        metadata=self._augment_guardrail_metadata(
                            metadata=goal.request.metadata if isinstance(goal.request.metadata, dict) else {},
                            context=context,
                        ),
                    )
                    guidance_applied = self._apply_guardrail_runtime_guidance(
                        goal=goal,
                        plan=plan,
                        context=context,
                        guidance=guardrail_guidance,
                    )
                    if guidance_applied:
                        self.telemetry.emit(
                            "goal.guardrail_runtime_adjusted",
                            {
                                "goal_id": goal.goal_id,
                                "attempt": attempt,
                                **guidance_applied,
                            },
                        )
                    context["goal_budget"] = {"max_runtime_s": runtime_budget_s, "max_steps": step_budget}
                    planned_steps_hint = max(planned_steps_hint, len(plan.steps))
                    self.goal_manager.sync(goal)
                    self.telemetry.emit(
                        "plan.built",
                        {"goal_id": goal.goal_id, "plan_id": plan.plan_id, "attempt": attempt, "steps": len(plan.steps)},
                    )
                    step_lookup = {
                        str(item.step_id).strip(): item
                        for item in plan.steps
                        if isinstance(item, PlanStep) and str(item.step_id).strip()
                    }

                    execution_metadata = dict(goal.request.metadata) if isinstance(goal.request.metadata, dict) else {}
                    execution_metadata[self._goal_metadata_key] = goal.goal_id
                    if mission_id:
                        execution_metadata[self._mission_metadata_key] = mission_id
                    mission_feedback_row = context.get("mission_feedback")
                    if isinstance(mission_feedback_row, dict) and mission_feedback_row:
                        execution_metadata["mission_feedback"] = dict(mission_feedback_row)
                    mission_trend_row = context.get("mission_trend_feedback")
                    if isinstance(mission_trend_row, dict) and mission_trend_row:
                        execution_metadata["mission_trend_feedback"] = dict(mission_trend_row)
                    repair_hint_rows = context.get("repair_memory_hints")
                    if isinstance(repair_hint_rows, list) and repair_hint_rows:
                        execution_metadata["repair_memory_hints"] = [row for row in repair_hint_rows[:8] if isinstance(row, dict)]
                    execution_metadata["runtime_goal_source"] = str(goal.request.source or "").strip().lower()
                    execution_metadata["runtime_replan_attempt"] = int(attempt)
                    external_auth_state = self._external_auth_runtime_state(
                        providers=self._external_providers_for_plan(plan),
                    )
                    if external_auth_state:
                        execution_metadata["external_auth_state"] = external_auth_state
                    if isinstance(plan.context, dict):
                        for key in ("voice_interaction_policy", "voice_delivery_policy", "voice_execution_policy"):
                            value = plan.context.get(key)
                            if isinstance(value, dict) and value:
                                execution_metadata[key] = dict(value)
                    on_step_started = None
                    if mission_id:
                        on_step_started = (
                            lambda started_step: self.mission_control.checkpoint_step_started(
                                mission_id,
                                goal_id=goal.goal_id,
                                plan_id=plan.plan_id,
                                step=started_step,
                            )
                        )
                    results = await self.executor.execute_plan(
                        plan,
                        source=goal.request.source,
                        metadata=execution_metadata,
                        interrupt_check=interrupt_check,
                        interrupt_reason=self.goal_manager.cancel_reason(goal.goal_id),
                        interrupt_reason_provider=interrupt_reason_provider,
                        on_step_started=on_step_started,
                        on_step_result=on_step_result,
                    )
                    all_results.extend(results)
                    failed = next((r for r in results if r.status in ("failed", "blocked")), None)
                    if failed is None:
                        break

                    interrupted = isinstance(failed.output, dict) and failed.output.get("interrupted") is True
                    if interrupted:
                        break

                    failure_context = self._extract_replan_failure_context(failed)
                    failure_category = str(failure_context.get("last_failure_category", "")).strip().lower() or "unknown"
                    should_replan, stop_reason = self._should_replan_after_failure(
                        failed=failed,
                        attempt=attempt,
                        failure_category=failure_category,
                        policy=replan_policy,
                    )
                    context.update(
                        {
                            "last_plan_id": plan.plan_id,
                            **failure_context,
                        }
                    )
                    if not should_replan:
                        self.telemetry.emit(
                            "goal.replan_stopped",
                            {
                                "goal_id": goal.goal_id,
                                "attempt": attempt,
                                "failed_action": failed.action,
                                "failure_category": failure_category,
                                "reason": stop_reason,
                            },
                        )
                        break

                    replan_category_counts[failure_category] = replan_category_counts.get(failure_category, 0) + 1
                    context.update(
                        {
                            "replan_category_counts": dict(replan_category_counts),
                        }
                    )
                    replan_delay_s = self._compute_replan_delay_s(
                        policy=replan_policy,
                        failure_context=failure_context,
                        next_attempt=attempt + 1,
                    )
                    if replan_delay_s > 0:
                        self.telemetry.emit(
                            "goal.replan_waiting",
                            {
                                "goal_id": goal.goal_id,
                                "attempt": attempt + 1,
                                "failed_action": failed.action,
                                "failure_category": failure_category,
                                "delay_s": round(replan_delay_s, 3),
                            },
                        )
                        await asyncio.sleep(replan_delay_s)
                    self.telemetry.emit(
                        "goal.replanned",
                        {
                            "goal_id": goal.goal_id,
                            "attempt": attempt + 1,
                            "reason": failed.error or "Execution failed",
                            "failed_action": failed.action,
                            "failure_category": failure_category,
                            "replan_category_counts": dict(replan_category_counts),
                            "execution_quality_score": context.get("execution_quality_score", 0.0),
                        },
                    )

                goal.results = all_results
                goal.completed_at = datetime.now(timezone.utc).isoformat()
                self.goal_manager.sync(goal)
                cancel_requested = self.goal_manager.is_cancel_requested(goal.goal_id)
                interrupted_result = next(
                    (
                        item
                        for item in all_results
                        if isinstance(item.output, dict) and item.output.get("interrupted") is True
                    ),
                    None,
                )
                interrupted_reason = ""
                if interrupted_result is not None:
                    interrupted_reason = interrupted_result.error or self.goal_manager.cancel_reason(goal.goal_id)
                elif cancel_requested:
                    interrupted_reason = self.goal_manager.cancel_reason(goal.goal_id)

                if interrupted_reason:
                    self.goal_manager.mark_cancelled(goal, interrupted_reason)
                    if mission_id:
                        self.mission_control.mark_finished(mission_id, status="cancelled", error=interrupted_reason)
                    self._update_schedule_checkpoint(goal)
                    self.runtime_memory.remember_goal(
                        text=goal.request.text,
                        status="cancelled",
                        results=all_results,
                        metadata=goal.request.metadata if isinstance(goal.request.metadata, dict) else {},
                    )
                    self.episodic_memory.remember_goal(
                        goal_id=goal.goal_id,
                        text=goal.request.text,
                        status="cancelled",
                        source=goal.request.source,
                        results=all_results,
                        metadata=goal.request.metadata,
                    )
                    self.telemetry.emit(
                        "memory.updated",
                        {
                            "goal_id": goal.goal_id,
                            "status": "cancelled",
                            "source": goal.request.source,
                            "memory_backends": ["runtime_lexical", "episodic_semantic"],
                        },
                    )
                    self._record_policy_bandit_outcome(
                        goal=goal,
                        mission_id=mission_id,
                        results=all_results,
                        outcome="cancelled",
                    )
                    self._record_execution_strategy_outcome(
                        goal=goal,
                        results=all_results,
                        outcome="cancelled",
                    )
                    self.telemetry.emit("goal.cancelled", {"goal_id": goal.goal_id, "reason": interrupted_reason})
                elif failed:
                    reason = failed.error or "Execution failed."
                    failure_category = self._failure_category_from_result(failed, fallback_error=reason)
                    self.goal_manager.mark_failed(goal, f"{failed.action}: {reason}")
                    if mission_id:
                        self.mission_control.mark_finished(mission_id, status=failed.status or "failed", error=reason)
                    self._update_schedule_checkpoint(goal)
                    self.runtime_memory.remember_goal(
                        text=goal.request.text,
                        status="failed",
                        results=all_results,
                        metadata=goal.request.metadata if isinstance(goal.request.metadata, dict) else {},
                    )
                    self.episodic_memory.remember_goal(
                        goal_id=goal.goal_id,
                        text=goal.request.text,
                        status="failed",
                        source=goal.request.source,
                        results=all_results,
                        metadata=goal.request.metadata,
                    )
                    self.telemetry.emit(
                        "memory.updated",
                        {
                            "goal_id": goal.goal_id,
                            "status": "failed",
                            "source": goal.request.source,
                            "memory_backends": ["runtime_lexical", "episodic_semantic"],
                        },
                    )
                    self.telemetry.emit(
                        "goal.failed",
                        {"goal_id": goal.goal_id, "reason": goal.failure_reason, "action": failed.action},
                    )
                    self._record_policy_bandit_outcome(
                        goal=goal,
                        mission_id=mission_id,
                        results=all_results,
                        outcome=str(failed.status or "failed"),
                    )
                    self._record_execution_strategy_outcome(
                        goal=goal,
                        results=all_results,
                        outcome=str(failed.status or "failed"),
                    )
                    auto_rollback = self._maybe_auto_rollback(
                        goal=goal,
                        failed=failed,
                        failure_category=failure_category,
                    )
                    if mission_id and not bool(auto_rollback.get("executed", False)):
                        self._maybe_schedule_auto_mission_resume(
                            mission_id=mission_id,
                            goal=goal,
                            failed=failed,
                            failure_category=failure_category,
                            reason=reason,
                        )
                else:
                    self.goal_manager.mark_completed(goal)
                    if mission_id:
                        self.mission_control.mark_finished(mission_id, status="completed")
                    self._update_schedule_checkpoint(goal)
                    self.runtime_memory.remember_goal(
                        text=goal.request.text,
                        status="completed",
                        results=all_results,
                        metadata=goal.request.metadata if isinstance(goal.request.metadata, dict) else {},
                    )
                    self.episodic_memory.remember_goal(
                        goal_id=goal.goal_id,
                        text=goal.request.text,
                        status="completed",
                        source=goal.request.source,
                        results=all_results,
                        metadata=goal.request.metadata,
                    )
                    learned_macro = self.macro_manager.learn_from_goal(
                        text=goal.request.text,
                        source=goal.request.source,
                        status="completed",
                        results=all_results,
                    )
                    self.telemetry.emit(
                        "memory.updated",
                        {
                            "goal_id": goal.goal_id,
                            "status": "completed",
                            "source": goal.request.source,
                            "memory_backends": ["runtime_lexical", "episodic_semantic"],
                        },
                    )
                    if learned_macro is not None:
                        self.telemetry.emit(
                            "macro.learned",
                            {
                                "goal_id": goal.goal_id,
                                "macro_id": learned_macro.macro_id,
                                "success_count": learned_macro.success_count,
                            },
                        )
                    self._record_policy_bandit_outcome(
                        goal=goal,
                        mission_id=mission_id,
                        results=all_results,
                        outcome="completed",
                    )
                    self._record_execution_strategy_outcome(
                        goal=goal,
                        results=all_results,
                        outcome="completed",
                    )
                    self.telemetry.emit("goal.completed", {"goal_id": goal.goal_id, "steps": len(all_results)})
            except Exception as exc:  # noqa: BLE001
                self.goal_manager.mark_failed(goal, str(exc))
                if mission_id:
                    self.mission_control.mark_finished(mission_id, status="failed", error=str(exc))
                self._update_schedule_checkpoint(goal)
                self.telemetry.emit("goal.failed", {"goal_id": goal.goal_id, "reason": str(exc)})
                self._record_policy_bandit_outcome(
                    goal=goal,
                    mission_id=mission_id,
                    results=all_results if "all_results" in locals() and isinstance(all_results, list) else [],
                    outcome="failed",
                    fallback_error=str(exc),
                )
                self._record_execution_strategy_outcome(
                    goal=goal,
                    results=all_results if "all_results" in locals() and isinstance(all_results, list) else [],
                    outcome="failed",
                )
                synthetic_failed = ActionResult(action="goal_runtime", status="failed", error=str(exc))
                self._maybe_auto_rollback(
                    goal=goal,
                    failed=synthetic_failed,
                    failure_category=self._failure_category_from_result(synthetic_failed, fallback_error=str(exc)),
                )
                if mission_id:
                    self._maybe_schedule_auto_mission_resume(
                        mission_id=mission_id,
                        goal=goal,
                        failed=synthetic_failed,
                        failure_category=self._failure_category_from_result(synthetic_failed, fallback_error=str(exc)),
                        reason=str(exc),
                    )

    async def _run_periodic_oauth_maintenance(self) -> None:
        if not self.oauth_maintenance_enabled:
            return
        now_monotonic = time.monotonic()
        if (now_monotonic - self._last_oauth_maintenance_monotonic) < float(self.oauth_maintenance_interval_s):
            return
        self._last_oauth_maintenance_monotonic = now_monotonic
        try:
            payload = await self.maintain_oauth_tokens(
                refresh_window_s=self.oauth_refresh_window_s,
                dry_run=False,
            )
            if payload.get("status") != "success":
                self.log.warning(
                    "OAuth maintenance completed with errors "
                    f"(candidates={payload.get('candidate_count', 0)}, errors={payload.get('error_count', 0)})."
                )
        except Exception as exc:  # noqa: BLE001
            error_payload = {"status": "error", "message": str(exc), "candidate_count": 0, "refreshed_count": 0, "error_count": 1}
            self._record_oauth_maintenance(error_payload)
            self.telemetry.emit(
                "oauth.maintenance",
                {"status": "error", "error_count": 1, "candidate_count": 0, "refreshed_count": 0, "message": str(exc)},
            )

    async def _run_periodic_autonomy_tune(self) -> None:
        if not self.autonomy_auto_tune_enabled:
            return
        now_monotonic = time.monotonic()
        if (now_monotonic - self._last_autonomy_tune_monotonic) < float(self.autonomy_auto_tune_interval_s):
            return
        self._last_autonomy_tune_monotonic = now_monotonic
        try:
            payload = self.autonomy_tune(dry_run=False, reason="periodic")
            if str(payload.get("status", "")).strip().lower() != "success":
                self.log.warning(f"Periodic autonomy tune failed: {payload}")
        except Exception as exc:  # noqa: BLE001
            self.log.warning(f"Periodic autonomy tune error: {exc}")

    async def _run_periodic_external_reliability_analysis(self) -> None:
        if not bool(getattr(self, "external_reliability_analysis_auto_emit_enabled", False)):
            return
        now_monotonic = time.monotonic()
        interval_s = float(
            self._coerce_int(
                getattr(self, "external_reliability_analysis_auto_emit_interval_s", 180),
                minimum=20,
                maximum=7200,
                default=180,
            )
        )
        if (now_monotonic - self._last_external_reliability_analysis_monotonic) < interval_s:
            return
        self._last_external_reliability_analysis_monotonic = now_monotonic
        try:
            analysis = self.external_reliability_mission_analysis(
                provider_limit=self._coerce_int(
                    getattr(self, "external_reliability_analysis_provider_limit", 260),
                    minimum=20,
                    maximum=5000,
                    default=260,
                ),
                history_limit=self._coerce_int(
                    getattr(self, "external_reliability_analysis_history_limit", 40),
                    minimum=8,
                    maximum=400,
                    default=40,
                ),
            )
            profile_analysis = (
                analysis.get("profile_history_analysis", {})
                if isinstance(analysis, dict) and isinstance(analysis.get("profile_history_analysis", {}), dict)
                else {}
            )
            provider_risk = (
                analysis.get("provider_risk_analysis", {})
                if isinstance(analysis, dict) and isinstance(analysis.get("provider_risk_analysis", {}), dict)
                else {}
            )
            trend = analysis.get("trend", {}) if isinstance(analysis.get("trend", {}), dict) else {}
            drift = analysis.get("mission_history_drift", {}) if isinstance(analysis.get("mission_history_drift", {}), dict) else {}
            provider_policy_tuning = (
                analysis.get("provider_policy_tuning", {})
                if isinstance(analysis.get("provider_policy_tuning", {}), dict)
                else {}
            )
            summary = {
                "status": str(analysis.get("status", "error")).strip().lower(),
                "generated_at": str(analysis.get("generated_at", "")).strip(),
                "volatility_mode": str(profile_analysis.get("volatility_mode", "")).strip().lower(),
                "volatility_index": self._coerce_float(
                    profile_analysis.get("volatility_index", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "at_risk_count": self._coerce_int(
                    provider_risk.get("at_risk_count", 0),
                    minimum=0,
                    maximum=100_000,
                    default=0,
                ),
                "provider_count": self._coerce_int(
                    analysis.get("provider_count", 0),
                    minimum=0,
                    maximum=100_000,
                    default=0,
                ),
                "trend_pressure": self._coerce_float(
                    trend.get("trend_pressure", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "trend_mode": str(trend.get("mode", "")).strip().lower(),
                "drift_mode": str(drift.get("mode", "")).strip().lower(),
                "drift_score": self._coerce_float(
                    drift.get("drift_score", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "provider_policy_changed": bool(provider_policy_tuning.get("changed", False)),
                "provider_policy_updated_count": self._coerce_int(
                    provider_policy_tuning.get("updated_count", 0),
                    minimum=0,
                    maximum=100_000,
                    default=0,
                ),
            }
            self._last_external_reliability_analysis = summary
            self.telemetry.emit(
                "external_reliability.mission_analysis",
                {
                    **summary,
                    "recommendations": analysis.get("recommendations", [])[:4]
                    if isinstance(analysis.get("recommendations", []), list)
                    else [],
                },
            )
        except Exception as exc:  # noqa: BLE001
            self.log.warning(f"Periodic external reliability analysis error: {exc}")
            self._last_external_reliability_analysis = {
                "status": "error",
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "volatility_mode": "",
                "volatility_index": 0.0,
                "at_risk_count": 0,
                "provider_count": 0,
                "drift_mode": "",
                "drift_score": 0.0,
                "provider_policy_changed": False,
                "provider_policy_updated_count": 0,
                "error": str(exc),
            }

    async def _run_periodic_mission_recovery(self) -> None:
        if not self.auto_mission_recovery_enabled:
            return
        now_monotonic = time.monotonic()
        if (now_monotonic - self._last_mission_recovery_monotonic) < float(self.auto_mission_recovery_poll_s):
            return
        self._last_mission_recovery_monotonic = now_monotonic

        candidates: list[Dict[str, Any]] = []
        for status in ("failed", "blocked"):
            payload = self.mission_control.list(status=status, limit=300)
            rows = payload.get("items", [])
            if isinstance(rows, list):
                candidates.extend(item for item in rows if isinstance(item, dict))

        if not candidates:
            return

        scheduled = 0
        skipped = 0
        for mission in candidates:
            mission_id = str(mission.get("mission_id", "")).strip()
            if not mission_id or mission_id in self._pending_auto_resume_missions:
                skipped += 1
                continue
            resume_probe = self.mission_control.build_resume_payload(mission_id)
            if resume_probe.get("status") != "success":
                skipped += 1
                continue

            latest_goal_id = str(mission.get("latest_goal_id", "")).strip()
            latest_goal = self.goal_manager.get(latest_goal_id) if latest_goal_id else None
            goal_metadata = latest_goal.request.metadata if latest_goal and isinstance(latest_goal.request.metadata, dict) else {}
            failure_status = str(mission.get("status", "")).strip().lower()
            failure_reason = str(mission.get("last_error", "")).strip()
            failure_category = self._classify_failure_category(failure_reason)
            eligible, reason, delay_s = self._evaluate_auto_mission_resume_policy(
                mission=mission,
                goal_metadata=goal_metadata,
                failure_status=failure_status,
                failure_category=failure_category,
                failure_reason=failure_reason,
            )
            if not eligible:
                skipped += 1
                continue
            if self._schedule_auto_mission_resume(
                mission_id=mission_id,
                delay_s=delay_s,
                reason=reason,
                failure_category=failure_category,
                failure_reason=failure_reason,
            ):
                scheduled += 1
            else:
                skipped += 1

        self.telemetry.emit(
            "mission.recovery_sweep",
            {"candidates": len(candidates), "scheduled": scheduled, "skipped": skipped},
        )

    def _maybe_schedule_auto_mission_resume(
        self,
        *,
        mission_id: str,
        goal: GoalRecord,
        failed: ActionResult,
        failure_category: str,
        reason: str,
    ) -> None:
        clean_mission_id = str(mission_id or "").strip()
        if not clean_mission_id:
            return
        mission = self.mission_control.get(clean_mission_id)
        if not isinstance(mission, dict):
            return
        resume_probe = self.mission_control.build_resume_payload(clean_mission_id)
        if resume_probe.get("status") != "success":
            self.telemetry.emit(
                "mission.auto_resume_skipped",
                {
                    "mission_id": clean_mission_id,
                    "goal_id": goal.goal_id,
                    "reason": str(resume_probe.get("message", "resume unavailable")),
                },
            )
            return
        goal_metadata = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
        eligible, policy_reason, delay_s = self._evaluate_auto_mission_resume_policy(
            mission=mission,
            goal_metadata=goal_metadata,
            failure_status=str(failed.status or "").strip().lower(),
            failure_category=str(failure_category or "").strip().lower(),
            failure_reason=reason,
        )
        if not eligible:
            self.telemetry.emit(
                "mission.auto_resume_skipped",
                {
                    "mission_id": clean_mission_id,
                    "goal_id": goal.goal_id,
                    "reason": policy_reason,
                    "failure_category": failure_category,
                },
            )
            return
        scheduled = self._schedule_auto_mission_resume(
            mission_id=clean_mission_id,
            delay_s=delay_s,
            reason=reason,
            failure_category=failure_category,
            failure_reason=reason,
        )
        if scheduled:
            self.telemetry.emit(
                "mission.auto_resume_scheduled",
                {
                    "mission_id": clean_mission_id,
                    "goal_id": goal.goal_id,
                    "delay_s": delay_s,
                    "failure_category": failure_category,
                },
            )

    def _schedule_auto_mission_resume(
        self,
        *,
        mission_id: str,
        delay_s: int,
        reason: str,
        failure_category: str = "",
        failure_reason: str = "",
    ) -> bool:
        clean_mission_id = str(mission_id or "").strip()
        if not clean_mission_id:
            return False
        if clean_mission_id in self._pending_auto_resume_missions:
            return False
        self._pending_auto_resume_missions.add(clean_mission_id)

        async def _runner() -> None:
            try:
                if delay_s > 0:
                    await asyncio.sleep(float(delay_s))
                if not self._running:
                    return
                mission = self.mission_control.get(clean_mission_id)
                if not isinstance(mission, dict):
                    return
                mission_status = str(mission.get("status", "")).strip().lower()
                if mission_status in {"running", "completed", "cancelled"}:
                    self.telemetry.emit(
                        "mission.auto_resume_skipped",
                        {
                            "mission_id": clean_mission_id,
                            "reason": f"mission status is {mission_status}",
                        },
                    )
                    return
                latest_goal_id = str(mission.get("latest_goal_id", "")).strip()
                latest_goal = self.goal_manager.get(latest_goal_id) if latest_goal_id else None
                metadata: Dict[str, object] = {
                    "auto_recovery": True,
                    "auto_recovery_reason": str(reason or "").strip() or "mission recovery supervisor",
                }
                mission_meta = mission.get("metadata", {})
                merged_mission_meta = mission_meta if isinstance(mission_meta, dict) else {}
                if latest_goal and isinstance(latest_goal.request.metadata, dict):
                    inherited_profile = str(latest_goal.request.metadata.get("policy_profile", "")).strip()
                    if inherited_profile:
                        metadata["policy_profile"] = inherited_profile
                current_profile = str(metadata.get("policy_profile", merged_mission_meta.get("policy_profile", ""))).strip().lower()
                target_profile, escalated = self._resolve_auto_recovery_profile(
                    current_profile=current_profile,
                    metadata=merged_mission_meta,
                    failure_category=failure_category,
                    failure_reason=failure_reason or reason,
                )
                if target_profile:
                    metadata["policy_profile"] = target_profile
                if escalated and current_profile and target_profile and target_profile != current_profile:
                    metadata["policy_profile_previous"] = current_profile
                    metadata["auto_recovery_profile_escalated_from"] = current_profile
                    metadata["auto_recovery_profile_escalated_to"] = target_profile
                    self.telemetry.emit(
                        "mission.auto_resume_profile_escalated",
                        {
                            "mission_id": clean_mission_id,
                            "from": current_profile,
                            "to": target_profile,
                            "failure_category": failure_category,
                        },
                    )
                result = await self.resume_mission(clean_mission_id, source="desktop-mission-auto", metadata=metadata)
                if result.get("status") == "success":
                    self.telemetry.emit(
                        "mission.auto_resumed",
                        {
                            "mission_id": clean_mission_id,
                            "goal_id": result.get("goal_id", ""),
                            "remaining_steps": int(result.get("remaining_steps", 0) or 0),
                        },
                    )
                else:
                    self.telemetry.emit(
                        "mission.auto_resume_failed",
                        {
                            "mission_id": clean_mission_id,
                            "message": str(result.get("message", "resume failed")),
                        },
                    )
            except Exception as exc:  # noqa: BLE001
                self.telemetry.emit(
                    "mission.auto_resume_failed",
                    {"mission_id": clean_mission_id, "message": str(exc)},
                )
            finally:
                self._pending_auto_resume_missions.discard(clean_mission_id)

        self._spawn_background_task(_runner(), name=f"mission-auto-resume-{clean_mission_id[:8]}")
        return True

    def _evaluate_auto_mission_resume_policy(
        self,
        *,
        mission: Dict[str, Any],
        goal_metadata: Dict[str, Any],
        failure_status: str,
        failure_category: str,
        failure_reason: str = "",
    ) -> tuple[bool, str, int]:
        if not self.auto_mission_recovery_enabled:
            return (False, "auto mission recovery disabled", 0)

        normalized_status = str(failure_status or "").strip().lower()
        if normalized_status not in {"failed", "blocked"}:
            return (False, f"mission status '{normalized_status or 'unknown'}' is not recoverable", 0)

        mission_metadata = mission.get("metadata", {})
        merged_metadata: Dict[str, Any] = {}
        if isinstance(mission_metadata, dict):
            merged_metadata.update(mission_metadata)
        if isinstance(goal_metadata, dict):
            merged_metadata.update(goal_metadata)

        auto_recover = self._coerce_bool(merged_metadata.get("auto_recover", True), default=True)
        if not auto_recover:
            return (False, "auto_recover metadata disabled", 0)

        max_resumes = self._coerce_int(
            merged_metadata.get("auto_recover_max_resumes", self.auto_mission_recovery_max_resumes),
            minimum=0,
            maximum=100,
            default=self.auto_mission_recovery_max_resumes,
        )
        resume_count = self._coerce_int(mission.get("resume_count", 0), minimum=0, maximum=100, default=0)
        if max_resumes <= 0:
            return (False, "auto_recover_max_resumes is 0", 0)
        if resume_count >= max_resumes:
            return (False, f"resume limit reached ({resume_count}/{max_resumes})", 0)

        allow_blocked = self.auto_mission_recovery_allow_blocked or self._coerce_bool(
            merged_metadata.get("auto_recover_allow_blocked", False),
            default=False,
        )
        if normalized_status == "blocked" and not allow_blocked:
            return (False, "blocked missions are excluded by policy", 0)

        normalized_category = str(failure_category or "").strip().lower() or "unknown"
        allow_unknown = self.auto_mission_recovery_allow_unknown or self._coerce_bool(
            merged_metadata.get("auto_recover_allow_unknown", False),
            default=False,
        )
        allow_profile_escalation = self.auto_mission_recovery_profile_escalate and self._coerce_bool(
            merged_metadata.get("auto_recover_allow_profile_escalation", True),
            default=True,
        )
        retryable_categories = {"transient", "timeout", "rate_limited"}
        if normalized_category not in retryable_categories and not (allow_unknown and normalized_category == "unknown"):
            if not (
                normalized_category == "non_retryable"
                and allow_profile_escalation
                and self._is_policy_profile_block_message(failure_reason)
            ):
                return (False, f"failure category '{normalized_category}' is not auto-recoverable", 0)

        base_delay_s = self._coerce_int(
            merged_metadata.get("auto_recover_base_delay_s", self.auto_mission_recovery_base_delay_s),
            minimum=0,
            maximum=3600,
            default=self.auto_mission_recovery_base_delay_s,
        )
        max_delay_s = self._coerce_int(
            merged_metadata.get("auto_recover_max_delay_s", self.auto_mission_recovery_max_delay_s),
            minimum=5,
            maximum=7200,
            default=self.auto_mission_recovery_max_delay_s,
        )
        forced_delay = merged_metadata.get("auto_recover_delay_s")
        if forced_delay is not None and str(forced_delay).strip():
            delay_s = self._coerce_int(forced_delay, minimum=0, maximum=max_delay_s, default=base_delay_s)
        else:
            delay_s = min(max_delay_s, base_delay_s * (2**resume_count))
        return (True, "eligible", delay_s)

    def _resolve_auto_recovery_profile(
        self,
        *,
        current_profile: str,
        metadata: Dict[str, Any],
        failure_category: str,
        failure_reason: str,
    ) -> tuple[str, bool]:
        clean_profile = str(current_profile or "").strip().lower()
        merged_metadata = metadata if isinstance(metadata, dict) else {}
        allow_escalation = self.auto_mission_recovery_profile_escalate and self._coerce_bool(
            merged_metadata.get("auto_recover_allow_profile_escalation", True),
            default=True,
        )
        if not allow_escalation:
            return (clean_profile, False)

        escalation_target = str(merged_metadata.get("auto_recover_escalation_profile", "automation_power")).strip().lower()
        if not escalation_target:
            escalation_target = "automation_power"
        if escalation_target == clean_profile:
            return (clean_profile, False)

        category = str(failure_category or "").strip().lower()
        reason = str(failure_reason or "").strip()
        if category != "non_retryable":
            return (clean_profile, False)
        if not self._is_policy_profile_block_message(reason):
            return (clean_profile, False)

        # Only escalate from automation-safe by default.
        if clean_profile not in {"automation_safe"}:
            allow_interactive = self._coerce_bool(
                merged_metadata.get("auto_recover_allow_interactive_profile_escalation", False),
                default=False,
            )
            if not (allow_interactive and clean_profile in {"interactive"}):
                return (clean_profile, False)

        return (escalation_target, True)

    @staticmethod
    def _is_policy_profile_block_message(message: str) -> bool:
        lowered = str(message or "").strip().lower()
        if not lowered:
            return False
        return any(
            token in lowered
            for token in (
                "not allowed for policy profile",
                "action denied for policy profile",
                "allow_high_risk",
                "action not allowed for source",
                "action not in allow-list",
                "policy profile",
            )
        )

    def _maybe_auto_rollback(
        self,
        *,
        goal: GoalRecord,
        failed: ActionResult,
        failure_category: str,
    ) -> Dict[str, Any]:
        metadata = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
        policy = self._normalize_rollback_policy(metadata.get("rollback_policy", self.auto_rollback_default_policy))
        should_run, reason = self._evaluate_auto_rollback_policy(
            policy=policy,
            failed_status=str(failed.status or "").strip().lower(),
            failure_category=str(failure_category or "").strip().lower(),
            failed_error=str(failed.error or ""),
        )
        if not should_run:
            self.telemetry.emit(
                "rollback.auto_skipped",
                {
                    "goal_id": goal.goal_id,
                    "policy": policy,
                    "reason": reason,
                },
            )
            return {"executed": False, "reason": reason, "policy": policy}

        ready_payload = self.rollback_manager.list_entries(status="ready", goal_id=goal.goal_id, limit=1000)
        ready_count = int(ready_payload.get("count", 0) or 0)
        if ready_count <= 0:
            self.telemetry.emit(
                "rollback.auto_skipped",
                {
                    "goal_id": goal.goal_id,
                    "policy": policy,
                    "reason": "no ready rollback entries",
                },
            )
            return {"executed": False, "reason": "no ready rollback entries", "policy": policy}

        dry_run = self._coerce_bool(metadata.get("rollback_dry_run", self.auto_rollback_dry_run), default=self.auto_rollback_dry_run)
        result = self.rollback_manager.rollback_goal(goal.goal_id, dry_run=dry_run)
        self.telemetry.emit(
            "rollback.auto_executed",
            {
                "goal_id": goal.goal_id,
                "status": result.get("status", "error"),
                "policy": policy,
                "ready_count": ready_count,
                "rolled_back": int(result.get("rolled_back", 0) or 0),
                "failed": int(result.get("failed", 0) or 0),
                "dry_run": dry_run,
            },
        )
        return {"executed": True, "reason": "executed", "policy": policy, "result": result}

    def _evaluate_auto_rollback_policy(
        self,
        *,
        policy: str,
        failed_status: str,
        failure_category: str,
        failed_error: str = "",
    ) -> tuple[bool, str]:
        if not self.auto_rollback_enabled:
            return (False, "auto rollback disabled")
        normalized_policy = self._normalize_rollback_policy(policy)
        if normalized_policy in {"off", "manual", "disabled", "never"}:
            return (False, f"rollback policy '{normalized_policy}' disables auto rollback")

        normalized_status = str(failed_status or "").strip().lower()
        if normalized_status not in {"failed", "blocked"}:
            return (False, f"status '{normalized_status or 'unknown'}' is not rollback-eligible")

        normalized_category = str(failure_category or "").strip().lower() or "unknown"
        if normalized_category in {"transient", "timeout", "rate_limited"} and normalized_policy != "always":
            return (False, f"failure category '{normalized_category}' should prefer mission recovery")

        if normalized_status == "blocked":
            allow_blocked = normalized_policy in {"always", "on_failure_or_blocked"} or self.auto_rollback_allow_blocked
            if not allow_blocked:
                return (False, "blocked status is excluded by rollback policy")
            lowered_error = str(failed_error or "").strip().lower()
            if any(marker in lowered_error for marker in ("approval required", "requires explicit user approval")) and normalized_policy != "always":
                return (False, "blocked approval flow should remain manual")

        return (True, "eligible")

    def _failure_category_from_result(self, failed: ActionResult, *, fallback_error: str = "") -> str:
        evidence = failed.evidence if isinstance(failed.evidence, dict) else {}
        recovery = evidence.get("recovery")
        if isinstance(recovery, dict):
            last_category = str(recovery.get("last_category", "")).strip().lower()
            if last_category:
                return last_category
        return self._classify_failure_category(str(failed.error or fallback_error or ""))

    def _normalize_rollback_policy(self, value: object) -> str:
        normalized = str(value or "").strip().lower()
        if normalized in {"on_failure", "always", "on_failure_or_blocked", "manual", "off", "disabled", "never"}:
            return normalized
        return self.auto_rollback_default_policy

    def _record_oauth_maintenance(self, payload: Dict[str, Any]) -> None:
        self._last_oauth_maintenance = {
            "status": str(payload.get("status", "error")),
            "last_run_at": datetime.now(timezone.utc).isoformat(),
            "candidate_count": int(payload.get("candidate_count", 0) or 0),
            "refreshed_count": int(payload.get("refreshed_count", 0) or 0),
            "error_count": int(payload.get("error_count", 0) or 0),
            "provider_filter": str(payload.get("provider_filter", "")),
            "account_filter": str(payload.get("account_filter", "")),
            "dry_run": bool(payload.get("dry_run", False)),
        }

    def _spawn_background_task(self, coro: Any, *, name: str) -> None:
        task = asyncio.create_task(coro, name=name)
        self._background_tasks.add(task)

        def _on_done(done: asyncio.Task[Any]) -> None:
            self._background_tasks.discard(done)
            if done.cancelled():
                return
            try:
                exc = done.exception()
            except Exception:  # noqa: BLE001
                return
            if exc is not None:
                self.log.error(f"Background task '{name}' failed: {exc}")

        task.add_done_callback(_on_done)

    async def _dispatch_due_triggers(self) -> None:
        due_items = self.trigger_manager.due(limit=20)
        for item in due_items:
            try:
                metadata: Dict[str, object] = dict(item.metadata)
                metadata[self._trigger_metadata_key] = item.trigger_id
                dispatch_source = item.source or "desktop-trigger"
                if dispatch_source == "desktop-ui":
                    dispatch_source = "desktop-trigger"
                goal_id = await self.submit_goal(
                    text=item.text,
                    source=dispatch_source,
                    metadata=metadata,
                )
                record = self.trigger_manager.mark_dispatched(item.trigger_id, goal_id)
                self.telemetry.emit(
                    "trigger.dispatched",
                    {
                        "trigger_id": item.trigger_id,
                        "goal_id": goal_id,
                        "run_count": record.run_count if record else item.run_count + 1,
                    },
                )
            except Exception as exc:  # noqa: BLE001
                self.trigger_manager.mark_dispatch_failed(item.trigger_id, str(exc))
                self.telemetry.emit(
                    "trigger.dispatch_failed",
                    {"trigger_id": item.trigger_id, "error": str(exc)},
                )

    async def _dispatch_due_schedules(self) -> None:
        due_items = self.schedule_manager.due(limit=20)
        for item in due_items:
            try:
                metadata: Dict[str, object] = dict(item.metadata)
                metadata[SCHEDULE_METADATA_KEY] = item.schedule_id
                dispatch_source = item.source or "desktop-schedule"
                if dispatch_source == "desktop-ui":
                    dispatch_source = "desktop-schedule"
                goal_id = await self.submit_goal(
                    text=item.text,
                    source=dispatch_source,
                    metadata=metadata,
                )
                dispatched = self.schedule_manager.mark_dispatched(item.schedule_id, goal_id)
                self.telemetry.emit(
                    "schedule.dispatched",
                    {
                        "schedule_id": item.schedule_id,
                        "goal_id": goal_id,
                        "attempt": dispatched.attempt_count if dispatched else item.attempt_count + 1,
                        "run_count": dispatched.run_count if dispatched else item.run_count + 1,
                    },
                )
            except Exception as exc:  # noqa: BLE001
                self.schedule_manager.mark_goal_result(
                    item.schedule_id,
                    goal_id=item.last_goal_id,
                    goal_status="failed",
                    failure_reason=str(exc),
                )
                self.telemetry.emit(
                    "schedule.dispatch_failed",
                    {"schedule_id": item.schedule_id, "error": str(exc)},
                )

    def _analyze_plan_readiness(self, plan: Any) -> Dict[str, Any]:
        errors: list[Dict[str, Any]] = []
        warnings: list[Dict[str, Any]] = []
        risk_counts: Dict[str, int] = {"unknown": 0, "low": 0, "medium": 0, "high": 0, "critical": 0}
        confirmation_required_steps = 0

        steps = plan.steps if hasattr(plan, "steps") and isinstance(plan.steps, list) else []
        step_ids = [str(step.step_id) for step in steps]
        step_id_set = set(step_ids)
        seen_step_ids: set[str] = set()
        planned_actions = [str(step.action) for step in steps]

        if not steps:
            errors.append(
                {
                    "code": "empty_plan",
                    "message": "Planner returned an empty step list.",
                    "step_id": "",
                    "action": "",
                    "path": "",
                }
            )

        for index, step in enumerate(steps, start=1):
            step_id = str(step.step_id)
            action = str(step.action)
            definition = self.registry.get(action)
            required_args: tuple[str, ...] = ()
            step_risk = "unknown"

            if definition is None:
                errors.append(
                    {
                        "code": "tool_missing",
                        "message": f"Step #{index} references unregistered action '{action}'.",
                        "step_id": step_id,
                        "action": action,
                        "path": "action",
                    }
                )
            else:
                step_risk = str(definition.risk or "unknown").strip().lower() or "unknown"
                required_args = tuple(definition.required_args or ())
                if definition.requires_confirmation:
                    confirmation_required_steps += 1

            risk_counts[step_risk] = int(risk_counts.get(step_risk, 0)) + 1

            step_args = step.args if isinstance(step.args, dict) else {}
            if not isinstance(step.args, dict):
                errors.append(
                    {
                        "code": "args_not_object",
                        "message": f"Step #{index} args must be a JSON object.",
                        "step_id": step_id,
                        "action": action,
                        "path": "args",
                    }
                )

            if required_args:
                missing = [name for name in required_args if name not in step_args]
                if missing:
                    errors.append(
                        {
                            "code": "missing_required_args",
                            "message": f"Step #{index} missing required args: {', '.join(missing)}.",
                            "step_id": step_id,
                            "action": action,
                            "path": "args",
                        }
                    )

            deps = step.depends_on if isinstance(step.depends_on, list) else []
            for dep in deps:
                dep_id = str(dep).strip()
                if not dep_id:
                    continue
                if dep_id not in step_id_set:
                    errors.append(
                        {
                            "code": "unknown_dependency",
                            "message": f"Step #{index} depends on unknown step_id '{dep_id}'.",
                            "step_id": step_id,
                            "action": action,
                            "path": "depends_on",
                        }
                    )
                    continue
                if dep_id not in seen_step_ids:
                    warnings.append(
                        {
                            "code": "forward_dependency",
                            "message": f"Step #{index} depends on '{dep_id}' declared later in plan order.",
                            "step_id": step_id,
                            "action": action,
                            "path": "depends_on",
                        }
                    )

            for token_item in self._collect_template_tokens(step_args, path="args"):
                token = str(token_item.get("token", "")).strip()
                token_path = str(token_item.get("path", "args"))
                is_exact = bool(token_item.get("exact", False))
                if not token:
                    continue

                if not is_exact:
                    warnings.append(
                        {
                            "code": "non_exact_template",
                            "message": (
                                f"Step #{index} has non-exact template text at {token_path}; "
                                "runtime interpolation only resolves full-token values."
                            ),
                            "step_id": step_id,
                            "action": action,
                            "path": token_path,
                        }
                    )
                    continue

                head, _, tail = token.partition(".")
                if head not in {"args", "steps", "actions", "last"}:
                    errors.append(
                        {
                            "code": "unsupported_template_namespace",
                            "message": f"Step #{index} uses unsupported template namespace '{head}'.",
                            "step_id": step_id,
                            "action": action,
                            "path": token_path,
                        }
                    )
                    continue

                if head == "steps":
                    step_ref, dot, ref_path = tail.partition(".")
                    if not step_ref or not dot or not ref_path:
                        errors.append(
                            {
                                "code": "invalid_step_template",
                                "message": f"Step #{index} has invalid step template '{token}'.",
                                "step_id": step_id,
                                "action": action,
                                "path": token_path,
                            }
                        )
                        continue
                    if step_ref not in step_id_set:
                        errors.append(
                            {
                                "code": "unknown_step_template_ref",
                                "message": f"Step #{index} references unknown step '{step_ref}' in template.",
                                "step_id": step_id,
                                "action": action,
                                "path": token_path,
                            }
                        )
                        continue
                    if step_ref not in deps:
                        warnings.append(
                            {
                                "code": "template_missing_dependency",
                                "message": (
                                    f"Step #{index} references step '{step_ref}' but does not declare it in depends_on."
                                ),
                                "step_id": step_id,
                                "action": action,
                                "path": token_path,
                            }
                        )
                elif head == "actions":
                    action_ref, dot, action_path = tail.partition(".")
                    if not action_ref:
                        errors.append(
                            {
                                "code": "invalid_action_template",
                                "message": f"Step #{index} has invalid action template '{token}'.",
                                "step_id": step_id,
                                "action": action,
                                "path": token_path,
                            }
                        )
                        continue
                    if action_ref not in planned_actions:
                        warnings.append(
                            {
                                "code": "unknown_action_template_ref",
                                "message": (
                                    f"Step #{index} references action '{action_ref}' not present in this plan; "
                                    "runtime will rely on prior execution history."
                                ),
                                "step_id": step_id,
                                "action": action,
                                "path": token_path,
                            }
                        )
                    if action_path.startswith("last.") and action_ref not in planned_actions:
                        warnings.append(
                            {
                                "code": "history_dependent_template",
                                "message": f"Step #{index} template '{token}' depends on external action history.",
                                "step_id": step_id,
                                "action": action,
                                "path": token_path,
                            }
                        )
                elif head == "last":
                    warnings.append(
                        {
                            "code": "last_snapshot_template",
                            "message": f"Step #{index} template '{token}' depends on previous step execution state.",
                            "step_id": step_id,
                            "action": action,
                            "path": token_path,
                        }
                    )

            seen_step_ids.add(step_id)

        context = plan.context if hasattr(plan, "context") and isinstance(plan.context, dict) else {}
        filtered_actions = context.get("policy_filtered_actions")
        if isinstance(filtered_actions, list) and filtered_actions:
            warnings.append(
                {
                    "code": "policy_filtered_actions",
                    "message": f"Policy profile filtered actions: {', '.join(str(item) for item in filtered_actions[:6])}.",
                    "step_id": "",
                    "action": "",
                    "path": "context.policy_filtered_actions",
                }
            )

        top_risk = "unknown"
        risk_rank = {"unknown": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}
        for risk_name, count in risk_counts.items():
            if int(count) > 0 and risk_rank.get(risk_name, 0) > risk_rank.get(top_risk, 0):
                top_risk = risk_name

        contract_diag: Dict[str, Any] = {}
        try:
            runtime_contract = getattr(plan, "runtime_contract", None)
            if callable(runtime_contract):
                payload = runtime_contract()
                if isinstance(payload, dict):
                    contract_diag = payload
        except Exception as exc:  # noqa: BLE001
            contract_diag = {"status": "error", "message": str(exc)}
        contract_risk = contract_diag.get("risk", {}) if isinstance(contract_diag, dict) else {}
        if not isinstance(contract_risk, dict):
            contract_risk = {}
        execution_layers = contract_diag.get("execution_layers", []) if isinstance(contract_diag, dict) else []
        if not isinstance(execution_layers, list):
            execution_layers = []

        return {
            "can_execute": len(errors) == 0,
            "error_count": len(errors),
            "warning_count": len(warnings),
            "errors": errors[:80],
            "warnings": warnings[:80],
            "contract": contract_diag,
            "summary": {
                "step_count": len(steps),
                "action_count": len(planned_actions),
                "confirmation_required_steps": confirmation_required_steps,
                "risk_counts": risk_counts,
                "top_risk": top_risk,
                "execution_depth": int(len(execution_layers)),
                "critical_path_timeout_s": int(contract_diag.get("critical_path_timeout_s", 0) or 0)
                if isinstance(contract_diag, dict)
                else 0,
                "estimated_total_cost_units": self._coerce_float(
                    contract_risk.get("estimated_total_cost_units", 0.0),
                    minimum=0.0,
                    maximum=10_000_000.0,
                    default=0.0,
                ),
                "high_risk_steps": self._coerce_int(
                    contract_risk.get("high_risk_count", 0),
                    minimum=0,
                    maximum=1_000_000,
                    default=0,
                ),
            },
        }

    @classmethod
    def _collect_template_tokens(cls, payload: Any, *, path: str) -> list[Dict[str, Any]]:
        rows: list[Dict[str, Any]] = []
        if isinstance(payload, dict):
            for key, value in payload.items():
                child_path = f"{path}.{key}" if path else str(key)
                rows.extend(cls._collect_template_tokens(value, path=child_path))
            return rows

        if isinstance(payload, list):
            for index, item in enumerate(payload):
                child_path = f"{path}[{index}]"
                rows.extend(cls._collect_template_tokens(item, path=child_path))
            return rows

        if not isinstance(payload, str):
            return rows

        clean = payload.strip()
        match = cls._TEMPLATE_TOKEN_EXACT_RE.fullmatch(clean)
        if match:
            rows.append({"path": path, "token": match.group(1), "exact": True})
            return rows

        if "{{" in clean and "}}" in clean:
            rows.append({"path": path, "token": clean, "exact": False})
        return rows

    @classmethod
    def _summarize_execution_feedback(cls, results: list[ActionResult]) -> Dict[str, Any]:
        rows = [item for item in results if isinstance(item, ActionResult)]
        if not rows:
            return {}

        status_counts: Dict[str, int] = {"success": 0, "failed": 0, "blocked": 0, "skipped": 0}
        failure_categories: Counter[str] = Counter()
        failed_actions: Counter[str] = Counter()
        confirm_total = 0
        confirm_failed = 0
        desktop_total = 0
        desktop_changed = 0
        verification_signals = 0
        verification_failed = 0
        remediation_attempted = 0
        remediation_success = 0
        remediation_checkpoint_runs = 0
        remediation_checkpoint_failed = 0
        remediation_contract_risk_values: list[float] = []
        duration_values: list[int] = []
        latest_failed_action = ""
        latest_failed_error = ""
        latest_failure_category = ""

        for row in rows:
            status_name = str(row.status or "").strip().lower()
            if status_name not in status_counts:
                status_name = "failed"
            status_counts[status_name] += 1

            duration_ms = max(0, int(row.duration_ms or 0))
            if duration_ms > 0:
                duration_values.append(duration_ms)

            evidence = row.evidence if isinstance(row.evidence, dict) else {}
            confirm_policy = evidence.get("confirm_policy")
            if isinstance(confirm_policy, dict):
                confirm_total += 1
                verification_signals += 1
                if not bool(confirm_policy.get("satisfied", False)):
                    confirm_failed += 1
                    verification_failed += 1

            desktop_state = evidence.get("desktop_state")
            if isinstance(desktop_state, dict):
                desktop_total += 1
                if bool(desktop_state.get("state_changed", False)):
                    desktop_changed += 1
            remediation = evidence.get("external_remediation")
            if isinstance(remediation, dict):
                rem_actions = remediation.get("actions", [])
                if isinstance(rem_actions, list):
                    for rem_row in rem_actions:
                        if not isinstance(rem_row, dict):
                            continue
                        rem_status = str(rem_row.get("status", "")).strip().lower()
                        if rem_status in {"", "skipped"}:
                            continue
                        remediation_attempted += 1
                        if rem_status == "success":
                            remediation_success += 1
                checkpoints = remediation.get("checkpoints", [])
                if isinstance(checkpoints, list):
                    for checkpoint in checkpoints:
                        if not isinstance(checkpoint, dict):
                            continue
                        checkpoint_status = str(checkpoint.get("status", "")).strip().lower()
                        if checkpoint_status in {"", "skipped"}:
                            continue
                        remediation_checkpoint_runs += 1
                        if checkpoint_status != "success":
                            remediation_checkpoint_failed += 1
                remediation_contract_risk_values.append(
                    cls._coerce_float(
                        remediation.get("contract_risk", 0.0),
                        minimum=0.0,
                        maximum=1.0,
                        default=0.0,
                    )
                )

            if status_name in {"failed", "blocked"}:
                action_name = str(row.action or "").strip()
                if action_name:
                    failed_actions[action_name] += 1
                    latest_failed_action = action_name
                latest_failed_error = str(row.error or "").strip()
                lowered_error = latest_failed_error.lower()
                if "verification" in lowered_error or "confirm policy failed" in lowered_error:
                    verification_signals += 1
                    verification_failed += 1
                category = cls._classify_failure_category(latest_failed_error) or "unknown"
                latest_failure_category = category
                failure_categories[category] += 1

        total_steps = len(rows)
        success_count = int(status_counts.get("success", 0))
        failed_count = int(status_counts.get("failed", 0))
        blocked_count = int(status_counts.get("blocked", 0))
        skipped_count = int(status_counts.get("skipped", 0))
        failure_total = failed_count + blocked_count
        success_rate = success_count / max(1, total_steps)
        failure_ratio = failure_total / max(1, total_steps)
        confirm_failure_ratio = (confirm_failed / confirm_total) if confirm_total > 0 else 0.0
        desktop_change_rate = (desktop_changed / desktop_total) if desktop_total > 0 else 0.0
        remediation_success_rate = (
            float(remediation_success) / max(1.0, float(remediation_attempted))
            if remediation_attempted > 0
            else 0.0
        )
        remediation_checkpoint_blocked_ratio = (
            float(remediation_checkpoint_failed) / max(1.0, float(remediation_checkpoint_runs))
            if remediation_checkpoint_runs > 0
            else 0.0
        )
        verification_failure_ratio = (
            float(verification_failed) / max(1.0, float(verification_signals))
            if verification_signals > 0
            else 0.0
        )
        remediation_contract_risk = (
            sum(remediation_contract_risk_values) / float(len(remediation_contract_risk_values))
            if remediation_contract_risk_values
            else 0.0
        )
        avg_duration_ms = int(round(sum(duration_values) / max(1, len(duration_values)))) if duration_values else 0
        max_duration_ms = max(duration_values) if duration_values else 0

        quality_score = 1.0 - min(
            1.0,
            (failure_ratio * 0.55) + (confirm_failure_ratio * 0.3) + ((1.0 - desktop_change_rate) * 0.15 if desktop_total > 0 else 0.0),
        )
        quality_score = max(0.0, min(1.0, quality_score))
        verification_pressure = cls._coerce_float(
            (verification_failure_ratio * 0.7)
            + (confirm_failure_ratio * 0.2)
            + (remediation_checkpoint_blocked_ratio * 0.1),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        verification_pressure_mode = "stable"
        if verification_pressure >= 0.66:
            verification_pressure_mode = "severe"
        elif verification_pressure >= 0.38:
            verification_pressure_mode = "moderate"
        if quality_score >= 0.82:
            quality_level = "high"
        elif quality_score >= 0.58:
            quality_level = "medium"
        else:
            quality_level = "low"

        top_failed_actions = [
            {"action": action, "count": count}
            for action, count in failed_actions.most_common(5)
            if action
        ]
        top_failure_categories = [
            {"category": name, "count": count}
            for name, count in failure_categories.most_common(5)
            if name
        ]

        return {
            "window_size": total_steps,
            "status_counts": status_counts,
            "success_rate": round(success_rate, 4),
            "failure_ratio": round(failure_ratio, 4),
            "confirm_checks_total": int(confirm_total),
            "confirm_checks_failed": int(confirm_failed),
            "confirm_failure_ratio": round(confirm_failure_ratio, 4),
            "verification_signals": int(verification_signals),
            "verification_failed": int(verification_failed),
            "verification_failure_ratio": round(verification_failure_ratio, 4),
            "verification_pressure": round(verification_pressure, 4),
            "verification_pressure_mode": verification_pressure_mode,
            "desktop_checks_total": int(desktop_total),
            "desktop_changed_count": int(desktop_changed),
            "desktop_change_rate": round(desktop_change_rate, 4),
            "remediation_attempted": int(remediation_attempted),
            "remediation_success_rate": round(remediation_success_rate, 4),
            "remediation_checkpoint_runs": int(remediation_checkpoint_runs),
            "remediation_checkpoint_blocked_ratio": round(remediation_checkpoint_blocked_ratio, 4),
            "remediation_contract_risk": round(remediation_contract_risk, 4),
            "avg_duration_ms": int(avg_duration_ms),
            "max_duration_ms": int(max_duration_ms),
            "top_failed_actions": top_failed_actions,
            "top_failure_categories": top_failure_categories,
            "latest_failed_action": latest_failed_action,
            "latest_failed_error": latest_failed_error,
            "latest_failure_category": latest_failure_category,
            "quality_score": round(quality_score, 4),
            "quality_level": quality_level,
            "successful_steps": success_count,
            "failed_steps": failed_count,
            "blocked_steps": blocked_count,
            "skipped_steps": skipped_count,
        }

    @classmethod
    def _compact_mission_feedback(cls, diagnostics: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(diagnostics, dict):
            return {}
        if str(diagnostics.get("status", "")).strip().lower() != "success":
            return {}

        risk = diagnostics.get("risk", {})
        quality = diagnostics.get("quality", {})
        resume = diagnostics.get("resume", {})
        hotspots = diagnostics.get("hotspots", {})
        retry_hotspots = hotspots.get("retry", []) if isinstance(hotspots, dict) else []
        failure_hotspots = hotspots.get("failures", []) if isinstance(hotspots, dict) else []

        top_retry = retry_hotspots[0] if isinstance(retry_hotspots, list) and retry_hotspots else {}
        top_failure = failure_hotspots[0] if isinstance(failure_hotspots, list) and failure_hotspots else {}

        return {
            "mission_status": str(diagnostics.get("mission_status", "")).strip().lower(),
            "risk_level": str(risk.get("level", "")).strip().lower(),
            "risk_score": cls._coerce_float(risk.get("score", 0.0), minimum=0.0, maximum=1.0, default=0.0),
            "quality_level": str(quality.get("level", "")).strip().lower(),
            "quality_score": cls._coerce_float(quality.get("score", 0.0), minimum=0.0, maximum=1.0, default=0.0),
            "recommended_recovery_profile": str(quality.get("recommended_recovery_profile", "")).strip().lower(),
            "recommended_verification_strictness": str(quality.get("recommended_verification_strictness", "")).strip().lower(),
            "resume_ready": bool(resume.get("ready", False)),
            "remaining_steps": cls._coerce_int(resume.get("remaining_steps", 0), minimum=0, maximum=100000, default=0),
            "top_retry_hotspot": {
                "step_id": str(top_retry.get("step_id", "")),
                "attempts": cls._coerce_int(top_retry.get("attempts", 0), minimum=0, maximum=100000, default=0),
                "action": str(top_retry.get("action", "")),
            },
            "top_failure_hotspot": {
                "step_id": str(top_failure.get("step_id", "")),
                "status": str(top_failure.get("status", "")),
                "action": str(top_failure.get("action", "")),
            },
        }

    def _planner_reliability_context(self) -> Dict[str, object]:
        now_utc = datetime.now(timezone.utc)
        breaker_snapshot = self.action_circuit_breaker.snapshot(limit=500)
        breaker_items = breaker_snapshot.get("items", []) if isinstance(breaker_snapshot, dict) else []
        open_circuits: list[Dict[str, Any]] = []
        if isinstance(breaker_items, list):
            for item in breaker_items:
                if not isinstance(item, dict):
                    continue
                open_until_text = str(item.get("open_until", "")).strip()
                if not open_until_text:
                    continue
                open_until = self._parse_iso_utc(open_until_text)
                if open_until is None:
                    continue
                retry_after_s = max(0.0, (open_until - now_utc).total_seconds())
                if retry_after_s <= 0.0:
                    continue
                open_circuits.append(
                    {
                        "action": str(item.get("action", "")).strip(),
                        "scope": str(item.get("scope", "")).strip().lower(),
                        "retry_after_s": round(retry_after_s, 3),
                        "last_failure_category": str(item.get("last_failure_category", "")).strip().lower(),
                        "opened_count": self._coerce_int(item.get("opened_count", 0), minimum=0, maximum=100_000, default=0),
                    }
                )

        external_snapshot = self.external_reliability.snapshot(limit=260)
        mission_policy_raw = external_snapshot.get("mission_outage_policy", {}) if isinstance(external_snapshot, dict) else {}
        mission_policy = mission_policy_raw if isinstance(mission_policy_raw, dict) else {}
        external_items = external_snapshot.get("items", []) if isinstance(external_snapshot, dict) else []
        provider_health: list[Dict[str, Any]] = []
        if isinstance(external_items, list):
            for item in external_items:
                if not isinstance(item, dict):
                    continue
                provider = str(item.get("provider", "")).strip().lower()
                if not provider:
                    continue
                cooldown_until_text = str(item.get("cooldown_until", "")).strip()
                cooldown_until = self._parse_iso_utc(cooldown_until_text) if cooldown_until_text else None
                cooldown_active = bool(cooldown_until is not None and cooldown_until > now_utc)
                retry_after_s = (
                    round(max(0.0, (cooldown_until - now_utc).total_seconds()), 3)
                    if cooldown_until is not None
                    else 0.0
                )
                provider_health.append(
                    {
                        "provider": provider,
                        "cooldown_active": cooldown_active,
                        "retry_after_s": retry_after_s,
                        "health_score": self._coerce_float(item.get("health_score", 0.5), minimum=0.0, maximum=1.0, default=0.5),
                        "failure_ema": self._coerce_float(item.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        "failure_trend_ema": self._coerce_float(
                            item.get("failure_trend_ema", 0.0),
                            minimum=-1.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        "outage_active": bool(item.get("outage_active", False)),
                        "outage_ema": self._coerce_float(item.get("outage_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                        "outage_mission_pressure": self._coerce_float(
                            item.get("outage_mission_pressure", 0.0),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        "mission_profile_alignment": self._coerce_float(
                            item.get("mission_profile_alignment", 0.0),
                            minimum=-1.0,
                            maximum=1.0,
                            default=0.0,
                        ),
                        "consecutive_failures": self._coerce_int(
                            item.get("consecutive_failures", 0),
                            minimum=0,
                            maximum=100_000,
                            default=0,
                        ),
                        "samples": self._coerce_int(item.get("samples", 0), minimum=0, maximum=10_000_000, default=0),
                        "top_action_risks": item.get("top_action_risks", []) if isinstance(item.get("top_action_risks", []), list) else [],
                        "top_operation_risks": item.get("top_operation_risks", []) if isinstance(item.get("top_operation_risks", []), list) else [],
                    }
                )

        provider_health.sort(
            key=lambda row: (
                float(row.get("health_score", 0.0) or 0.0),
                -float(row.get("failure_ema", 0.0)),
                -int(row.get("consecutive_failures", 0)),
                str(row.get("provider", "")),
            )
        )
        open_circuits.sort(
            key=lambda row: (
                -float(row.get("retry_after_s", 0.0)),
                str(row.get("action", "")),
                str(row.get("scope", "")),
            )
        )
        payload: Dict[str, object] = {
            "open_action_circuits": open_circuits[:120],
            "external_provider_health": provider_health[:120],
        }
        reliability_trend = self._external_reliability_trend_summary(
            provider_health=provider_health,
            mission_policy=mission_policy,
        )
        if reliability_trend:
            payload["external_reliability_trend"] = reliability_trend
        mission_analysis = self.external_reliability_mission_analysis(provider_limit=120, history_limit=24, record=False)
        if isinstance(mission_analysis, dict) and mission_analysis:
            profile_analysis = mission_analysis.get("profile_history_analysis", {})
            provider_analysis = mission_analysis.get("provider_risk_analysis", {})
            drift_analysis = mission_analysis.get("mission_history_drift", {})
            provider_policy_tuning = mission_analysis.get("provider_policy_tuning", {})
            payload["external_reliability_mission_analysis"] = {
                "status": str(mission_analysis.get("status", "")).strip().lower(),
                "provider_count": self._coerce_int(
                    mission_analysis.get("provider_count", 0),
                    minimum=0,
                    maximum=100_000,
                    default=0,
                ),
                "volatility_mode": str(profile_analysis.get("volatility_mode", "")).strip().lower()
                if isinstance(profile_analysis, dict)
                else "",
                "volatility_index": self._coerce_float(
                    profile_analysis.get("volatility_index", 0.0)
                    if isinstance(profile_analysis, dict)
                    else 0.0,
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "at_risk_count": self._coerce_int(
                    provider_analysis.get("at_risk_count", 0)
                    if isinstance(provider_analysis, dict)
                    else 0,
                    minimum=0,
                    maximum=100_000,
                    default=0,
                ),
                "at_risk_ratio": self._coerce_float(
                    provider_analysis.get("at_risk_ratio", 0.0)
                    if isinstance(provider_analysis, dict)
                    else 0.0,
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "drift_mode": str(drift_analysis.get("mode", "")).strip().lower()
                if isinstance(drift_analysis, dict)
                else "",
                "drift_score": self._coerce_float(
                    drift_analysis.get("drift_score", 0.0)
                    if isinstance(drift_analysis, dict)
                    else 0.0,
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "provider_policy_changed": bool(
                    provider_policy_tuning.get("changed", False)
                    if isinstance(provider_policy_tuning, dict)
                    else False
                ),
                "provider_policy_updated_count": self._coerce_int(
                    provider_policy_tuning.get("updated_count", 0)
                    if isinstance(provider_policy_tuning, dict)
                    else 0,
                    minimum=0,
                    maximum=10_000,
                    default=0,
                ),
            }
        mission_trend_feedback = self._runtime_mission_trend_feedback(force=False)
        if mission_trend_feedback:
            payload["mission_trend_feedback"] = mission_trend_feedback
        return payload

    def _external_reliability_trend_summary(
        self,
        *,
        provider_health: list[Dict[str, Any]],
        mission_policy: Dict[str, Any],
    ) -> Dict[str, Any]:
        rows = [row for row in provider_health if isinstance(row, dict)]
        if not rows and not isinstance(mission_policy, dict):
            return {}

        provider_count = len(rows)
        cooldown_active_count = sum(1 for row in rows if bool(row.get("cooldown_active", False)))
        outage_active_count = sum(1 for row in rows if bool(row.get("outage_active", False)))
        avg_health = 0.0
        avg_failure = 0.0
        avg_trend = 0.0
        avg_alignment = 0.0
        weighted_failure = 0.0
        total_samples = 0
        if provider_count > 0:
            avg_health = sum(
                self._coerce_float(row.get("health_score", 0.0), minimum=0.0, maximum=1.0, default=0.0)
                for row in rows
            ) / float(provider_count)
            avg_failure = sum(
                self._coerce_float(row.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
                for row in rows
            ) / float(provider_count)
            avg_trend = sum(
                self._coerce_float(row.get("failure_trend_ema", 0.0), minimum=-1.0, maximum=1.0, default=0.0)
                for row in rows
            ) / float(provider_count)
            avg_alignment = sum(
                self._coerce_float(row.get("mission_profile_alignment", 0.0), minimum=-1.0, maximum=1.0, default=0.0)
                for row in rows
            ) / float(provider_count)
            total_samples = sum(
                self._coerce_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0)
                for row in rows
            )
            if total_samples > 0:
                weighted_failure = sum(
                    self._coerce_float(row.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
                    * float(self._coerce_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0))
                    for row in rows
                ) / float(total_samples)

        risk_rows: list[Dict[str, Any]] = []
        for row in rows:
            provider = str(row.get("provider", "")).strip().lower()
            if not provider:
                continue
            failure_ema = self._coerce_float(row.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            trend_ema = self._coerce_float(row.get("failure_trend_ema", 0.0), minimum=-1.0, maximum=1.0, default=0.0)
            health_score = self._coerce_float(row.get("health_score", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            outage_ema = self._coerce_float(
                row.get("outage_ema", row.get("outage_mission_pressure", 0.0)),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            alignment = self._coerce_float(row.get("mission_profile_alignment", 0.0), minimum=-1.0, maximum=1.0, default=0.0)
            risk_score = (
                (failure_ema * 0.46)
                + (max(0.0, trend_ema) * 0.16)
                + ((1.0 - health_score) * 0.24)
                + (outage_ema * 0.14)
            )
            if bool(row.get("cooldown_active", False)):
                risk_score += 0.1
            if bool(row.get("outage_active", False)):
                risk_score += 0.14
            if alignment < 0.0:
                risk_score += min(0.08, abs(alignment) * 0.08)
            risk_rows.append(
                {
                    "provider": provider,
                    "risk_score": round(max(0.0, min(1.0, risk_score)), 6),
                    "health_score": round(health_score, 6),
                    "failure_ema": round(failure_ema, 6),
                    "failure_trend_ema": round(trend_ema, 6),
                    "cooldown_active": bool(row.get("cooldown_active", False)),
                    "outage_active": bool(row.get("outage_active", False)),
                    "retry_after_s": round(float(row.get("retry_after_s", 0.0) or 0.0), 3),
                }
            )
        risk_rows.sort(
            key=lambda row: (
                -self._coerce_float(row.get("risk_score", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                str(row.get("provider", "")),
            )
        )

        policy = mission_policy if isinstance(mission_policy, dict) else {}
        mode = str(policy.get("mode", "stable")).strip().lower() or "stable"
        mission_profile = str(policy.get("profile", "balanced")).strip().lower() or "balanced"
        policy_pressure = self._coerce_float(policy.get("pressure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        policy_bias = self._coerce_float(policy.get("bias", 0.0), minimum=-1.0, maximum=1.0, default=0.0)
        failed_ratio_ema = self._coerce_float(policy.get("failed_ratio_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        blocked_ratio_ema = self._coerce_float(policy.get("blocked_ratio_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        provider_risk_avg = (
            sum(self._coerce_float(row.get("risk_score", 0.0), minimum=0.0, maximum=1.0, default=0.0) for row in risk_rows)
            / float(len(risk_rows))
            if risk_rows
            else 0.0
        )
        trend_pressure = min(
            1.0,
            max(
                0.0,
                (policy_pressure * 0.36)
                + (provider_risk_avg * 0.34)
                + (failed_ratio_ema * 0.18)
                + (blocked_ratio_ema * 0.12),
            ),
        )
        if mode == "worsening":
            trend_pressure = min(1.0, trend_pressure + 0.06)
        elif mode == "improving":
            trend_pressure = max(0.0, trend_pressure - 0.06)

        return {
            "status": "success",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "provider_count": int(provider_count),
            "cooldown_active_count": int(cooldown_active_count),
            "outage_active_count": int(outage_active_count),
            "cooldown_active_ratio": round(
                float(cooldown_active_count) / float(provider_count)
                if provider_count > 0
                else 0.0,
                6,
            ),
            "outage_active_ratio": round(
                float(outage_active_count) / float(provider_count)
                if provider_count > 0
                else 0.0,
                6,
            ),
            "avg_health_score": round(max(0.0, min(1.0, avg_health)), 6),
            "avg_failure_ema": round(max(0.0, min(1.0, avg_failure)), 6),
            "weighted_failure_ema": round(max(0.0, min(1.0, weighted_failure)), 6),
            "avg_failure_trend_ema": round(max(-1.0, min(1.0, avg_trend)), 6),
            "avg_mission_profile_alignment": round(max(-1.0, min(1.0, avg_alignment)), 6),
            "total_samples": int(total_samples),
            "trend_pressure": round(trend_pressure, 6),
            "mode": mode,
            "mission_profile": mission_profile,
            "mission_bias": round(policy_bias, 6),
            "failed_ratio_ema": round(failed_ratio_ema, 6),
            "blocked_ratio_ema": round(blocked_ratio_ema, 6),
            "top_provider_risks": risk_rows[:6],
        }

    @staticmethod
    def _contract_pressure_from_code(code: str) -> float:
        clean = str(code or "").strip().lower()
        pressure_by_code = {
            "auth_preflight_failed": 0.72,
            "no_provider_candidates_after_contract": 0.68,
            "provider_not_supported_for_action": 0.58,
            "provider_cooldown_blocked": 0.64,
            "provider_outage_blocked": 0.76,
            "provider_runtime_blocked": 0.62,
            "missing_required_fields": 0.46,
            "missing_any_of_fields": 0.44,
            "invalid_field_type_or_range": 0.48,
            "missing_event_window": 0.46,
            "invalid_event_window": 0.5,
            "invalid_due_timestamp": 0.5,
            "contract_validation_failed": 0.42,
            "provider_contract_failed": 0.52,
        }
        return max(0.0, min(1.0, pressure_by_code.get(clean, 0.0)))

    def _contract_guardrail_signal(self, context: Dict[str, object]) -> Dict[str, Any]:
        if not bool(getattr(self, "runtime_policy_contract_guardrail_enabled", True)):
            return {}

        failed_action = str(context.get("last_failure_action", "")).strip().lower()
        if not failed_action or not failed_action.startswith("external_"):
            return {}

        contract_raw = context.get("last_failure_external_contract", {})
        contract = contract_raw if isinstance(contract_raw, dict) else {}
        external_raw = context.get("last_failure_external_reliability", {})
        external = external_raw if isinstance(external_raw, dict) else {}

        code = str(contract.get("code", "")).strip().lower()
        severity = str(contract.get("severity", "")).strip().lower()
        severity_score = self._coerce_float(
            contract.get("severity_score", external.get("severity_score", 0.0)),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        blocking_class = str(
            contract.get("blocking_class", external.get("blocking_class", ""))
        ).strip().lower()
        estimated_recovery_s = self._coerce_int(
            contract.get("estimated_recovery_s", external.get("estimated_recovery_s", 0)),
            minimum=0,
            maximum=86_400,
            default=0,
        )
        automation_tier = str(
            contract.get("automation_tier", external.get("automation_tier", ""))
        ).strip().lower()
        execution_contract_raw = contract.get("execution_contract", {})
        execution_contract = execution_contract_raw if isinstance(execution_contract_raw, dict) else {}
        execution_mode = str(
            execution_contract.get("mode", external.get("execution_mode", automation_tier))
        ).strip().lower()
        execution_max_retry_attempts = self._coerce_int(
            execution_contract.get("max_retry_attempts", external.get("execution_max_retry_attempts", 0)),
            minimum=0,
            maximum=20,
            default=0,
        )
        execution_allow_provider_reroute = self._coerce_bool(
            execution_contract.get("allow_provider_reroute", external.get("allow_provider_reroute", True)),
            default=True,
        )
        execution_stop_conditions_raw = execution_contract.get(
            "stop_conditions",
            external.get("execution_stop_conditions", []),
        )
        execution_stop_conditions = (
            [str(item).strip().lower() for item in execution_stop_conditions_raw if str(item).strip()]
            if isinstance(execution_stop_conditions_raw, list)
            else []
        )
        preflight_status = str(contract.get("preflight_status", external.get("preflight_status", ""))).strip().lower()
        pressure = self._contract_pressure_from_code(code)
        if preflight_status in {"blocked", "error"}:
            pressure = max(pressure, 0.34)
        if severity == "error":
            pressure = max(pressure, 0.42)
        if severity == "critical":
            pressure = max(pressure, 0.68)
        if severity_score > 0.0:
            pressure = max(pressure, min(1.0, 0.18 + (severity_score * 0.84)))
        if blocking_class == "auth":
            pressure = max(pressure, 0.62)
        elif blocking_class == "provider":
            pressure = max(pressure, 0.54)
        elif blocking_class == "reliability":
            pressure = max(pressure, 0.48)
        if estimated_recovery_s >= 1800:
            pressure = min(1.0, pressure + 0.12)
        elif estimated_recovery_s >= 900:
            pressure = min(1.0, pressure + 0.08)
        elif estimated_recovery_s >= 300:
            pressure = min(1.0, pressure + 0.04)
        if automation_tier == "manual":
            pressure = min(1.0, pressure + 0.08)
        elif automation_tier == "assisted":
            pressure = min(1.0, pressure + 0.04)
        if execution_mode == "manual":
            pressure = min(1.0, pressure + 0.09)
        elif execution_mode == "assisted":
            pressure = min(1.0, pressure + 0.04)
        if execution_max_retry_attempts > 0 and execution_max_retry_attempts <= 1:
            pressure = min(1.0, pressure + 0.04)
        if not execution_allow_provider_reroute:
            pressure = min(1.0, pressure + 0.05)
        if execution_stop_conditions:
            pressure = min(
                1.0,
                pressure + min(0.12, float(len(execution_stop_conditions)) * 0.03),
            )
        remediation_hints = contract.get("remediation_hints", [])
        hint_count = len(remediation_hints) if isinstance(remediation_hints, list) else 0
        if hint_count > 0 and code:
            pressure = min(1.0, pressure + min(0.12, float(hint_count) * 0.03))
        if pressure <= 0.0:
            return {}
        attempt = self._coerce_int(
            context.get("last_failure_attempt", 1),
            minimum=1,
            maximum=10_000,
            default=1,
        )
        return {
            "action": failed_action,
            "code": code,
            "pressure": round(max(0.0, min(1.0, pressure)), 6),
            "severity": severity or "error",
            "severity_score": round(severity_score, 6),
            "blocking_class": blocking_class,
            "estimated_recovery_s": int(estimated_recovery_s),
            "automation_tier": automation_tier,
            "execution_mode": execution_mode,
            "execution_max_retry_attempts": int(execution_max_retry_attempts),
            "execution_allow_provider_reroute": bool(execution_allow_provider_reroute),
            "execution_stop_conditions": execution_stop_conditions[:12],
            "preflight_status": preflight_status,
            "attempt": attempt,
            "remediation_hint_count": max(0, hint_count),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    def _augment_guardrail_metadata(
        self,
        *,
        metadata: Dict[str, object],
        context: Dict[str, object],
    ) -> Dict[str, object]:
        base = dict(metadata) if isinstance(metadata, dict) else {}
        signal = self._contract_guardrail_signal(context)
        if not signal:
            base.pop("external_contract_pressure", None)
            return base

        existing = base.get("external_contract_pressure", {})
        pressure_map = dict(existing) if isinstance(existing, dict) else {}
        action_name = str(signal.get("action", "")).strip().lower()
        if not action_name:
            return base
        pressure_map[action_name] = {
            "pressure": self._coerce_float(signal.get("pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0),
            "code": str(signal.get("code", "")).strip().lower(),
            "severity": str(signal.get("severity", "")).strip().lower(),
            "severity_score": self._coerce_float(signal.get("severity_score", 0.0), minimum=0.0, maximum=1.0, default=0.0),
            "blocking_class": str(signal.get("blocking_class", "")).strip().lower(),
            "estimated_recovery_s": self._coerce_int(
                signal.get("estimated_recovery_s", 0),
                minimum=0,
                maximum=86_400,
                default=0,
            ),
            "automation_tier": str(signal.get("automation_tier", "")).strip().lower(),
            "execution_mode": str(signal.get("execution_mode", "")).strip().lower(),
            "execution_max_retry_attempts": self._coerce_int(
                signal.get("execution_max_retry_attempts", 0),
                minimum=0,
                maximum=20,
                default=0,
            ),
            "execution_allow_provider_reroute": bool(signal.get("execution_allow_provider_reroute", True)),
            "execution_stop_conditions": [
                str(item).strip().lower()
                for item in (
                    signal.get("execution_stop_conditions", [])
                    if isinstance(signal.get("execution_stop_conditions", []), list)
                    else []
                )
                if str(item).strip()
            ][:12],
            "preflight_status": str(signal.get("preflight_status", "")).strip().lower(),
            "attempt": self._coerce_int(signal.get("attempt", 1), minimum=1, maximum=10_000, default=1),
            "updated_at": str(signal.get("updated_at", "")).strip(),
        }
        base["external_contract_pressure"] = pressure_map
        return base

    def _repair_memory_hints(
        self,
        *,
        goal_text: str,
        context: Dict[str, object],
        limit: int = 8,
    ) -> list[Dict[str, Any]]:
        bounded = self._coerce_int(limit, minimum=1, maximum=50, default=8)
        failed_action = str(context.get("last_failure_action", "")).strip().lower()
        if not failed_action or not failed_action.startswith("external_"):
            return []

        contract_raw = context.get("last_failure_external_contract", {})
        contract = contract_raw if isinstance(contract_raw, dict) else {}
        contract_code = str(contract.get("code", "")).strip().lower()
        external_raw = context.get("last_failure_external_reliability", {})
        external = external_raw if isinstance(external_raw, dict) else {}
        selected_provider = str(external.get("selected_provider", "")).strip().lower()

        query_parts = [str(goal_text or "").strip(), failed_action]
        if contract_code:
            query_parts.append(contract_code.replace("_", " "))
        if selected_provider:
            query_parts.append(selected_provider)
        query = " ".join(part for part in query_parts if part).strip()
        if not query:
            query = failed_action

        search_limit = max(bounded * 4, 16)
        rows = self.runtime_memory.search(query, limit=search_limit)
        if not isinstance(rows, list) or not rows:
            return []

        hints: list[Dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            signals_raw = row.get("repair_signals", [])
            if not isinstance(signals_raw, list) or not signals_raw:
                continue

            ranked_signals: list[tuple[float, Dict[str, Any]]] = []
            for signal_raw in signals_raw:
                if not isinstance(signal_raw, dict):
                    continue
                action_name = str(signal_raw.get("action", "")).strip().lower()
                if action_name != failed_action:
                    continue
                status = str(signal_raw.get("status", "")).strip().lower()
                signal_code = str(signal_raw.get("contract_code", "")).strip().lower()
                provider = str(signal_raw.get("provider", "")).strip().lower()
                score = 0.0
                if status == "success":
                    score += 0.58
                elif status == "failed":
                    score += 0.12
                else:
                    score += 0.2
                if contract_code and signal_code == contract_code:
                    score += 0.34
                elif contract_code and signal_code:
                    score += 0.1
                if selected_provider and provider and provider == selected_provider:
                    score += 0.08
                memory_score = self._coerce_float(row.get("memory_score", 0.0), minimum=0.0, maximum=10.0, default=0.0)
                score += min(0.22, memory_score * 0.08)
                ranked_signals.append((score, signal_raw))

            if not ranked_signals:
                continue
            ranked_signals.sort(key=lambda item: item[0], reverse=True)
            top_signals: list[Dict[str, Any]] = []
            for score, signal_payload in ranked_signals[:3]:
                clean_signal = {
                    "action": str(signal_payload.get("action", "")).strip().lower(),
                    "status": str(signal_payload.get("status", "")).strip().lower(),
                    "provider": str(signal_payload.get("provider", "")).strip().lower(),
                    "contract_code": str(signal_payload.get("contract_code", "")).strip().lower(),
                    "attempt": self._coerce_int(signal_payload.get("attempt", 1), minimum=1, maximum=10_000, default=1),
                    "score": round(max(0.0, min(2.0, score)), 6),
                    "completed_at": str(signal_payload.get("completed_at", "")).strip(),
                }
                args_payload = signal_payload.get("args", {})
                if isinstance(args_payload, dict) and args_payload:
                    clean_signal["args"] = self._sanitize_replan_args(args_payload)
                top_signals.append(clean_signal)

            provider_key = str(top_signals[0].get("provider", "")).strip().lower() if top_signals else ""
            dedupe_key = f"{failed_action}|{provider_key}|{contract_code}|{str(row.get('memory_id', '')).strip()}"
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            hints.append(
                {
                    "memory_id": str(row.get("memory_id", "")).strip(),
                    "memory_score": self._coerce_float(row.get("memory_score", 0.0), minimum=0.0, maximum=10.0, default=0.0),
                    "goal_status": str(row.get("status", "")).strip().lower(),
                    "text": str(row.get("text", "")).strip()[:220],
                    "signals": top_signals,
                }
            )
            if len(hints) >= bounded:
                break

        return hints[:bounded]

    def _external_failure_clusters(
        self,
        *,
        goal_text: str,
        context: Dict[str, object],
        limit: int = 8,
    ) -> list[Dict[str, Any]]:
        bounded = self._coerce_int(limit, minimum=1, maximum=40, default=8)
        clean_goal = str(goal_text or "").strip()
        failed_action = str(context.get("last_failure_action", "")).strip().lower()
        contract_raw = context.get("last_failure_external_contract", {})
        contract = contract_raw if isinstance(contract_raw, dict) else {}
        failed_contract_code = str(contract.get("code", "")).strip().lower()

        query_parts = [clean_goal, failed_action]
        if failed_contract_code:
            query_parts.append(failed_contract_code.replace("_", " "))
        query = " ".join(part for part in query_parts if part).strip() or clean_goal
        if not query:
            return []

        search_limit = self._coerce_int(max(48, bounded * 12), minimum=24, maximum=400, default=96)
        search_rows = self.runtime_memory.search(query, limit=search_limit)
        recent_rows = self.runtime_memory.recent_hints(limit=search_limit)
        merged_rows: list[Dict[str, Any]] = []
        seen_rows: set[str] = set()
        for row in [*(search_rows if isinstance(search_rows, list) else []), *(recent_rows if isinstance(recent_rows, list) else [])]:
            if not isinstance(row, dict):
                continue
            memory_id = str(row.get("memory_id", "")).strip()
            if not memory_id:
                memory_id = (
                    f"{str(row.get('created_at', '')).strip()}|"
                    f"{str(row.get('text', '')).strip()}|"
                    f"{str(row.get('status', '')).strip()}"
                )
            if memory_id in seen_rows:
                continue
            seen_rows.add(memory_id)
            merged_rows.append(row)

        if not merged_rows:
            return []

        allowed_patch_fields = {
            "provider",
            "message_id",
            "event_id",
            "document_id",
            "task_id",
            "list_id",
            "calendar_id",
            "query",
            "max_results",
            "status",
            "title",
            "subject",
        }
        groups: Dict[str, Dict[str, Any]] = {}
        for row in merged_rows:
            signals_raw = row.get("repair_signals", [])
            if not isinstance(signals_raw, list):
                continue
            for signal in signals_raw[:32]:
                if not isinstance(signal, dict):
                    continue
                action = str(signal.get("action", "")).strip().lower()
                if not action.startswith(("external_", "oauth_token_")):
                    continue
                status = str(signal.get("status", "")).strip().lower() or "unknown"
                contract_code = str(signal.get("contract_code", "")).strip().lower() or "unknown"
                provider = str(signal.get("provider", "")).strip().lower()
                group_key = f"{action}|{contract_code}"
                aggregate = groups.get(group_key)
                if aggregate is None:
                    aggregate = {
                        "action": action,
                        "contract_code": contract_code,
                        "samples": 0,
                        "successes": 0,
                        "failures": 0,
                        "blocked": 0,
                        "unknown": 0,
                        "latest_status": "",
                        "latest_at": "",
                        "provider_stats": {},
                        "arg_support": {},
                    }
                    groups[group_key] = aggregate
                aggregate["samples"] = int(aggregate.get("samples", 0)) + 1
                if status == "success":
                    aggregate["successes"] = int(aggregate.get("successes", 0)) + 1
                elif status == "failed":
                    aggregate["failures"] = int(aggregate.get("failures", 0)) + 1
                elif status == "blocked":
                    aggregate["blocked"] = int(aggregate.get("blocked", 0)) + 1
                else:
                    aggregate["unknown"] = int(aggregate.get("unknown", 0)) + 1
                completed_at = str(signal.get("completed_at", "")).strip()
                latest_at = str(aggregate.get("latest_at", "")).strip()
                if completed_at and (not latest_at or completed_at >= latest_at):
                    aggregate["latest_at"] = completed_at
                    aggregate["latest_status"] = status

                provider_stats_raw = aggregate.get("provider_stats", {})
                provider_stats = provider_stats_raw if isinstance(provider_stats_raw, dict) else {}
                provider_key = provider or "_none"
                provider_row = provider_stats.get(provider_key, {})
                provider_row = dict(provider_row) if isinstance(provider_row, dict) else {}
                provider_row["provider"] = provider
                provider_row["samples"] = int(provider_row.get("samples", 0) or 0) + 1
                if status == "success":
                    provider_row["successes"] = int(provider_row.get("successes", 0) or 0) + 1
                elif status == "failed":
                    provider_row["failures"] = int(provider_row.get("failures", 0) or 0) + 1
                elif status == "blocked":
                    provider_row["blocked"] = int(provider_row.get("blocked", 0) or 0) + 1
                provider_stats[provider_key] = provider_row
                aggregate["provider_stats"] = provider_stats

                args_payload = signal.get("args", {})
                args = args_payload if isinstance(args_payload, dict) else {}
                if status == "success" and args:
                    arg_support_raw = aggregate.get("arg_support", {})
                    arg_support = arg_support_raw if isinstance(arg_support_raw, dict) else {}
                    for key, value in list(args.items())[:20]:
                        clean_key = str(key).strip().lower()
                        if clean_key not in allowed_patch_fields:
                            continue
                        if isinstance(value, (dict, list, tuple, set)) or value is None:
                            continue
                        value_text = str(value).strip()
                        if not value_text:
                            continue
                        field_rows_raw = arg_support.get(clean_key, {})
                        field_rows = field_rows_raw if isinstance(field_rows_raw, dict) else {}
                        field_rows[value_text] = int(field_rows.get(value_text, 0) or 0) + 1
                        arg_support[clean_key] = field_rows
                    aggregate["arg_support"] = arg_support

        if not groups:
            return []

        clusters: list[Dict[str, Any]] = []
        for group_key, row in groups.items():
            if not isinstance(row, dict):
                continue
            samples = self._coerce_int(row.get("samples", 0), minimum=0, maximum=100_000, default=0)
            if samples <= 0:
                continue
            action = str(row.get("action", "")).strip().lower()
            contract_code = str(row.get("contract_code", "")).strip().lower()
            if failed_action and action != failed_action and samples < 3:
                continue
            failures = self._coerce_int(row.get("failures", 0), minimum=0, maximum=100_000, default=0)
            blocked = self._coerce_int(row.get("blocked", 0), minimum=0, maximum=100_000, default=0)
            successes = self._coerce_int(row.get("successes", 0), minimum=0, maximum=100_000, default=0)
            fail_total = failures + blocked
            failure_ratio = float(fail_total) / float(max(1, samples))
            success_ratio = float(successes) / float(max(1, samples))

            provider_rows_raw = row.get("provider_stats", {})
            provider_rows = provider_rows_raw if isinstance(provider_rows_raw, dict) else {}
            ranked_providers: list[Dict[str, Any]] = []
            preferred_provider = ""
            best_provider_score = -9999.0
            for provider_value in provider_rows.values():
                if not isinstance(provider_value, dict):
                    continue
                provider = str(provider_value.get("provider", "")).strip().lower()
                provider_samples = self._coerce_int(provider_value.get("samples", 0), minimum=0, maximum=100_000, default=0)
                provider_successes = self._coerce_int(provider_value.get("successes", 0), minimum=0, maximum=100_000, default=0)
                provider_failures = self._coerce_int(provider_value.get("failures", 0), minimum=0, maximum=100_000, default=0)
                provider_blocked = self._coerce_int(provider_value.get("blocked", 0), minimum=0, maximum=100_000, default=0)
                provider_success_rate = float(provider_successes) / float(max(1, provider_samples))
                provider_failure_rate = float(provider_failures + provider_blocked) / float(max(1, provider_samples))
                provider_score = (provider_successes * 1.8) - ((provider_failures + provider_blocked) * 0.85) + (provider_success_rate * 0.6)
                if provider and provider_score > best_provider_score:
                    best_provider_score = provider_score
                    preferred_provider = provider
                ranked_providers.append(
                    {
                        "provider": provider,
                        "samples": provider_samples,
                        "successes": provider_successes,
                        "failures": provider_failures,
                        "blocked": provider_blocked,
                        "success_rate": round(provider_success_rate, 6),
                        "failure_rate": round(provider_failure_rate, 6),
                    }
                )
            ranked_providers.sort(
                key=lambda item: (
                    -self._coerce_float(item.get("success_rate", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                    self._coerce_float(item.get("failure_rate", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                    -self._coerce_int(item.get("samples", 0), minimum=0, maximum=100_000, default=0),
                    str(item.get("provider", "")),
                )
            )

            arg_support_raw = row.get("arg_support", {})
            arg_support = arg_support_raw if isinstance(arg_support_raw, dict) else {}
            suggested_patch: Dict[str, Any] = {}
            top_args: list[Dict[str, Any]] = []
            if preferred_provider:
                suggested_patch["provider"] = preferred_provider
            for field in sorted(arg_support.keys()):
                field_rows_raw = arg_support.get(field, {})
                field_rows = field_rows_raw if isinstance(field_rows_raw, dict) else {}
                if not field_rows:
                    continue
                ranked_values = sorted(
                    field_rows.items(),
                    key=lambda item: (-int(item[1]), str(item[0])),
                )
                top_value, top_count = ranked_values[0]
                support = float(top_count) / float(max(1, successes))
                if field != "provider" and support >= 0.34 and len(top_args) < 4:
                    suggested_patch[field] = top_value
                top_args.append(
                    {
                        "field": str(field),
                        "value": str(top_value),
                        "support": round(max(0.0, min(1.0, support)), 6),
                        "count": int(top_count),
                    }
                )
                if len(top_args) >= 6:
                    break

            risk_score = min(
                1.0,
                max(
                    0.0,
                    (failure_ratio * 0.72)
                    + (min(1.0, float(samples) / 14.0) * 0.18)
                    + (0.1 if str(row.get("latest_status", "")).strip().lower() in {"failed", "blocked"} else 0.0),
                ),
            )
            relevance = 0.0
            if failed_action and action == failed_action:
                relevance += 1.0
            if failed_contract_code and contract_code == failed_contract_code:
                relevance += 0.8
            if failed_contract_code and contract_code == "unknown":
                relevance += 0.1
            relevance += min(0.5, success_ratio * 0.35 + (float(samples) / 40.0))

            clusters.append(
                {
                    "cluster_id": f"cluster_{abs(hash(group_key)) % 100000000:08d}",
                    "action": action,
                    "contract_code": contract_code,
                    "samples": samples,
                    "successes": successes,
                    "failures": failures,
                    "blocked": blocked,
                    "failure_ratio": round(max(0.0, min(1.0, failure_ratio)), 6),
                    "success_ratio": round(max(0.0, min(1.0, success_ratio)), 6),
                    "risk_score": round(risk_score, 6),
                    "relevance": round(relevance, 6),
                    "preferred_provider": preferred_provider,
                    "provider_stats": ranked_providers[:6],
                    "top_args": top_args[:6],
                    "suggested_patch": self._sanitize_replan_args(suggested_patch) if suggested_patch else {},
                    "latest_status": str(row.get("latest_status", "")).strip().lower(),
                    "latest_at": str(row.get("latest_at", "")).strip(),
                }
            )

        clusters.sort(
            key=lambda row: (
                -self._coerce_float(row.get("relevance", 0.0), minimum=0.0, maximum=10.0, default=0.0),
                -self._coerce_float(row.get("risk_score", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                -self._coerce_int(row.get("samples", 0), minimum=0, maximum=100_000, default=0),
                str(row.get("action", "")),
                str(row.get("contract_code", "")),
            )
        )
        return clusters[:bounded]

    def _external_providers_for_plan(self, plan: ExecutionPlan) -> list[str]:
        providers: list[str] = []
        if not isinstance(plan, ExecutionPlan):
            return providers
        for step in plan.steps:
            if not isinstance(step, PlanStep):
                continue
            action = str(step.action or "").strip().lower()
            if not action:
                continue
            managed = action.startswith("external_") or action.startswith("oauth_token_")
            if not managed:
                continue
            args = step.args if isinstance(step.args, dict) else {}
            explicit_provider = str(args.get("provider", "")).strip().lower()
            if explicit_provider and explicit_provider not in {"auto", ""}:
                providers.append(explicit_provider)
                continue
            if action.startswith("external_email_"):
                providers.extend(["google", "graph", "smtp"])
            elif action.startswith("oauth_token_"):
                providers.extend(["google", "graph"])
            else:
                providers.extend(["google", "graph"])
        deduped: list[str] = []
        seen: set[str] = set()
        for provider in providers:
            clean = str(provider or "").strip().lower()
            if not clean or clean in seen:
                continue
            seen.add(clean)
            deduped.append(clean)
        return deduped

    def _external_auth_runtime_state(self, *, providers: list[str]) -> Dict[str, Any]:
        clean_providers = [str(item or "").strip().lower() for item in providers if str(item or "").strip()]
        if not clean_providers:
            return {}
        try:
            token_payload = self.oauth_store.list(limit=500, include_secrets=False)
        except Exception:  # noqa: BLE001
            token_payload = {}
        token_items = token_payload.get("items", []) if isinstance(token_payload, dict) else []
        rows_by_provider: Dict[str, list[Dict[str, Any]]] = {}
        if isinstance(token_items, list):
            for item in token_items:
                if not isinstance(item, dict):
                    continue
                provider = str(item.get("provider", "")).strip().lower()
                if not provider:
                    continue
                rows_by_provider.setdefault(provider, []).append(item)

        providers_state: Dict[str, Dict[str, Any]] = {}
        for provider in clean_providers:
            rows = rows_by_provider.get(provider, [])
            if rows:
                ranked = sorted(
                    rows,
                    key=lambda row: (
                        bool(row.get("has_access_token", False)),
                        bool(row.get("has_refresh_token", False)),
                        self._coerce_int(
                            row.get("expires_in_s", -10_000_000),
                            minimum=-10_000_000,
                            maximum=10_000_000,
                            default=-10_000_000,
                        ),
                        str(row.get("updated_at", "")),
                        str(row.get("account_id", "")),
                    ),
                    reverse=True,
                )
                best = ranked[0]
                scopes_raw = best.get("scopes", [])
                scopes_items = scopes_raw if isinstance(scopes_raw, list) else []
                scopes = [
                    str(item).strip().lower()
                    for item in scopes_items
                    if str(item).strip()
                ]
                providers_state[provider] = {
                    "source": "oauth_store",
                    "account_id": str(best.get("account_id", "")).strip().lower() or "default",
                    "has_credentials": bool(best.get("has_access_token", False)),
                    "has_refresh_token": bool(best.get("has_refresh_token", False)),
                    "expires_in_s": best.get("expires_in_s"),
                    "scopes": scopes,
                }
                continue
            if provider == "smtp":
                has_smtp_credentials = bool(
                    str(os.getenv("SMTP_HOST", "")).strip()
                    and str(os.getenv("SMTP_USERNAME", "")).strip()
                    and str(os.getenv("SMTP_PASSWORD", "")).strip()
                )
                providers_state[provider] = {
                    "source": "env",
                    "account_id": "default",
                    "has_credentials": has_smtp_credentials,
                    "has_refresh_token": False,
                    "expires_in_s": None,
                    "scopes": [],
                }
                continue
            providers_state[provider] = {
                "source": "oauth_store",
                "account_id": "default",
                "has_credentials": False,
                "has_refresh_token": False,
                "expires_in_s": None,
                "scopes": [],
            }
        if not providers_state:
            return {}
        return {
            "source": "kernel_oauth_runtime",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "providers": providers_state,
        }

    def _summarize_mission_trends(self, *, limit: int = 220) -> Dict[str, Any]:
        bounded = self._coerce_int(limit, minimum=20, maximum=2000, default=220)
        status_counts: Dict[str, int] = {}
        mission_rows: list[Dict[str, str]] = []
        default_trend = {
            "mode": "stable",
            "pressure": 0.0,
            "risk_delta": 0.0,
            "quality_delta": 0.0,
            "failed_ratio_delta": 0.0,
            "blocked_ratio_delta": 0.0,
            "risk_trend": "stable",
            "quality_trend": "stable",
            "failed_trend": "stable",
            "blocked_trend": "stable",
            "recent_window": 0,
            "baseline_window": 0,
        }
        for status_name in ("running", "failed", "blocked", "completed", "cancelled"):
            listed = self.list_missions(status=status_name, limit=bounded)
            rows = listed.get("items", []) if isinstance(listed, dict) else []
            if not isinstance(rows, list):
                continue
            status_counts[status_name] = len(rows)
            for row in rows:
                if not isinstance(row, dict):
                    continue
                mission_id = str(row.get("mission_id", "")).strip()
                if mission_id:
                    mission_rows.append(
                        {
                            "mission_id": mission_id,
                            "status": status_name,
                            "updated_at": str(row.get("updated_at", "")).strip(),
                        }
                    )
        if not mission_rows:
            return {
                "status": "success",
                "count": 0,
                "status_counts": status_counts,
                "risk": {"avg_score": 0.0, "level": "low"},
                "quality": {"avg_score": 0.0, "level": "high"},
                "failed_ratio": 0.0,
                "blocked_ratio": 0.0,
                "recommendation": "insufficient_data",
                "trend": default_trend,
            }

        mission_rows.sort(key=lambda row: str(row.get("updated_at", "")), reverse=True)
        ordered_rows: list[Dict[str, str]] = []
        seen: set[str] = set()
        for row in mission_rows:
            mission_id = str(row.get("mission_id", "")).strip()
            if not mission_id or mission_id in seen:
                continue
            seen.add(mission_id)
            ordered_rows.append(row)
            if len(ordered_rows) >= bounded:
                break

        risk_scores: list[float] = []
        quality_scores: list[float] = []
        risk_by_mission: Dict[str, float] = {}
        quality_by_mission: Dict[str, float] = {}
        hotspots_retry = 0
        hotspots_failures = 0
        for row in ordered_rows:
            mission_id = str(row.get("mission_id", "")).strip()
            if not mission_id:
                continue
            diagnostics = self.mission_diagnostics(mission_id, hotspot_limit=4)
            if not isinstance(diagnostics, dict) or diagnostics.get("status") != "success":
                continue
            risk_payload = diagnostics.get("risk", {})
            quality_payload = diagnostics.get("quality", {})
            if isinstance(risk_payload, dict):
                score = self._coerce_float(risk_payload.get("score", 0.0), minimum=0.0, maximum=1.0, default=0.0)
                risk_scores.append(score)
                risk_by_mission[mission_id] = score
            if isinstance(quality_payload, dict):
                score = self._coerce_float(quality_payload.get("score", 0.0), minimum=0.0, maximum=1.0, default=0.0)
                quality_scores.append(score)
                quality_by_mission[mission_id] = score
            hotspots = diagnostics.get("hotspots", {})
            if isinstance(hotspots, dict):
                retry_rows = hotspots.get("retry", [])
                failure_rows = hotspots.get("failures", [])
                if isinstance(retry_rows, list):
                    hotspots_retry += len(retry_rows)
                if isinstance(failure_rows, list):
                    hotspots_failures += len(failure_rows)

        mission_count = len(ordered_rows)
        avg_risk = (sum(risk_scores) / max(1, len(risk_scores))) if risk_scores else 0.0
        avg_quality = (sum(quality_scores) / max(1, len(quality_scores))) if quality_scores else 0.0
        failed_ratio = float(status_counts.get("failed", 0) + status_counts.get("cancelled", 0)) / max(1.0, float(mission_count))
        blocked_ratio = float(status_counts.get("blocked", 0)) / max(1.0, float(mission_count))

        risk_level = "low"
        if avg_risk >= 0.66:
            risk_level = "high"
        elif avg_risk >= 0.4:
            risk_level = "medium"

        quality_level = "high"
        if avg_quality <= 0.48:
            quality_level = "low"
        elif avg_quality <= 0.72:
            quality_level = "medium"

        if risk_level == "high" or quality_level == "low" or failed_ratio >= 0.32:
            recommendation = "stability"
        elif avg_quality >= 0.78 and failed_ratio <= 0.12 and blocked_ratio <= 0.08:
            recommendation = "throughput"
        else:
            recommendation = "balanced"

        def _segment_stats(rows: list[Dict[str, str]]) -> Dict[str, float]:
            if not rows:
                return {
                    "avg_risk": 0.0,
                    "avg_quality": 0.0,
                    "failed_ratio": 0.0,
                    "blocked_ratio": 0.0,
                }
            segment_risk = [
                self._coerce_float(risk_by_mission.get(str(row.get("mission_id", "")).strip(), 0.0), minimum=0.0, maximum=1.0, default=0.0)
                for row in rows
            ]
            segment_quality = [
                self._coerce_float(
                    quality_by_mission.get(str(row.get("mission_id", "")).strip(), 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                for row in rows
            ]
            segment_failed = sum(
                1
                for row in rows
                if str(row.get("status", "")).strip().lower() in {"failed", "cancelled"}
            )
            segment_blocked = sum(
                1
                for row in rows
                if str(row.get("status", "")).strip().lower() == "blocked"
            )
            return {
                "avg_risk": sum(segment_risk) / max(1.0, float(len(segment_risk))),
                "avg_quality": sum(segment_quality) / max(1.0, float(len(segment_quality))),
                "failed_ratio": float(segment_failed) / max(1.0, float(len(rows))),
                "blocked_ratio": float(segment_blocked) / max(1.0, float(len(rows))),
            }

        segment_size = min(40, max(4, mission_count // 3))
        recent_rows = ordered_rows[:segment_size]
        baseline_rows = ordered_rows[segment_size : segment_size * 2]
        if not baseline_rows and mission_count > segment_size:
            baseline_rows = ordered_rows[segment_size:]

        trend = dict(default_trend)
        if recent_rows and baseline_rows:
            recent_stats = _segment_stats(recent_rows)
            baseline_stats = _segment_stats(baseline_rows)
            risk_delta = float(recent_stats.get("avg_risk", 0.0)) - float(baseline_stats.get("avg_risk", 0.0))
            quality_delta = float(recent_stats.get("avg_quality", 0.0)) - float(baseline_stats.get("avg_quality", 0.0))
            failed_delta = float(recent_stats.get("failed_ratio", 0.0)) - float(baseline_stats.get("failed_ratio", 0.0))
            blocked_delta = float(recent_stats.get("blocked_ratio", 0.0)) - float(baseline_stats.get("blocked_ratio", 0.0))
            trend_pressure = self._coerce_float(
                (max(0.0, risk_delta) * 0.42)
                + (max(0.0, -quality_delta) * 0.38)
                + (max(0.0, failed_delta) * 0.28)
                + (max(0.0, blocked_delta) * 0.2),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            risk_trend = "stable"
            if risk_delta >= 0.05:
                risk_trend = "worsening"
            elif risk_delta <= -0.05:
                risk_trend = "improving"
            quality_trend = "stable"
            if quality_delta >= 0.05:
                quality_trend = "improving"
            elif quality_delta <= -0.05:
                quality_trend = "degrading"
            failed_trend = "stable"
            if failed_delta >= 0.05:
                failed_trend = "worsening"
            elif failed_delta <= -0.05:
                failed_trend = "improving"
            blocked_trend = "stable"
            if blocked_delta >= 0.04:
                blocked_trend = "worsening"
            elif blocked_delta <= -0.04:
                blocked_trend = "improving"
            mode = "stable"
            if trend_pressure >= 0.3 or risk_trend == "worsening" or quality_trend == "degrading":
                mode = "worsening"
            elif trend_pressure <= 0.12 and risk_trend == "improving" and quality_trend == "improving":
                mode = "improving"
            trend = {
                "mode": mode,
                "pressure": round(trend_pressure, 4),
                "risk_delta": round(risk_delta, 4),
                "quality_delta": round(quality_delta, 4),
                "failed_ratio_delta": round(failed_delta, 4),
                "blocked_ratio_delta": round(blocked_delta, 4),
                "risk_trend": risk_trend,
                "quality_trend": quality_trend,
                "failed_trend": failed_trend,
                "blocked_trend": blocked_trend,
                "recent_window": len(recent_rows),
                "baseline_window": len(baseline_rows),
            }

        return {
            "status": "success",
            "count": mission_count,
            "status_counts": status_counts,
            "risk": {"avg_score": round(avg_risk, 4), "level": risk_level},
            "quality": {"avg_score": round(avg_quality, 4), "level": quality_level},
            "failed_ratio": round(failed_ratio, 4),
            "blocked_ratio": round(blocked_ratio, 4),
            "hotspots": {
                "retry_total": int(hotspots_retry),
                "failure_total": int(hotspots_failures),
            },
            "recommendation": recommendation,
            "trend": trend,
        }

    def _runtime_mission_trend_feedback(self, *, force: bool = False) -> Dict[str, Any]:
        if not bool(getattr(self, "runtime_policy_trend_feedback_enabled", True)):
            return {}
        now = time.monotonic()
        refresh_s = self._coerce_float(
            getattr(self, "runtime_policy_trend_refresh_s", 45),
            minimum=5.0,
            maximum=1800.0,
            default=45.0,
        )
        cached = getattr(self, "_last_mission_trend_feedback", {})
        last_mono = self._coerce_float(
            getattr(self, "_last_mission_trend_feedback_monotonic", 0.0),
            minimum=0.0,
            maximum=1_000_000_000.0,
            default=0.0,
        )
        if not force and isinstance(cached, dict) and cached and (now - last_mono) < refresh_s:
            return dict(cached)

        summary = self._summarize_mission_trends(
            limit=self._coerce_int(
                getattr(self, "runtime_policy_trend_limit", 220),
                minimum=40,
                maximum=2000,
                default=220,
            )
        )
        if not isinstance(summary, dict) or str(summary.get("status", "")).strip().lower() != "success":
            return {}
        trend = summary.get("trend", {})
        trend_row = trend if isinstance(trend, dict) else {}
        feedback = {
            "status": "success",
            "last_updated_at": datetime.now(timezone.utc).isoformat(),
            "mode": str(trend_row.get("mode", "stable")).strip().lower() or "stable",
            "trend_pressure": self._coerce_float(trend_row.get("pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0),
            "risk_trend": str(trend_row.get("risk_trend", "stable")).strip().lower() or "stable",
            "quality_trend": str(trend_row.get("quality_trend", "stable")).strip().lower() or "stable",
            "failed_trend": str(trend_row.get("failed_trend", "stable")).strip().lower() or "stable",
            "blocked_trend": str(trend_row.get("blocked_trend", "stable")).strip().lower() or "stable",
            "risk_delta": self._coerce_float(trend_row.get("risk_delta", 0.0), minimum=-1.0, maximum=1.0, default=0.0),
            "quality_delta": self._coerce_float(trend_row.get("quality_delta", 0.0), minimum=-1.0, maximum=1.0, default=0.0),
            "failed_ratio_delta": self._coerce_float(
                trend_row.get("failed_ratio_delta", 0.0),
                minimum=-1.0,
                maximum=1.0,
                default=0.0,
            ),
            "blocked_ratio_delta": self._coerce_float(
                trend_row.get("blocked_ratio_delta", 0.0),
                minimum=-1.0,
                maximum=1.0,
                default=0.0,
            ),
            "risk_level": str(summary.get("risk", {}).get("level", "") if isinstance(summary.get("risk", {}), dict) else "").strip().lower(),
            "quality_level": str(
                summary.get("quality", {}).get("level", "") if isinstance(summary.get("quality", {}), dict) else ""
            )
            .strip()
            .lower(),
            "failed_ratio": self._coerce_float(summary.get("failed_ratio", 0.0), minimum=0.0, maximum=1.0, default=0.0),
            "blocked_ratio": self._coerce_float(summary.get("blocked_ratio", 0.0), minimum=0.0, maximum=1.0, default=0.0),
            "recommendation": str(summary.get("recommendation", "")).strip().lower(),
        }
        self._last_mission_trend_feedback = dict(feedback)
        self._last_mission_trend_feedback_monotonic = now
        return feedback

    def _runtime_policy_telemetry_feedback(self, *, force: bool = False) -> Dict[str, Any]:
        if not bool(getattr(self, "runtime_policy_telemetry_feedback_enabled", True)):
            return {}
        cached = getattr(self, "_last_runtime_policy_telemetry_feedback", {})
        if not force and isinstance(cached, dict) and cached:
            status = str(cached.get("status", "")).strip().lower()
            if status == "success":
                return dict(cached)

        telemetry_limit = self._coerce_int(
            getattr(self, "runtime_policy_telemetry_feedback_limit", 900),
            minimum=20,
            maximum=10_000,
            default=900,
        )
        summary = self.telemetry.summary(limit=telemetry_limit)
        if not isinstance(summary, dict) or str(summary.get("status", "")).strip().lower() != "success":
            return {}

        sample_count = self._coerce_int(
            summary.get("count", 0),
            minimum=0,
            maximum=1_000_000,
            default=0,
        )
        min_events = self._coerce_int(
            getattr(self, "runtime_policy_telemetry_feedback_min_events", 28),
            minimum=1,
            maximum=5000,
            default=28,
        )
        if sample_count < min_events:
            if isinstance(cached, dict) and str(cached.get("status", "")).strip().lower() == "success":
                return dict(cached)
            return {
                "status": "insufficient_samples",
                "mode": "stable",
                "pressure": 0.0,
                "failure_ratio": 0.0,
                "event_rate_pressure": 0.0,
                "sample_count": sample_count,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }

        failure_ratio = self._coerce_float(
            summary.get("failure_ratio", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        events_per_s = self._coerce_float(
            summary.get("events_per_s", 0.0),
            minimum=0.0,
            maximum=1000.0,
            default=0.0,
        )
        event_rate_scale = self._coerce_float(
            getattr(self, "runtime_policy_telemetry_feedback_event_rate_scale", 8.0),
            minimum=0.5,
            maximum=50.0,
            default=8.0,
        )
        event_rate_pressure = self._coerce_float(
            events_per_s / max(0.5, event_rate_scale),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        failure_weight = self._coerce_float(
            getattr(self, "runtime_policy_telemetry_feedback_failure_weight", 0.74),
            minimum=0.05,
            maximum=0.95,
            default=0.74,
        )
        pressure_raw = self._coerce_float(
            (failure_ratio * failure_weight) + (event_rate_pressure * (1.0 - failure_weight)),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        decay = self._coerce_float(
            getattr(self, "runtime_policy_telemetry_feedback_decay", 0.72),
            minimum=0.3,
            maximum=0.99,
            default=0.72,
        )
        previous_pressure = self._coerce_float(
            cached.get("pressure", 0.0) if isinstance(cached, dict) else 0.0,
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        pressure = self._coerce_float(
            (previous_pressure * decay) + (pressure_raw * (1.0 - decay)),
            minimum=0.0,
            maximum=1.0,
            default=pressure_raw,
        )
        mode = "stable"
        if pressure >= 0.66:
            mode = "severe"
        elif pressure >= 0.4:
            mode = "moderate"
        row = {
            "status": "success",
            "mode": mode,
            "pressure": round(pressure, 6),
            "pressure_raw": round(pressure_raw, 6),
            "failure_ratio": round(failure_ratio, 6),
            "event_rate_pressure": round(event_rate_pressure, 6),
            "events_per_s": round(events_per_s, 6),
            "sample_count": int(sample_count),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._last_runtime_policy_telemetry_feedback = dict(row)
        return row

    def _policy_bandit_candidate_profiles(self, *, source_name: str) -> list[str]:
        profiles_payload = self.policy.list_profiles()
        items = profiles_payload.get("items", []) if isinstance(profiles_payload, dict) else []
        known = [
            str(item.get("name", "")).strip().lower()
            for item in items
            if isinstance(item, dict) and str(item.get("name", "")).strip()
        ]
        if not known:
            return []

        source = str(source_name or "").strip().lower()
        preferred_order = ["interactive", "automation_safe", "automation_power"]
        if source in {"desktop-schedule", "desktop-trigger", "voice-loop"}:
            preferred_order = ["automation_safe", "automation_power", "interactive"]
        elif source in {"desktop-mission", "desktop-macro"}:
            preferred_order = ["automation_power", "automation_safe", "interactive"]

        ranked = [name for name in preferred_order if name in known]
        for name in known:
            if name not in ranked:
                ranked.append(name)
        return ranked[:8]

    @staticmethod
    def _apply_execution_strategy_recommendation(
        *,
        metadata: Dict[str, object],
        recommendation: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not isinstance(metadata, dict):
            return {}
        if not isinstance(recommendation, dict) or str(recommendation.get("status", "")).strip().lower() != "success":
            return {}
        strategy = recommendation.get("strategy", {})
        if not isinstance(strategy, dict) or not strategy:
            return {}

        allow_override = AgentKernel._coerce_bool(
            metadata.get("allow_execution_strategy_override", False),
            default=False,
        )
        applied: Dict[str, Any] = {}
        for key in (
            "execution_allow_parallel",
            "execution_max_parallel_steps",
            "external_branch_strategy",
            "external_mutation_simulation_enabled",
            "verification_strictness",
        ):
            if key not in strategy:
                continue
            if key in metadata and not allow_override:
                continue
            metadata[key] = strategy.get(key)
            applied[key] = strategy.get(key)

        mode = str(recommendation.get("mode", "")).strip().lower()
        if mode:
            metadata["execution_strategy_mode"] = mode
            applied.setdefault("execution_strategy_mode", mode)
        confidence = recommendation.get("confidence")
        if confidence is not None and ("execution_strategy_confidence" not in metadata or allow_override):
            metadata["execution_strategy_confidence"] = float(confidence or 0.0)
            applied["execution_strategy_confidence"] = float(confidence or 0.0)
        return applied

    @staticmethod
    def _infer_policy_task_class(*, text: str, source: str) -> str:
        lowered = str(text or "").strip().lower()
        source_name = str(source or "").strip().lower().replace("-", "_")
        if not lowered:
            return f"{source_name}:generic"

        task_type = "generic"
        if any(token in lowered for token in ("email", "calendar", "document", "task", "oauth", "connector")):
            task_type = "external"
        elif any(token in lowered for token in ("click", "ui element", "window", "screen", "desktop", "open app")):
            task_type = "desktop"
        elif any(token in lowered for token in ("read file", "write file", "folder", "copy file", "backup file")):
            task_type = "filesystem"
        elif any(token in lowered for token in ("schedule", "every", "trigger", "repeat", "at ")):
            task_type = "automation"
        elif any(token in lowered for token in ("search", "read webpage", "extract links", "browser")):
            task_type = "browser"
        elif any(token in lowered for token in ("what", "who", "when", "time", "status")):
            task_type = "query"

        complexity = "simple"
        if any(marker in lowered for marker in (" and then ", " then ", " after ", " before ", ";", " and ")):
            complexity = "compound"
        words = [token for token in re.split(r"\s+", lowered) if token]
        if len(words) >= 16:
            complexity = "complex"
        return f"{source_name}:{task_type}:{complexity}"

    def _compute_policy_bandit_reward(
        self,
        *,
        results: list[ActionResult],
        outcome: str,
        mission_id: str = "",
        fallback_error: str = "",
    ) -> float:
        normalized_outcome = str(outcome or "").strip().lower()
        base = 0.5
        if normalized_outcome == "completed":
            base = 0.95
        elif normalized_outcome == "cancelled":
            base = 0.35
        elif normalized_outcome == "blocked":
            base = 0.18
        elif normalized_outcome == "failed":
            base = 0.06

        rows = results if isinstance(results, list) else []
        success_count = sum(1 for row in rows if isinstance(row, ActionResult) and row.status == "success")
        failed_count = sum(1 for row in rows if isinstance(row, ActionResult) and row.status == "failed")
        blocked_count = sum(1 for row in rows if isinstance(row, ActionResult) and row.status == "blocked")
        retry_penalty = 0.0
        if rows:
            avg_attempt = sum(max(1, int(row.attempt or 1)) for row in rows if isinstance(row, ActionResult)) / float(max(1, len(rows)))
            retry_penalty = max(0.0, avg_attempt - 1.0) * 0.07
            success_ratio = float(success_count) / max(1.0, float(len(rows)))
            failure_ratio = float(failed_count + blocked_count) / max(1.0, float(len(rows)))
            base += (success_ratio * 0.25)
            base -= (failure_ratio * 0.28)

        if mission_id:
            diagnostics = self.mission_control.diagnostics(mission_id, hotspot_limit=4)
            if isinstance(diagnostics, dict) and diagnostics.get("status") == "success":
                quality = diagnostics.get("quality", {})
                risk = diagnostics.get("risk", {})
                if isinstance(quality, dict):
                    base += self._coerce_float(quality.get("score", 0.0), minimum=0.0, maximum=1.0, default=0.0) * 0.18
                if isinstance(risk, dict):
                    base -= self._coerce_float(risk.get("score", 0.0), minimum=0.0, maximum=1.0, default=0.0) * 0.2

        if fallback_error:
            lowered_error = str(fallback_error).strip().lower()
            if any(token in lowered_error for token in ("timeout", "timed out", "rate limit", "service unavailable")):
                base -= 0.07
            if any(token in lowered_error for token in ("approval required", "not allowed", "denied")):
                base -= 0.04

        score = base - retry_penalty
        return max(0.0, min(1.0, round(score, 6)))

    def _record_policy_bandit_outcome(
        self,
        *,
        goal: GoalRecord,
        mission_id: str = "",
        results: list[ActionResult],
        outcome: str,
        fallback_error: str = "",
    ) -> None:
        if not isinstance(goal, GoalRecord):
            return
        metadata = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
        task_class = str(metadata.get(self._policy_bandit_task_class_key, "")).strip().lower()
        if not task_class:
            task_class = self._infer_policy_task_class(text=goal.request.text, source=goal.request.source)
        profile = str(metadata.get(self._policy_bandit_profile_key, "")).strip().lower()
        if not profile:
            profile = str(metadata.get("policy_profile", "")).strip().lower()
        if not profile:
            return

        reward = self._compute_policy_bandit_reward(
            results=results if isinstance(results, list) else [],
            outcome=outcome,
            mission_id=mission_id,
            fallback_error=fallback_error,
        )
        record_payload = self.policy_bandit.record_outcome(
            task_class=task_class,
            profile=profile,
            reward=reward,
            outcome=outcome,
            metadata={"goal_id": goal.goal_id},
        )
        if str(record_payload.get("status", "")).strip().lower() == "success":
            self.telemetry.emit(
                "policy.bandit_outcome",
                {
                    "goal_id": goal.goal_id,
                    "task_class": task_class,
                    "profile": profile,
                    "outcome": str(outcome or "").strip().lower(),
                    "reward": reward,
                },
            )

    def _record_execution_strategy_outcome(
        self,
        *,
        goal: GoalRecord,
        results: list[ActionResult],
        outcome: str,
    ) -> None:
        if not isinstance(goal, GoalRecord):
            return
        metadata = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
        task_class = str(metadata.get(self._policy_bandit_task_class_key, "")).strip().lower()
        if not task_class:
            task_class = self._infer_policy_task_class(text=goal.request.text, source=goal.request.source)
        payload = self.execution_strategy.record_outcome(
            task_class=task_class,
            outcome=outcome,
            results=results if isinstance(results, list) else [],
            metadata={
                "goal_id": goal.goal_id,
                "source": goal.request.source,
            },
        )
        if str(payload.get("status", "")).strip().lower() == "success":
            self.telemetry.emit(
                "execution_strategy.updated",
                {
                    "goal_id": goal.goal_id,
                    "task_class": task_class,
                    "mode": str(payload.get("mode", "")),
                    "mode_changed": bool(payload.get("mode_changed", False)),
                    "pulls": int(payload.get("pulls", 0) or 0),
                    "outcome": str(outcome or "").strip().lower(),
                },
            )

    def _resolve_replan_policy(
        self,
        *,
        source_name: str,
        metadata: Optional[Dict[str, object]] = None,
    ) -> Dict[str, Any]:
        payload = metadata if isinstance(metadata, dict) else {}
        source = str(source_name or "").strip().lower()

        default_max = max(0, int(getattr(self, "max_replans", 2)))
        configured_max = self._coerce_int(
            payload.get("max_replans", payload.get("replan_max_attempts", default_max)),
            minimum=0,
            maximum=12,
            default=default_max,
        )

        # Trigger/schedule jobs are bounded to avoid runaway autonomous retries.
        if source in {"desktop-trigger", "desktop-schedule"}:
            configured_max = min(configured_max, 2)

        allow_blocked_default = bool(getattr(self, "replan_allow_blocked", False))
        allow_non_retryable_default = bool(getattr(self, "replan_allow_non_retryable", False))
        allow_blocked = self._coerce_bool(
            payload.get("replan_allow_blocked", payload.get("allow_replan_blocked", allow_blocked_default)),
            default=allow_blocked_default,
        )
        allow_non_retryable = self._coerce_bool(
            payload.get("replan_allow_non_retryable", payload.get("allow_replan_non_retryable", allow_non_retryable_default)),
            default=allow_non_retryable_default,
        )

        category_limits_default: Dict[str, int] = {
            "rate_limited": 3,
            "timeout": 2,
            "transient": 2,
            "unknown": 1,
            "non_retryable": 0,
            "blocked": 0,
        }
        raw_category_limits = payload.get("replan_category_limits")
        category_limits: Dict[str, int] = dict(category_limits_default)
        if isinstance(raw_category_limits, dict):
            for key, value in raw_category_limits.items():
                clean_key = str(key or "").strip().lower()
                if not clean_key:
                    continue
                category_limits[clean_key] = self._coerce_int(
                    value,
                    minimum=0,
                    maximum=12,
                    default=category_limits.get(clean_key, 1),
                )

        delay_base_default = float(getattr(self, "replan_delay_base_s", 0.0))
        delay_base_s = self._coerce_float(
            payload.get("replan_delay_base_s", delay_base_default),
            minimum=0.0,
            maximum=30.0,
            default=delay_base_default,
        )

        escalate_recovery_default = bool(getattr(self, "replan_escalate_recovery_profile", True))
        escalate_verification_default = bool(getattr(self, "replan_escalate_verification", True))
        escalate_policy_default = bool(getattr(self, "replan_escalate_policy_profile", True))
        escalate_recovery = self._coerce_bool(
            payload.get("replan_escalate_recovery_profile", escalate_recovery_default),
            default=escalate_recovery_default,
        )
        escalate_verification = self._coerce_bool(
            payload.get("replan_escalate_verification", escalate_verification_default),
            default=escalate_verification_default,
        )
        escalate_policy = self._coerce_bool(
            payload.get("replan_escalate_policy_profile", escalate_policy_default),
            default=escalate_policy_default,
        )

        return {
            "max_replans": configured_max,
            "allow_blocked": allow_blocked,
            "allow_non_retryable": allow_non_retryable,
            "category_limits": category_limits,
            "delay_base_s": delay_base_s,
            "escalate_recovery_profile": escalate_recovery,
            "escalate_verification": escalate_verification,
            "escalate_policy_profile": escalate_policy,
        }

    @classmethod
    def _should_replan_after_failure(
        cls,
        *,
        failed: ActionResult,
        attempt: int,
        failure_category: str,
        policy: Dict[str, Any],
    ) -> tuple[bool, str]:
        max_replans = cls._coerce_int(
            policy.get("max_replans", 0),
            minimum=0,
            maximum=12,
            default=0,
        )
        if attempt >= max_replans:
            return (False, f"replan budget exhausted ({max_replans})")

        status = str(failed.status or "").strip().lower()
        if status == "blocked" and not bool(policy.get("allow_blocked", False)):
            return (False, "blocked result is not eligible for replanning")

        normalized_category = str(failure_category or "").strip().lower() or "unknown"
        if normalized_category == "non_retryable" and not bool(policy.get("allow_non_retryable", False)):
            return (False, "non-retryable failure category is excluded by policy")

        raw_limits = policy.get("category_limits")
        category_limits = raw_limits if isinstance(raw_limits, dict) else {}
        category_cap_raw = category_limits.get(normalized_category)
        if category_cap_raw is not None:
            category_cap = cls._coerce_int(category_cap_raw, minimum=0, maximum=12, default=max_replans)
            if attempt >= category_cap:
                return (False, f"category retry budget exhausted for {normalized_category} ({category_cap})")

        if status == "blocked":
            blocked_cap_raw = category_limits.get("blocked")
            if blocked_cap_raw is not None:
                blocked_cap = cls._coerce_int(blocked_cap_raw, minimum=0, maximum=12, default=0)
                if attempt >= blocked_cap:
                    return (False, f"blocked retry budget exhausted ({blocked_cap})")

        return (True, "eligible")

    @classmethod
    def _compute_replan_delay_s(
        cls,
        *,
        policy: Dict[str, Any],
        failure_context: Dict[str, object],
        next_attempt: int,
    ) -> float:
        base_delay = cls._coerce_float(policy.get("delay_base_s", 0.0), minimum=0.0, maximum=30.0, default=0.0)
        if base_delay <= 0:
            return 0.0

        failure_category = str(failure_context.get("last_failure_category", "")).strip().lower() or "unknown"
        factor_by_category = {
            "rate_limited": 2.0,
            "timeout": 1.6,
            "transient": 1.35,
            "unknown": 1.1,
            "non_retryable": 1.0,
            "blocked": 1.0,
        }
        category_factor = factor_by_category.get(failure_category, 1.0)
        delay_s = base_delay * category_factor * max(1, int(next_attempt))

        # Use retry history as a floor when available to avoid thrashing services.
        recovery = failure_context.get("last_failure_recovery")
        if isinstance(recovery, dict):
            raw_history = recovery.get("retry_history")
            if isinstance(raw_history, list) and raw_history:
                last_row = raw_history[-1]
                if isinstance(last_row, dict):
                    history_delay = cls._coerce_float(last_row.get("delay_s", 0.0), minimum=0.0, maximum=60.0, default=0.0)
                    if history_delay > 0:
                        delay_s = max(delay_s, history_delay * 0.5)

        return max(0.0, min(delay_s, 30.0))

    def _runtime_policy_scope_key(self, metadata: Dict[str, object]) -> str:
        mission_key = str(getattr(self, "_mission_metadata_key", "__jarvis_mission_id") or "__jarvis_mission_id")
        goal_key = str(getattr(self, "_goal_metadata_key", "__jarvis_goal_id") or "__jarvis_goal_id")
        task_key = str(
            getattr(self, "_policy_bandit_task_class_key", "__jarvis_policy_bandit_task_class")
            or "__jarvis_policy_bandit_task_class"
        )
        mission_id = str(metadata.get(mission_key, "")).strip()
        if mission_id:
            return f"mission:{mission_id}"
        goal_id = str(metadata.get(goal_key, "")).strip()
        if goal_id:
            return f"goal:{goal_id}"
        task_class = str(metadata.get(task_key, "")).strip().lower()
        if task_class:
            return f"class:{task_class}"
        profile = str(metadata.get("policy_profile", "")).strip().lower() or "default"
        source = str(metadata.get("source", "")).strip().lower() or "unknown"
        return f"profile:{profile}|source:{source}"

    def _runtime_policy_prune_signal_state(self) -> None:
        state = getattr(self, "_runtime_policy_signal_state", None)
        if not isinstance(state, dict) or not state:
            return
        max_scopes = self._coerce_int(
            getattr(self, "runtime_policy_signal_state_max_scopes", 3000),
            minimum=2,
            maximum=100_000,
            default=3000,
        )
        if len(state) <= max_scopes:
            return
        overflow = len(state) - max_scopes
        ordered = sorted(
            state.items(),
            key=lambda item: self._coerce_float(
                item[1].get("last_seen_monotonic", 0.0) if isinstance(item[1], dict) else 0.0,
                minimum=0.0,
                maximum=10_000_000_000.0,
                default=0.0,
            ),
        )
        for scope_key, _ in ordered[:overflow]:
            state.pop(scope_key, None)

    def _runtime_policy_smooth_signals(
        self,
        *,
        metadata: Dict[str, object],
        quality_score: float,
        confirm_failure_ratio: float,
        external_pressure: float,
        desktop_change_rate: float,
        trend_pressure: float,
    ) -> Dict[str, Any]:
        scope_key = self._runtime_policy_scope_key(metadata)
        if not bool(getattr(self, "runtime_policy_signal_smoothing_enabled", True)):
            return {
                "scope_key": scope_key,
                "quality_score": quality_score,
                "confirm_failure_ratio": confirm_failure_ratio,
                "external_pressure": external_pressure,
                "desktop_change_rate": desktop_change_rate,
                "trend_pressure": trend_pressure,
                "previous_mode": "stable",
                "samples": 1,
            }

        state = getattr(self, "_runtime_policy_signal_state", None)
        if not isinstance(state, dict):
            state = {}
            self._runtime_policy_signal_state = state
        now_monotonic = time.monotonic()
        row = state.get(scope_key)
        if not isinstance(row, dict):
            row = {}
            state[scope_key] = row

        stale_reset_s = self._coerce_float(
            getattr(self, "runtime_policy_signal_stale_reset_s", 1200),
            minimum=30.0,
            maximum=86_400.0,
            default=1200.0,
        )
        last_seen_monotonic = self._coerce_float(
            row.get("last_seen_monotonic", 0.0),
            minimum=0.0,
            maximum=10_000_000_000.0,
            default=0.0,
        )
        sample_count = self._coerce_int(row.get("samples", 0), minimum=0, maximum=1_000_000, default=0)
        stale = bool(sample_count <= 0 or (now_monotonic - last_seen_monotonic) >= stale_reset_s)
        alpha = self._coerce_float(
            getattr(self, "runtime_policy_signal_ema_alpha", 0.42),
            minimum=0.05,
            maximum=1.0,
            default=0.42,
        )

        def _smooth(field: str, value: float) -> float:
            bounded_value = self._coerce_float(value, minimum=0.0, maximum=1.0, default=value)
            if stale:
                smoothed = bounded_value
            else:
                previous = self._coerce_float(
                    row.get(field, bounded_value),
                    minimum=0.0,
                    maximum=1.0,
                    default=bounded_value,
                )
                smoothed = (alpha * bounded_value) + ((1.0 - alpha) * previous)
            row[field] = max(0.0, min(1.0, smoothed))
            return float(row[field])

        quality_ema = _smooth("quality_score_ema", quality_score)
        confirm_ema = _smooth("confirm_failure_ratio_ema", confirm_failure_ratio)
        external_ema = _smooth("external_pressure_ema", external_pressure)
        desktop_ema = _smooth("desktop_change_rate_ema", desktop_change_rate)
        trend_ema = _smooth("trend_pressure_ema", trend_pressure)

        row["samples"] = sample_count + 1
        row["last_seen_monotonic"] = now_monotonic
        row.setdefault("mode", "stable")
        state[scope_key] = row
        self._runtime_policy_prune_signal_state()

        return {
            "scope_key": scope_key,
            "quality_score": quality_ema,
            "confirm_failure_ratio": confirm_ema,
            "external_pressure": external_ema,
            "desktop_change_rate": desktop_ema,
            "trend_pressure": trend_ema,
            "previous_mode": str(row.get("mode", "stable")).strip().lower() or "stable",
            "samples": int(row.get("samples", 0) or 0),
        }

    def _runtime_policy_set_mode(self, *, scope_key: str, mode: str) -> None:
        state = getattr(self, "_runtime_policy_signal_state", None)
        if not isinstance(state, dict):
            return
        row = state.get(scope_key)
        if not isinstance(row, dict):
            row = {}
            state[scope_key] = row
        normalized_mode = str(mode or "").strip().lower() or "stable"
        row["mode"] = normalized_mode
        row["last_mode_updated_at"] = datetime.now(timezone.utc).isoformat()
        row["last_seen_monotonic"] = time.monotonic()
        state[scope_key] = row

    @staticmethod
    def _normalize_external_route_profile(value: str) -> str:
        clean = str(value or "").strip().lower()
        if clean in {"defensive", "cautious", "balanced", "throughput"}:
            return clean
        return "balanced"

    def _derive_runtime_external_route_overrides(
        self,
        *,
        metadata: Dict[str, object],
        runtime_mode: str,
        attempt: int,
        quality_score: float,
        external_pressure: float,
        contract_pressure: float,
        mission_trend_worsening: bool,
        mission_trend_improving: bool,
    ) -> Dict[str, str]:
        if not bool(getattr(self, "runtime_external_route_adaptation_enabled", True)):
            return {}

        severe_profile = self._normalize_external_route_profile(
            getattr(self, "runtime_external_route_severe_profile", "defensive")
        )
        moderate_profile = self._normalize_external_route_profile(
            getattr(self, "runtime_external_route_moderate_profile", "cautious")
        )
        stable_profile = self._normalize_external_route_profile(
            getattr(self, "runtime_external_route_stable_profile", "balanced")
        )
        throughput_profile = self._normalize_external_route_profile(
            getattr(self, "runtime_external_route_throughput_profile", "throughput")
        )
        throughput_floor = self._coerce_float(
            getattr(self, "runtime_external_route_throughput_quality_floor", 0.88),
            minimum=0.5,
            maximum=0.99,
            default=0.88,
        )
        selected_profile = stable_profile
        if runtime_mode == "severe":
            selected_profile = severe_profile
        elif runtime_mode == "moderate":
            selected_profile = moderate_profile
        else:
            healthy_for_throughput = bool(
                quality_score >= throughput_floor
                and external_pressure <= 0.18
                and contract_pressure <= 0.16
                and not mission_trend_worsening
                and (mission_trend_improving or str(metadata.get("policy_profile", "")).strip().lower() in {"automation_power", "automation_safe"})
            )
            if healthy_for_throughput:
                selected_profile = throughput_profile

        updates: Dict[str, str] = {}
        current_profile = self._normalize_external_route_profile(str(metadata.get("external_route_profile", "")))
        if selected_profile != current_profile:
            updates["external_route_profile"] = selected_profile

        severe_probe = self._coerce_float(
            getattr(self, "runtime_external_route_probe_severe", 0.96),
            minimum=0.0,
            maximum=1.0,
            default=0.96,
        )
        moderate_probe = self._coerce_float(
            getattr(self, "runtime_external_route_probe_moderate", 0.72),
            minimum=0.0,
            maximum=1.0,
            default=0.72,
        )
        stable_probe = self._coerce_float(
            getattr(self, "runtime_external_route_probe_stable", 0.28),
            minimum=0.0,
            maximum=1.0,
            default=0.28,
        )
        entropy_force_enabled = bool(getattr(self, "runtime_external_route_entropy_force_enabled", True))
        if runtime_mode == "severe":
            updates["external_route_entropy_force"] = "false"
            updates["external_route_entropy_probe"] = f"{severe_probe:.6f}"
            updates["external_route_entropy_select_probe"] = "0.12"
            updates["external_cooldown_override"] = "false"
            updates["external_outage_override"] = "false"
        elif runtime_mode == "moderate":
            updates["external_route_entropy_force"] = "false"
            updates["external_route_entropy_probe"] = f"{moderate_probe:.6f}"
            updates["external_route_entropy_select_probe"] = "0.34"
            updates["external_cooldown_override"] = "false"
            updates["external_outage_override"] = "false"
        else:
            force_entropy = bool(
                entropy_force_enabled
                and selected_profile == "throughput"
                and contract_pressure <= 0.12
                and external_pressure <= 0.16
                and attempt == 0
            )
            select_probe = self._coerce_float(
                0.52 + (external_pressure * 0.2) + (contract_pressure * 0.16) - ((quality_score - 0.5) * 0.22),
                minimum=0.05,
                maximum=0.95,
                default=0.52,
            )
            updates["external_route_entropy_force"] = "true" if force_entropy else "false"
            updates["external_route_entropy_probe"] = f"{stable_probe:.6f}"
            updates["external_route_entropy_select_probe"] = f"{select_probe:.6f}"
            cooldown_override = bool(
                selected_profile == "throughput"
                and contract_pressure <= 0.1
                and external_pressure <= 0.12
                and attempt == 0
            )
            outage_override = bool(
                selected_profile == "throughput"
                and mission_trend_improving
                and contract_pressure <= 0.08
                and external_pressure <= 0.08
                and attempt == 0
            )
            updates["external_cooldown_override"] = "true" if cooldown_override else "false"
            updates["external_outage_override"] = "true" if outage_override else "false"

        if bool(getattr(self, "runtime_external_remediation_budget_enabled", True)):
            severe_actions = self._coerce_int(
                getattr(self, "runtime_external_remediation_actions_severe", 5),
                minimum=1,
                maximum=8,
                default=5,
            )
            moderate_actions = self._coerce_int(
                getattr(self, "runtime_external_remediation_actions_moderate", 3),
                minimum=1,
                maximum=8,
                default=3,
            )
            stable_actions = self._coerce_int(
                getattr(self, "runtime_external_remediation_actions_stable", 2),
                minimum=1,
                maximum=8,
                default=2,
            )
            severe_total = self._coerce_int(
                getattr(self, "runtime_external_remediation_total_severe", 12),
                minimum=2,
                maximum=24,
                default=12,
            )
            moderate_total = self._coerce_int(
                getattr(self, "runtime_external_remediation_total_moderate", 8),
                minimum=2,
                maximum=24,
                default=8,
            )
            stable_total = self._coerce_int(
                getattr(self, "runtime_external_remediation_total_stable", 6),
                minimum=2,
                maximum=24,
                default=6,
            )
            if runtime_mode == "severe":
                actions_budget = severe_actions + (1 if contract_pressure >= 0.62 else 0)
                total_budget = severe_total + (2 if contract_pressure >= 0.62 else 0)
            elif runtime_mode == "moderate":
                actions_budget = moderate_actions + (1 if contract_pressure >= 0.48 else 0)
                total_budget = moderate_total + (1 if contract_pressure >= 0.48 else 0)
            else:
                actions_budget = stable_actions
                total_budget = stable_total
                if selected_profile == "throughput" and contract_pressure <= 0.14 and external_pressure <= 0.16:
                    actions_budget = max(1, actions_budget - 1)
                    total_budget = max(2, total_budget - 1)
            actions_budget = self._coerce_int(actions_budget, minimum=1, maximum=8, default=stable_actions)
            total_budget = self._coerce_int(total_budget, minimum=2, maximum=24, default=stable_total)
            updates["external_remediation_max_actions"] = str(actions_budget)
            updates["external_remediation_max_total_actions"] = str(total_budget)

        base_risk_floor = self._coerce_float(
            getattr(self, "runtime_external_contract_risk_floor", 0.14),
            minimum=0.0,
            maximum=1.0,
            default=0.14,
        )
        if runtime_mode == "severe":
            risk_floor = max(base_risk_floor, 0.48, contract_pressure * 0.9)
        elif runtime_mode == "moderate":
            risk_floor = max(base_risk_floor, 0.28, contract_pressure * 0.76)
        else:
            risk_floor = max(base_risk_floor, contract_pressure * 0.62)
            if selected_profile == "throughput" and contract_pressure <= 0.14 and external_pressure <= 0.14:
                risk_floor = max(base_risk_floor * 0.72, risk_floor - 0.08)
        updates["external_remediation_contract_risk_floor"] = f"{self._coerce_float(risk_floor, minimum=0.0, maximum=1.0, default=base_risk_floor):.6f}"
        return updates

    def _derive_runtime_external_remediation_execution_overrides(
        self,
        *,
        metadata: Dict[str, object],
        runtime_mode: str,
        contract_signal: Dict[str, Any],
        contract_pressure: float,
        external_pressure: float,
    ) -> Dict[str, str]:
        signal = contract_signal if isinstance(contract_signal, dict) else {}
        if not signal:
            return {}
        updates: Dict[str, str] = {}
        blocking_class = str(signal.get("blocking_class", "")).strip().lower()
        signal_execution_mode = str(signal.get("execution_mode", "")).strip().lower()
        signal_automation_tier = str(signal.get("automation_tier", "")).strip().lower()
        signal_retry_cap = self._coerce_int(
            signal.get("execution_max_retry_attempts", 0),
            minimum=0,
            maximum=20,
            default=0,
        )
        signal_allow_reroute = self._coerce_bool(
            signal.get("execution_allow_provider_reroute", True),
            default=True,
        )
        signal_stop_conditions = [
            str(item).strip().lower()
            for item in (
                signal.get("execution_stop_conditions", [])
                if isinstance(signal.get("execution_stop_conditions", []), list)
                else []
            )
            if str(item).strip()
        ]
        severity_score = self._coerce_float(
            signal.get("severity_score", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        estimated_recovery_s = self._coerce_int(
            signal.get("estimated_recovery_s", 0),
            minimum=0,
            maximum=86_400,
            default=0,
        )

        target_mode = "automated"
        checkpoint_mode = "off"
        allow_provider_reroute = True
        if (
            runtime_mode == "severe"
            or contract_pressure >= 0.68
            or severity_score >= 0.82
            or estimated_recovery_s >= 1200
            or signal_execution_mode == "manual"
            or signal_automation_tier == "manual"
            or "manual_escalation" in signal_stop_conditions
        ):
            target_mode = "assisted"
            checkpoint_mode = "strict"
            allow_provider_reroute = bool(
                signal_allow_reroute
                and blocking_class not in {"auth"}
                and "provider_reroute_locked" not in signal_stop_conditions
            )
        elif (
            runtime_mode == "moderate"
            or contract_pressure >= 0.46
            or severity_score >= 0.62
            or estimated_recovery_s >= 480
            or signal_execution_mode == "assisted"
            or signal_automation_tier == "assisted"
        ):
            target_mode = "assisted"
            checkpoint_mode = "standard"
            allow_provider_reroute = bool(
                signal_allow_reroute
                and "provider_reroute_locked" not in signal_stop_conditions
            )
        else:
            target_mode = "automated"
            checkpoint_mode = "standard" if external_pressure >= 0.26 else "off"
            allow_provider_reroute = bool(signal_allow_reroute)

        if signal_retry_cap > 0 and signal_retry_cap <= 1:
            checkpoint_mode = "strict" if checkpoint_mode == "standard" else checkpoint_mode
        if blocking_class == "auth":
            allow_provider_reroute = False
            checkpoint_mode = "strict" if checkpoint_mode != "off" else "standard"
            if target_mode == "automated":
                target_mode = "assisted"

        updates["external_remediation_execution_mode"] = target_mode
        updates["external_remediation_checkpoint_mode"] = checkpoint_mode
        updates["external_remediation_allow_provider_reroute"] = "true" if allow_provider_reroute else "false"
        return updates

    def _derive_runtime_adaptive_overrides(
        self,
        *,
        metadata: Dict[str, object],
        context: Dict[str, object],
        attempt: int,
    ) -> Dict[str, str]:
        if not bool(getattr(self, "runtime_policy_adaptation_enabled", True)):
            return {}

        updates: Dict[str, str] = {}
        current_policy = str(metadata.get("policy_profile", "")).strip().lower()
        current_recovery = str(metadata.get("recovery_profile", "")).strip().lower()
        current_strictness = str(metadata.get("verification_strictness", "")).strip().lower() or "standard"
        if not current_recovery:
            current_recovery = self._default_recovery_profile(current_policy)

        execution_feedback = context.get("execution_feedback", {})
        mission_feedback = context.get("mission_feedback", {})
        mission_trend_feedback = context.get("mission_trend_feedback", {})
        provider_health_rows = context.get("external_provider_health", [])
        open_circuits = context.get("open_action_circuits", [])

        quality_score = 1.0
        confirm_failure_ratio = 0.0
        latest_failure_category = ""
        desktop_change_rate = 1.0
        remediation_attempted = 0
        remediation_success_rate = 0.0
        remediation_checkpoint_blocked_ratio = 0.0
        remediation_contract_risk = 0.0
        verification_failure_ratio = 0.0
        verification_pressure = 0.0
        verification_pressure_mode = "stable"
        if isinstance(execution_feedback, dict) and execution_feedback:
            quality_score = self._coerce_float(
                execution_feedback.get("quality_score", 1.0),
                minimum=0.0,
                maximum=1.0,
                default=1.0,
            )
            confirm_failure_ratio = self._coerce_float(
                execution_feedback.get("confirm_failure_ratio", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            latest_failure_category = str(execution_feedback.get("latest_failure_category", "")).strip().lower()
            desktop_change_rate = self._coerce_float(
                execution_feedback.get("desktop_change_rate", 1.0),
                minimum=0.0,
                maximum=1.0,
                default=1.0,
            )
            remediation_attempted = self._coerce_int(
                execution_feedback.get("remediation_attempted", 0),
                minimum=0,
                maximum=10_000,
                default=0,
            )
            remediation_success_rate = self._coerce_float(
                execution_feedback.get("remediation_success_rate", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            remediation_checkpoint_blocked_ratio = self._coerce_float(
                execution_feedback.get("remediation_checkpoint_blocked_ratio", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            remediation_contract_risk = self._coerce_float(
                execution_feedback.get("remediation_contract_risk", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            verification_failure_ratio = self._coerce_float(
                execution_feedback.get("verification_failure_ratio", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            verification_pressure = self._coerce_float(
                execution_feedback.get("verification_pressure", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            verification_pressure_mode = (
                str(execution_feedback.get("verification_pressure_mode", "stable")).strip().lower() or "stable"
            )

        mission_risk_high = False
        mission_quality_low = False
        if isinstance(mission_feedback, dict) and mission_feedback:
            mission_risk_high = str(mission_feedback.get("risk_level", "")).strip().lower() == "high"
            mission_quality_low = str(mission_feedback.get("quality_level", "")).strip().lower() == "low"

        trend_pressure = 0.0
        mission_trend_worsening = False
        mission_trend_improving = False
        if isinstance(mission_trend_feedback, dict) and mission_trend_feedback:
            trend_pressure = self._coerce_float(
                mission_trend_feedback.get("trend_pressure", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            trend_mode = str(mission_trend_feedback.get("mode", "")).strip().lower()
            risk_trend = str(mission_trend_feedback.get("risk_trend", "")).strip().lower()
            quality_trend = str(mission_trend_feedback.get("quality_trend", "")).strip().lower()
            mission_trend_worsening = bool(
                trend_mode == "worsening"
                or risk_trend == "worsening"
                or quality_trend == "degrading"
            )
            mission_trend_improving = bool(
                trend_mode == "improving"
                or risk_trend == "improving"
                or quality_trend == "improving"
            )

        telemetry_feedback_raw = context.get("telemetry_feedback", {})
        telemetry_feedback = telemetry_feedback_raw if isinstance(telemetry_feedback_raw, dict) else {}
        telemetry_mode = str(telemetry_feedback.get("mode", "")).strip().lower()
        telemetry_pressure = self._coerce_float(
            telemetry_feedback.get("pressure", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        telemetry_failure_ratio = self._coerce_float(
            telemetry_feedback.get("failure_ratio", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        telemetry_event_rate_pressure = self._coerce_float(
            telemetry_feedback.get("event_rate_pressure", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )

        mission_analysis_raw = context.get("external_reliability_mission_analysis", {})
        mission_analysis = mission_analysis_raw if isinstance(mission_analysis_raw, dict) else {}
        if not mission_analysis:
            cached_analysis = getattr(self, "_last_external_reliability_analysis", {})
            if isinstance(cached_analysis, dict) and cached_analysis:
                mission_analysis = dict(cached_analysis)
        mission_drift_mode = str(mission_analysis.get("drift_mode", "")).strip().lower() if isinstance(mission_analysis, dict) else ""
        mission_drift_score = self._coerce_float(
            mission_analysis.get("drift_score", 0.0) if isinstance(mission_analysis, dict) else 0.0,
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        mission_provider_policy_changed = bool(
            mission_analysis.get("provider_policy_changed", False) if isinstance(mission_analysis, dict) else False
        )
        mission_provider_policy_updated_count = self._coerce_int(
            mission_analysis.get("provider_policy_updated_count", 0) if isinstance(mission_analysis, dict) else 0,
            minimum=0,
            maximum=10_000,
            default=0,
        )

        provider_degraded_count = 0
        provider_total = 0
        if isinstance(provider_health_rows, list):
            for row in provider_health_rows:
                if not isinstance(row, dict):
                    continue
                provider_total += 1
                cooldown_active = bool(row.get("cooldown_active", False))
                failure_ema = self._coerce_float(row.get("failure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
                health_score = self._coerce_float(row.get("health_score", 0.5), minimum=0.0, maximum=1.0, default=0.5)
                if cooldown_active or failure_ema >= 0.68 or health_score <= 0.42:
                    provider_degraded_count += 1
        external_pressure = 0.0
        if provider_total > 0:
            external_pressure = float(provider_degraded_count) / float(provider_total)
        if isinstance(open_circuits, list):
            external_pressure = max(
                external_pressure,
                min(1.0, float(len(open_circuits)) / 10.0),
            )
        contract_signal = self._contract_guardrail_signal(context)
        contract_pressure = self._coerce_float(
            contract_signal.get("pressure", 0.0) if isinstance(contract_signal, dict) else 0.0,
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        contract_code = str(contract_signal.get("code", "")).strip().lower() if isinstance(contract_signal, dict) else ""
        if contract_pressure > 0.0 and bool(getattr(self, "runtime_policy_contract_guardrail_enabled", True)):
            external_pressure = max(external_pressure, contract_pressure)
        verification_pressure_enabled = bool(
            getattr(self, "runtime_policy_verification_pressure_enabled", True)
        )
        verification_pressure_threshold = self._coerce_float(
            getattr(self, "runtime_policy_verification_pressure_threshold", 0.36),
            minimum=0.05,
            maximum=0.95,
            default=0.36,
        )
        verification_pressure_severe_threshold = self._coerce_float(
            getattr(self, "runtime_policy_verification_pressure_severe_threshold", 0.62),
            minimum=0.1,
            maximum=1.0,
            default=0.62,
        )
        if verification_pressure_enabled and verification_pressure > 0.0:
            if verification_pressure_mode == "severe" or verification_pressure >= verification_pressure_severe_threshold:
                mission_trend_worsening = True
                trend_pressure = max(trend_pressure, min(1.0, verification_pressure))
                external_pressure = max(external_pressure, min(1.0, verification_pressure * 0.84))
                contract_pressure = max(contract_pressure, min(1.0, verification_pressure * 0.76))
                confirm_failure_ratio = max(confirm_failure_ratio, min(1.0, verification_failure_ratio + 0.08))
            elif verification_pressure_mode == "moderate" or verification_pressure >= verification_pressure_threshold:
                external_pressure = max(external_pressure, min(1.0, verification_pressure * 0.62))
                contract_pressure = max(contract_pressure, min(1.0, verification_pressure * 0.48))
                confirm_failure_ratio = max(confirm_failure_ratio, min(1.0, verification_failure_ratio + 0.03))
        telemetry_feedback_enabled = bool(getattr(self, "runtime_policy_telemetry_feedback_enabled", True))
        if telemetry_feedback_enabled and telemetry_pressure > 0.0:
            if telemetry_mode == "severe" or telemetry_pressure >= 0.68:
                mission_trend_worsening = True
                trend_pressure = max(trend_pressure, min(1.0, telemetry_pressure * 0.82))
                external_pressure = max(external_pressure, min(1.0, telemetry_pressure * 0.88))
                quality_score = min(
                    quality_score,
                    max(0.0, 1.0 - ((telemetry_failure_ratio * 0.74) + (telemetry_event_rate_pressure * 0.26))),
                )
            elif telemetry_mode == "moderate" or telemetry_pressure >= 0.4:
                external_pressure = max(external_pressure, min(1.0, telemetry_pressure * 0.64))
                quality_score = min(
                    quality_score,
                    max(0.0, 1.0 - ((telemetry_failure_ratio * 0.58) + (telemetry_event_rate_pressure * 0.18))),
                )

        mission_drift_enabled = bool(getattr(self, "runtime_policy_mission_drift_enabled", True))
        mission_drift_weight = self._coerce_float(
            getattr(self, "runtime_policy_mission_drift_weight", 0.32),
            minimum=0.0,
            maximum=1.0,
            default=0.32,
        )
        mission_drift_relief_weight = self._coerce_float(
            getattr(self, "runtime_policy_mission_drift_relief_weight", 0.18),
            minimum=0.0,
            maximum=1.0,
            default=0.18,
        )
        mission_drift_severe_threshold = self._coerce_float(
            getattr(self, "runtime_policy_mission_drift_severe_threshold", 0.66),
            minimum=0.1,
            maximum=1.0,
            default=0.66,
        )
        provider_policy_relief_enabled = bool(
            getattr(self, "runtime_policy_provider_policy_relief_enabled", True)
        )
        provider_policy_relief_gain = self._coerce_float(
            getattr(self, "runtime_policy_provider_policy_relief_gain", 0.12),
            minimum=0.0,
            maximum=0.5,
            default=0.12,
        )
        if mission_drift_enabled and mission_drift_score > 0.0:
            if mission_drift_mode in {"severe", "worsening"} or mission_drift_score >= mission_drift_severe_threshold:
                mission_trend_worsening = True
                trend_pressure = max(trend_pressure, mission_drift_score)
                external_pressure = min(
                    1.0,
                    external_pressure + (mission_drift_score * mission_drift_weight),
                )
            elif mission_drift_mode in {"stable", "improving"}:
                drift_relief = min(0.14, (1.0 - mission_drift_score) * mission_drift_relief_weight)
                external_pressure = max(0.0, external_pressure - drift_relief)
        if (
            provider_policy_relief_enabled
            and mission_provider_policy_changed
            and mission_provider_policy_updated_count > 0
        ):
            tuning_relief = min(
                0.18,
                provider_policy_relief_gain
                + (min(24, mission_provider_policy_updated_count) * 0.004),
            )
            if mission_trend_worsening and mission_drift_score >= mission_drift_severe_threshold:
                tuning_relief *= 0.55
            external_pressure = max(0.0, external_pressure - tuning_relief)

        remediation_feedback_enabled = bool(
            getattr(self, "runtime_policy_remediation_feedback_enabled", True)
        )
        remediation_hard_floor = self._coerce_float(
            getattr(self, "runtime_policy_remediation_hard_floor", 0.34),
            minimum=0.0,
            maximum=1.0,
            default=0.34,
        )
        remediation_relief_floor = self._coerce_float(
            getattr(self, "runtime_policy_remediation_relief_floor", 0.74),
            minimum=0.0,
            maximum=1.0,
            default=0.74,
        )
        remediation_min_samples = self._coerce_int(
            getattr(self, "runtime_policy_remediation_min_samples", 2),
            minimum=0,
            maximum=100,
            default=2,
        )
        if remediation_feedback_enabled and remediation_attempted >= remediation_min_samples:
            remediation_hard_signal = bool(
                remediation_success_rate <= remediation_hard_floor
                or remediation_checkpoint_blocked_ratio >= 0.42
                or remediation_contract_risk >= 0.68
            )
            remediation_relief_signal = bool(
                remediation_success_rate >= remediation_relief_floor
                and remediation_checkpoint_blocked_ratio <= 0.18
                and remediation_contract_risk <= 0.42
            )
            if remediation_hard_signal:
                remediation_pressure_gain = self._coerce_float(
                    0.12
                    + ((remediation_hard_floor - remediation_success_rate) * 0.28)
                    + (remediation_checkpoint_blocked_ratio * 0.18)
                    + (remediation_contract_risk * 0.08),
                    minimum=0.06,
                    maximum=0.42,
                    default=0.14,
                )
                external_pressure = min(1.0, external_pressure + remediation_pressure_gain)
                contract_pressure = min(
                    1.0,
                    max(
                        contract_pressure,
                        remediation_contract_risk + (remediation_checkpoint_blocked_ratio * 0.24),
                    ),
                )
                if remediation_checkpoint_blocked_ratio >= 0.55:
                    mission_trend_worsening = True
                    trend_pressure = max(trend_pressure, remediation_checkpoint_blocked_ratio)
            elif remediation_relief_signal:
                remediation_relief = self._coerce_float(
                    0.1
                    + ((remediation_success_rate - remediation_relief_floor) * 0.5)
                    + (max(0.0, 0.16 - remediation_checkpoint_blocked_ratio) * 0.25),
                    minimum=0.05,
                    maximum=0.24,
                    default=0.1,
                )
                external_pressure = max(0.0, external_pressure - remediation_relief)
                contract_pressure = max(0.0, contract_pressure - (remediation_relief * 0.62))

        smoothed_signals = self._runtime_policy_smooth_signals(
            metadata=metadata,
            quality_score=quality_score,
            confirm_failure_ratio=confirm_failure_ratio,
            external_pressure=external_pressure,
            desktop_change_rate=desktop_change_rate,
            trend_pressure=trend_pressure,
        )
        quality_score = self._coerce_float(
            smoothed_signals.get("quality_score", quality_score),
            minimum=0.0,
            maximum=1.0,
            default=quality_score,
        )
        confirm_failure_ratio = self._coerce_float(
            smoothed_signals.get("confirm_failure_ratio", confirm_failure_ratio),
            minimum=0.0,
            maximum=1.0,
            default=confirm_failure_ratio,
        )
        external_pressure = self._coerce_float(
            smoothed_signals.get("external_pressure", external_pressure),
            minimum=0.0,
            maximum=1.0,
            default=external_pressure,
        )
        desktop_change_rate = self._coerce_float(
            smoothed_signals.get("desktop_change_rate", desktop_change_rate),
            minimum=0.0,
            maximum=1.0,
            default=desktop_change_rate,
        )
        trend_pressure = self._coerce_float(
            smoothed_signals.get("trend_pressure", trend_pressure),
            minimum=0.0,
            maximum=1.0,
            default=trend_pressure,
        )
        previous_mode = str(smoothed_signals.get("previous_mode", "stable")).strip().lower() or "stable"
        scope_key = str(smoothed_signals.get("scope_key", "")).strip()

        external_threshold = self._coerce_float(
            getattr(self, "runtime_policy_external_pressure_threshold", 0.48),
            minimum=0.1,
            maximum=0.95,
            default=0.48,
        )
        quality_floor = self._coerce_float(
            getattr(self, "runtime_policy_quality_floor", 0.56),
            minimum=0.1,
            maximum=0.95,
            default=0.56,
        )
        trend_weight = self._coerce_float(
            getattr(self, "runtime_policy_trend_weight", 0.35),
            minimum=0.0,
            maximum=1.0,
            default=0.35,
        )
        trend_relief_weight = self._coerce_float(
            getattr(self, "runtime_policy_trend_relief_weight", 0.24),
            minimum=0.0,
            maximum=1.0,
            default=0.24,
        )
        contract_pressure_threshold = self._coerce_float(
            getattr(self, "runtime_policy_contract_pressure_threshold", 0.38),
            minimum=0.05,
            maximum=0.95,
            default=0.38,
        )
        contract_severe_threshold = self._coerce_float(
            getattr(self, "runtime_policy_contract_severe_threshold", 0.62),
            minimum=0.1,
            maximum=1.0,
            default=0.62,
        )
        contract_severe_codes = {
            "auth_preflight_failed",
            "no_provider_candidates_after_contract",
            "provider_not_supported_for_action",
            "provider_outage_blocked",
            "provider_cooldown_blocked",
        }
        contract_severe = bool(contract_pressure >= contract_severe_threshold or contract_code in contract_severe_codes)
        if mission_trend_worsening and trend_pressure > 0.0:
            external_shift = min(0.2, trend_pressure * (0.18 + (0.12 * trend_weight)))
            quality_shift = min(0.16, trend_pressure * (0.14 + (0.1 * trend_weight)))
            external_threshold = max(0.12, external_threshold - external_shift)
            quality_floor = min(0.9, quality_floor + quality_shift)
        elif mission_trend_improving and trend_pressure > 0.0:
            external_relief = min(0.14, trend_pressure * (0.1 + (0.08 * trend_relief_weight)))
            quality_relief = min(0.1, trend_pressure * (0.08 + (0.06 * trend_relief_weight)))
            external_threshold = min(0.95, external_threshold + external_relief)
            quality_floor = max(0.1, quality_floor - quality_relief)

        severe_mode = bool(
            mission_risk_high
            or mission_quality_low
            or quality_score <= quality_floor
            or confirm_failure_ratio >= 0.45
            or external_pressure >= external_threshold
            or contract_severe
            or latest_failure_category in {"non_retryable", "blocked", "unknown"}
            or (mission_trend_worsening and trend_pressure >= 0.58 and attempt >= 1)
        )
        moderate_floor = max(0.62, min(0.82, quality_floor + 0.12))
        moderate_mode = bool(
            quality_score <= moderate_floor
            or confirm_failure_ratio >= 0.24
            or external_pressure >= (external_threshold * 0.72)
            or contract_pressure >= contract_pressure_threshold
            or latest_failure_category in {"timeout", "rate_limited", "transient"}
            or desktop_change_rate <= 0.2
            or (mission_trend_worsening and trend_pressure >= 0.32)
        )

        hysteresis_external_margin = self._coerce_float(
            getattr(self, "runtime_policy_hysteresis_external_margin", 0.05),
            minimum=0.0,
            maximum=0.4,
            default=0.05,
        )
        hysteresis_quality_margin = self._coerce_float(
            getattr(self, "runtime_policy_hysteresis_quality_margin", 0.04),
            minimum=0.0,
            maximum=0.4,
            default=0.04,
        )
        hysteresis_confirm_margin = self._coerce_float(
            getattr(self, "runtime_policy_hysteresis_confirm_margin", 0.05),
            minimum=0.0,
            maximum=0.4,
            default=0.05,
        )
        hysteresis_trend_margin = self._coerce_float(
            getattr(self, "runtime_policy_hysteresis_trend_margin", 0.06),
            minimum=0.0,
            maximum=0.4,
            default=0.06,
        )

        severe_relief = bool(
            quality_score >= min(0.99, quality_floor + hysteresis_quality_margin)
            and confirm_failure_ratio <= max(0.0, 0.24 - hysteresis_confirm_margin)
            and external_pressure <= max(0.0, external_threshold - hysteresis_external_margin)
            and (
                not mission_trend_worsening
                or trend_pressure <= max(0.0, 0.32 - hysteresis_trend_margin)
            )
        )
        if previous_mode == "severe" and not severe_mode and not severe_relief:
            severe_mode = True

        moderate_relief = bool(
            quality_score >= min(0.99, moderate_floor + (hysteresis_quality_margin * 0.8))
            and confirm_failure_ratio <= max(0.0, 0.18 - (hysteresis_confirm_margin * 0.6))
            and external_pressure <= max(0.0, (external_threshold * 0.72) - (hysteresis_external_margin * 0.7))
            and desktop_change_rate >= min(1.0, 0.24 + (hysteresis_quality_margin * 0.5))
            and (
                not mission_trend_worsening
                or trend_pressure <= max(0.0, 0.26 - (hysteresis_trend_margin * 0.8))
            )
        )
        if severe_mode:
            moderate_mode = True
        else:
            if previous_mode == "severe":
                moderate_mode = True
            elif previous_mode == "moderate" and not moderate_mode and not moderate_relief:
                moderate_mode = True

        runtime_mode = "severe" if severe_mode else ("moderate" if moderate_mode else "stable")
        if scope_key:
            self._runtime_policy_set_mode(scope_key=scope_key, mode=runtime_mode)
        effective_verification_pressure = self._coerce_float(
            max(verification_pressure, confirm_failure_ratio, contract_pressure * 0.82),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        updates["runtime_verification_pressure"] = f"{effective_verification_pressure:.6f}"
        updates["runtime_verification_mode"] = (
            "severe"
            if effective_verification_pressure >= verification_pressure_severe_threshold
            else ("moderate" if effective_verification_pressure >= verification_pressure_threshold else "stable")
        )

        if severe_mode:
            next_strictness = self._next_verification_strictness(current_strictness)
            if next_strictness != current_strictness:
                updates["verification_strictness"] = next_strictness
            if current_recovery != "safe":
                updates["recovery_profile"] = "safe"
            if current_policy == "automation_power":
                updates["policy_profile"] = "automation_safe"
            elif current_policy == "interactive" and attempt >= 1:
                updates["policy_profile"] = "automation_safe"
        elif moderate_mode:
            if current_strictness == "off":
                updates["verification_strictness"] = "standard"
            if current_recovery == "aggressive":
                updates["recovery_profile"] = "balanced"
            if current_policy == "automation_power" and (
                external_pressure > 0.34 or (mission_trend_worsening and trend_pressure >= 0.28)
            ):
                updates["policy_profile"] = "automation_safe"
        else:
            allow_upgrade = bool(getattr(self, "runtime_policy_auto_upgrade", False)) or self._coerce_bool(
                metadata.get("runtime_policy_allow_upgrade", False),
                default=False,
            )
            if allow_upgrade and attempt == 0:
                healthy = bool(
                    quality_score >= 0.84
                    and confirm_failure_ratio <= 0.12
                    and external_pressure <= 0.08
                    and contract_pressure <= 0.08
                    and trend_pressure <= 0.24
                    and not mission_risk_high
                    and not mission_quality_low
                )
                if healthy and current_policy in {"interactive", "automation_safe"}:
                    updates["policy_profile"] = "automation_power"
                    if current_recovery == "safe":
                        updates["recovery_profile"] = "balanced"
                    if current_strictness == "strict":
                        updates["verification_strictness"] = "standard"

        route_updates = self._derive_runtime_external_route_overrides(
            metadata=metadata,
            runtime_mode=runtime_mode,
            attempt=attempt,
            quality_score=quality_score,
            external_pressure=external_pressure,
            contract_pressure=contract_pressure,
            mission_trend_worsening=mission_trend_worsening,
            mission_trend_improving=mission_trend_improving,
        )
        if route_updates:
            updates.update(route_updates)

        execution_updates = self._derive_runtime_external_remediation_execution_overrides(
            metadata=metadata,
            runtime_mode=runtime_mode,
            contract_signal=contract_signal if isinstance(contract_signal, dict) else {},
            contract_pressure=contract_pressure,
            external_pressure=external_pressure,
        )
        if execution_updates:
            updates.update(execution_updates)

        return updates

    def _derive_replan_overrides(
        self,
        *,
        metadata: Dict[str, object],
        context: Dict[str, object],
        policy: Dict[str, Any],
        replan_attempt: int,
    ) -> Dict[str, str]:
        if replan_attempt <= 0:
            return {}

        updates: Dict[str, str] = {}
        failure_category = str(context.get("last_failure_category", "")).strip().lower() or "unknown"
        retry_count = self._coerce_int(
            context.get("last_failure_retry_count", 0),
            minimum=0,
            maximum=1000,
            default=0,
        )
        external_contract_raw = context.get("last_failure_external_contract", {})
        external_contract = external_contract_raw if isinstance(external_contract_raw, dict) else {}
        contract_code = str(external_contract.get("code", "")).strip().lower()
        contract_pressure = self._coerce_float(
            external_contract.get("pressure", self._contract_pressure_from_code(contract_code)),
            minimum=0.0,
            maximum=1.0,
            default=self._contract_pressure_from_code(contract_code),
        )
        contract_severe = bool(
            contract_pressure >= self._coerce_float(
                getattr(self, "runtime_policy_contract_severe_threshold", 0.62),
                minimum=0.1,
                maximum=1.0,
                default=0.62,
            )
            or contract_code
            in {
                "auth_preflight_failed",
                "no_provider_candidates_after_contract",
                "provider_not_supported_for_action",
                "provider_outage_blocked",
                "provider_cooldown_blocked",
            }
        )

        if bool(policy.get("escalate_verification", True)):
            current_strictness = str(metadata.get("verification_strictness", "")).strip().lower() or "standard"
            next_strictness = current_strictness
            if failure_category in {"non_retryable", "unknown", "blocked"} or retry_count >= 2:
                next_strictness = self._next_verification_strictness(current_strictness)
            elif failure_category in {"timeout", "rate_limited"} and current_strictness == "off":
                next_strictness = "standard"
            if contract_pressure >= 0.34:
                next_strictness = self._next_verification_strictness(next_strictness)
            if next_strictness != current_strictness:
                updates["verification_strictness"] = next_strictness

        if bool(policy.get("escalate_recovery_profile", True)):
            current_policy = str(metadata.get("policy_profile", "")).strip().lower()
            current_recovery = str(metadata.get("recovery_profile", "")).strip().lower()
            if not current_recovery:
                current_recovery = self._default_recovery_profile(current_policy)
            target_recovery = current_recovery

            if failure_category in {"non_retryable", "unknown", "blocked"}:
                target_recovery = "safe"
            elif failure_category in {"timeout", "rate_limited"}:
                if current_recovery == "aggressive":
                    target_recovery = "balanced"
                elif current_recovery == "balanced" and replan_attempt >= 2:
                    target_recovery = "safe"
            elif failure_category == "transient" and retry_count >= 3 and current_recovery == "aggressive":
                target_recovery = "balanced"
            if contract_severe:
                target_recovery = "safe"
            elif contract_pressure >= 0.42 and current_recovery == "aggressive":
                target_recovery = "balanced"

            if target_recovery and target_recovery != str(metadata.get("recovery_profile", "")).strip().lower():
                updates["recovery_profile"] = target_recovery

        if bool(policy.get("escalate_policy_profile", True)):
            current_policy = str(metadata.get("policy_profile", "")).strip().lower()
            if current_policy == "automation_power" and failure_category in {"non_retryable", "unknown", "blocked"}:
                updates["policy_profile"] = "automation_safe"
            if current_policy in {"automation_power", "interactive"} and (contract_severe or contract_pressure >= 0.5):
                updates["policy_profile"] = "automation_safe"
            external_signal = context.get("last_failure_external_reliability", {})
            if isinstance(external_signal, dict):
                selected_health = self._coerce_float(
                    external_signal.get("selected_health_score", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                selected_effective = self._coerce_float(
                    external_signal.get("selected_effective_score", selected_health),
                    minimum=0.0,
                    maximum=1.0,
                    default=selected_health,
                )
                blocked_provider_count = self._coerce_int(
                    external_signal.get("blocked_provider_count", 0),
                    minimum=0,
                    maximum=1000,
                    default=0,
                )
                dropped_provider_count = self._coerce_int(
                    external_signal.get("dropped_provider_count", 0),
                    minimum=0,
                    maximum=1000,
                    default=0,
                )
                route_strategy = str(external_signal.get("strategy", "")).strip().lower()
                if (
                    current_policy in {"automation_power", "interactive"}
                    and failure_category in {"timeout", "rate_limited", "transient", "unknown"}
                    and (
                        selected_health <= 0.42
                        or selected_effective <= 0.38
                        or blocked_provider_count > 0
                        or dropped_provider_count > 0
                        or route_strategy in {"fallback_ranked", "override_cooldown"}
                    )
                ):
                    updates["policy_profile"] = "automation_safe"
                if contract_severe:
                    updates["external_route_profile"] = "defensive"
                    updates["external_route_entropy_force"] = "false"
                    updates["external_route_entropy_probe"] = "0.920000"
                    updates["external_cooldown_override"] = "false"
                    updates["external_outage_override"] = "false"
                    updates["external_remediation_max_actions"] = "5"
                    updates["external_remediation_max_total_actions"] = "12"
                    updates["external_remediation_contract_risk_floor"] = "0.520000"
                elif failure_category in {"timeout", "rate_limited", "transient", "unknown"} and (
                    selected_health <= 0.48
                    or selected_effective <= 0.44
                    or blocked_provider_count > 0
                    or dropped_provider_count > 0
                    or route_strategy in {"fallback_ranked", "override_cooldown"}
                ):
                    updates.setdefault("external_route_profile", "cautious")
                    updates.setdefault("external_route_entropy_force", "false")
                    updates.setdefault("external_route_entropy_probe", "0.640000")
                    updates.setdefault("external_cooldown_override", "false")
                    updates.setdefault("external_outage_override", "false")
                    updates.setdefault("external_remediation_contract_risk_floor", "0.300000")

        execution_contract_raw = external_contract.get("execution_contract", {})
        execution_contract = execution_contract_raw if isinstance(execution_contract_raw, dict) else {}
        execution_mode = str(
            execution_contract.get("mode", external_contract.get("automation_tier", ""))
        ).strip().lower()
        execution_max_retry_attempts = self._coerce_int(
            execution_contract.get("max_retry_attempts", 0),
            minimum=0,
            maximum=20,
            default=0,
        )
        execution_allow_provider_reroute = self._coerce_bool(
            execution_contract.get("allow_provider_reroute", True),
            default=True,
        )
        execution_stop_conditions = [
            str(item).strip().lower()
            for item in (
                execution_contract.get("stop_conditions", [])
                if isinstance(execution_contract.get("stop_conditions", []), list)
                else []
            )
            if str(item).strip()
        ]
        if contract_severe or execution_mode in {"manual", "assisted"} or execution_max_retry_attempts <= 1:
            updates["external_remediation_execution_mode"] = "assisted"
            updates["external_remediation_checkpoint_mode"] = "strict"
            updates["external_remediation_allow_provider_reroute"] = (
                "true"
                if execution_allow_provider_reroute and "provider_reroute_locked" not in execution_stop_conditions
                else "false"
            )
        elif contract_pressure >= 0.42:
            updates.setdefault("external_remediation_execution_mode", "assisted")
            updates.setdefault("external_remediation_checkpoint_mode", "standard")
            updates.setdefault(
                "external_remediation_allow_provider_reroute",
                "true" if execution_allow_provider_reroute else "false",
            )

        anchor_signal = context.get("last_failure_desktop_anchor", {})
        desktop_signal = context.get("last_failure_desktop_state", {})
        anchor_confidence = 1.0
        fallback_used = False
        if isinstance(anchor_signal, dict):
            anchor_confidence = self._coerce_float(
                anchor_signal.get("confidence", 1.0),
                minimum=0.0,
                maximum=1.0,
                default=1.0,
            )
            fallback_used = bool(anchor_signal.get("fallback_used", False))
        window_transition = bool(desktop_signal.get("window_transition", False)) if isinstance(desktop_signal, dict) else False
        if window_transition or fallback_used or anchor_confidence < 0.45:
            current_strictness = str(metadata.get("verification_strictness", "")).strip().lower() or "standard"
            next_strictness = self._next_verification_strictness(current_strictness)
            if next_strictness != current_strictness:
                updates["verification_strictness"] = next_strictness
            current_recovery = str(metadata.get("recovery_profile", "")).strip().lower()
            if current_recovery == "aggressive":
                updates["recovery_profile"] = "balanced"
            current_policy = str(metadata.get("policy_profile", "")).strip().lower()
            if current_policy == "automation_power":
                updates["policy_profile"] = "automation_safe"

        return updates

    @staticmethod
    def _next_verification_strictness(current: str) -> str:
        clean = str(current or "").strip().lower()
        if clean == "off":
            return "standard"
        if clean == "standard":
            return "strict"
        return "strict"

    def _apply_guardrail_runtime_guidance(
        self,
        *,
        goal: GoalRecord,
        plan: ExecutionPlan,
        context: Dict[str, object],
        guidance: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not isinstance(guidance, dict):
            return {}
        status = str(guidance.get("status", "")).strip().lower()
        if status in {"disabled", "error"}:
            return {}

        metadata_overrides = guidance.get("metadata_overrides")
        action_overrides = guidance.get("action_overrides")
        triggered_actions = guidance.get("triggered_actions")
        recommended_level = str(guidance.get("recommended_level", "")).strip().lower()
        if not isinstance(metadata_overrides, dict):
            metadata_overrides = {}
        if not isinstance(action_overrides, dict):
            action_overrides = {}
        if not isinstance(triggered_actions, list):
            triggered_actions = []

        applied_metadata: Dict[str, str] = {}
        goal_metadata = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}

        strictness_rank = {"off": 0, "standard": 1, "strict": 2}
        recovery_safety_rank = {"aggressive": 0, "balanced": 1, "safe": 2}
        policy_safety_rank = {"automation_power": 0, "interactive": 1, "automation_safe": 2}

        for key in ("verification_strictness", "recovery_profile", "policy_profile"):
            raw_value = metadata_overrides.get(key)
            value = str(raw_value or "").strip().lower()
            if not value:
                continue
            current = str(goal_metadata.get(key, "")).strip().lower()
            should_apply = False
            if key == "verification_strictness":
                if strictness_rank.get(value, -1) > strictness_rank.get(current, -1):
                    should_apply = True
            elif key == "recovery_profile":
                if recovery_safety_rank.get(value, -1) > recovery_safety_rank.get(current, -1):
                    should_apply = True
            elif key == "policy_profile":
                if policy_safety_rank.get(value, -1) > policy_safety_rank.get(current, -1):
                    should_apply = True

            if should_apply:
                goal_metadata[key] = value
                context[key] = value
                applied_metadata[key] = value

        if applied_metadata:
            goal.request.metadata = goal_metadata

        step_override_count = 0
        step_retry_tuned = 0
        for step in plan.steps:
            if not isinstance(step, PlanStep):
                continue
            override = action_overrides.get(step.action)
            if not isinstance(override, dict):
                continue

            max_retries_cap = self._coerce_int(
                override.get("max_retries_cap", step.max_retries),
                minimum=0,
                maximum=10,
                default=step.max_retries,
            )
            if step.max_retries > max_retries_cap:
                step.max_retries = max_retries_cap
                step_override_count += 1

            timeout_factor = self._coerce_float(
                override.get("timeout_factor", 1.0),
                minimum=1.0,
                maximum=3.0,
                default=1.0,
            )
            if timeout_factor > 1.0:
                new_timeout_s = self._coerce_int(
                    round(float(step.timeout_s) * timeout_factor),
                    minimum=1,
                    maximum=300,
                    default=step.timeout_s,
                )
                if new_timeout_s > step.timeout_s:
                    step.timeout_s = new_timeout_s
                    step_override_count += 1

            retry_multiplier = self._coerce_float(
                override.get("retry_multiplier", 1.0),
                minimum=1.0,
                maximum=4.0,
                default=1.0,
            )
            if retry_multiplier > 1.0:
                verify_payload = step.verify if isinstance(step.verify, dict) else {}
                retry_payload = verify_payload.get("retry")
                retry_config = retry_payload if isinstance(retry_payload, dict) else {}
                existing_base_delay = self._coerce_float(
                    retry_config.get("base_delay_s", 0.4),
                    minimum=0.0,
                    maximum=30.0,
                    default=0.4,
                )
                tuned_base_delay = self._coerce_float(
                    existing_base_delay * retry_multiplier,
                    minimum=0.0,
                    maximum=30.0,
                    default=existing_base_delay,
                )
                if tuned_base_delay > existing_base_delay:
                    retry_config["base_delay_s"] = round(tuned_base_delay, 3)
                    existing_max_delay = self._coerce_float(
                        retry_config.get("max_delay_s", max(1.2, existing_base_delay * 3.0)),
                        minimum=tuned_base_delay,
                        maximum=60.0,
                        default=max(1.2, tuned_base_delay * 3.0),
                    )
                    retry_config["max_delay_s"] = round(max(existing_max_delay, tuned_base_delay), 3)
                    verify_payload["retry"] = retry_config
                    step.verify = verify_payload
                    step_retry_tuned += 1

        if recommended_level:
            context["guardrail_recommended_level"] = recommended_level
        if triggered_actions:
            context["guardrail_triggered_actions"] = triggered_actions[:12]

        if not applied_metadata and step_override_count <= 0 and step_retry_tuned <= 0:
            return {}

        return {
            "recommended_level": recommended_level or "unknown",
            "metadata_overrides": dict(applied_metadata),
            "step_override_count": int(step_override_count),
            "step_retry_tuned": int(step_retry_tuned),
            "triggered_actions": [str(item.get("action", "")) for item in triggered_actions[:12] if isinstance(item, dict)],
        }

    @classmethod
    def _extract_replan_failure_context(cls, failed: ActionResult) -> Dict[str, object]:
        evidence = failed.evidence if isinstance(failed.evidence, dict) else {}
        raw_recovery = evidence.get("recovery")
        recovery = raw_recovery if isinstance(raw_recovery, dict) else {}

        raw_history = recovery.get("retry_history")
        retry_history: list[Dict[str, object]] = []
        if isinstance(raw_history, list):
            for entry in raw_history[:8]:
                if not isinstance(entry, dict):
                    continue
                try:
                    delay_s = float(entry.get("delay_s", 0.0))
                    delay_s = max(0.0, min(delay_s, 60.0))
                except Exception:  # noqa: BLE001
                    delay_s = 0.0
                retry_history.append(
                    {
                        "attempt": cls._coerce_int(entry.get("attempt", 1), minimum=1, maximum=1000, default=1),
                        "delay_s": delay_s,
                        "category": str(entry.get("category", "")).strip().lower(),
                        "reason": str(entry.get("reason", "")).strip(),
                    }
                )

        last_category = str(recovery.get("last_category", "")).strip().lower()
        if not last_category and retry_history:
            last_category = str(retry_history[-1].get("category", "")).strip().lower()
        if not last_category:
            last_category = cls._classify_failure_category(failed.error or "")
        if not last_category:
            last_category = "unknown"

        attempt = cls._coerce_int(
            recovery.get("attempt", failed.attempt),
            minimum=1,
            maximum=1000,
            default=max(1, int(failed.attempt)),
        )
        retry_count = cls._coerce_int(
            recovery.get("retry_count", len(retry_history)),
            minimum=0,
            maximum=1000,
            default=len(retry_history),
        )

        confirm_policy_raw = evidence.get("confirm_policy")
        confirm_policy = confirm_policy_raw if isinstance(confirm_policy_raw, dict) else {}
        confirm_actions_raw = evidence.get("confirm_actions")
        confirm_actions = confirm_actions_raw if isinstance(confirm_actions_raw, list) else []
        confirm_total = 0
        confirm_success = 0
        if confirm_actions:
            for row in confirm_actions:
                if not isinstance(row, dict):
                    continue
                confirm_total += 1
                if str(row.get("status", "")).strip().lower() == "success":
                    confirm_success += 1
        elif confirm_policy:
            confirm_total = cls._coerce_int(confirm_policy.get("total_count", 0), minimum=0, maximum=1000, default=0)
            confirm_success = cls._coerce_int(confirm_policy.get("success_count", 0), minimum=0, maximum=1000, default=0)

        desktop_state_raw = evidence.get("desktop_state")
        desktop_state = desktop_state_raw if isinstance(desktop_state_raw, dict) else {}
        desktop_change_count = cls._coerce_int(
            desktop_state.get("change_count", 0),
            minimum=0,
            maximum=100000,
            default=0,
        )
        desktop_state_changed = bool(desktop_state.get("state_changed", False))
        desktop_window_transition = bool(desktop_state.get("window_transition", False))
        desktop_app_transition = bool(desktop_state.get("app_transition", False))

        external_raw = evidence.get("external_reliability_preflight")
        if not isinstance(external_raw, dict):
            external_raw = evidence.get("external_reliability")
        external = external_raw if isinstance(external_raw, dict) else {}
        preflight_status = str(external.get("status", "")).strip().lower()
        provider_routing = external.get("provider_routing")
        provider_route = provider_routing if isinstance(provider_routing, dict) else {}
        blocked_providers = provider_route.get("blocked_providers")
        blocked_provider_count = len(blocked_providers) if isinstance(blocked_providers, list) else 0
        selected_health = cls._coerce_float(
            provider_route.get("selected_health_score", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        selected_effective = cls._coerce_float(
            provider_route.get("selected_effective_score", selected_health),
            minimum=0.0,
            maximum=1.0,
            default=selected_health,
        )
        route_strategy = str(provider_route.get("strategy", "")).strip().lower()
        selected_provider = str(provider_route.get("selected_provider", "")).strip().lower()
        operation_class = str(provider_route.get("operation_class", "")).strip().lower()
        contract_negotiation_raw = external.get("contract_negotiation")
        contract_negotiation = contract_negotiation_raw if isinstance(contract_negotiation_raw, dict) else {}
        dropped_providers = contract_negotiation.get("dropped_providers")
        dropped_provider_count = len(dropped_providers) if isinstance(dropped_providers, list) else 0
        retry_hint = external.get("retry_hint")
        retry_hint_payload = retry_hint if isinstance(retry_hint, dict) else {}
        contract_diagnostic_raw = external.get("contract_diagnostic")
        contract_diagnostic = contract_diagnostic_raw if isinstance(contract_diagnostic_raw, dict) else {}
        remediation_contract_raw = contract_diagnostic.get("remediation_contract")
        if not isinstance(remediation_contract_raw, dict):
            remediation_contract_raw = external.get("remediation_contract", {})
        remediation_contract = remediation_contract_raw if isinstance(remediation_contract_raw, dict) else {}
        execution_contract_raw = remediation_contract.get("execution_contract", {})
        execution_contract = execution_contract_raw if isinstance(execution_contract_raw, dict) else {}
        execution_verification_raw = execution_contract.get("verification", {})
        execution_verification = execution_verification_raw if isinstance(execution_verification_raw, dict) else {}
        auth_preflight_raw = external.get("auth_preflight")
        auth_preflight = auth_preflight_raw if isinstance(auth_preflight_raw, dict) else {}
        remediation_raw = external.get("remediation_hints")
        remediation_hints = remediation_raw if isinstance(remediation_raw, list) else []
        remediation_plan_raw = (
            contract_diagnostic.get("remediation_plan")
            if isinstance(contract_diagnostic.get("remediation_plan"), list)
            else external.get("remediation_plan", [])
        )
        remediation_plan = (
            [cls._sanitize_replan_args(row) for row in remediation_plan_raw if isinstance(row, dict)]
            if isinstance(remediation_plan_raw, list)
            else []
        )
        contract_checks_raw = contract_diagnostic.get("checks", [])
        contract_checks: list[Dict[str, Any]] = []
        if isinstance(contract_checks_raw, list):
            for row in contract_checks_raw[:12]:
                if not isinstance(row, dict):
                    continue
                contract_checks.append(
                    {
                        "check": str(row.get("check", "")).strip().lower(),
                        "status": str(row.get("status", "")).strip().lower(),
                        "severity": str(row.get("severity", "")).strip().lower(),
                        "details": cls._sanitize_replan_args(row.get("details", {})),
                    }
                )
        contract_runtime_diag_raw = contract_diagnostic.get("diagnostics", {})
        contract_runtime_diag = contract_runtime_diag_raw if isinstance(contract_runtime_diag_raw, dict) else {}
        blocked_ratio = cls._coerce_float(
            contract_runtime_diag.get("blocked_ratio", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        severity_score = cls._coerce_float(
            contract_diagnostic.get("severity_score", external.get("severity_score", 0.0)),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        blocking_class = str(
            contract_diagnostic.get(
                "blocking_class",
                external.get("blocking_class", remediation_contract.get("blocking_class", "")),
            )
        ).strip().lower()
        estimated_recovery_s = cls._coerce_int(
            contract_diagnostic.get(
                "estimated_recovery_s",
                external.get("estimated_recovery_s", remediation_contract.get("estimated_recovery_s", 0)),
            ),
            minimum=0,
            maximum=86_400,
            default=0,
        )
        automation_tier = str(
            contract_diagnostic.get(
                "automation_tier",
                external.get("automation_tier", remediation_contract.get("automation_tier", "")),
            )
        ).strip().lower()
        execution_mode = str(execution_contract.get("mode", automation_tier)).strip().lower()
        execution_max_retry_attempts = cls._coerce_int(
            execution_contract.get("max_retry_attempts", 0),
            minimum=0,
            maximum=20,
            default=0,
        )
        execution_allow_provider_reroute = bool(execution_verification.get("allow_provider_reroute", True))
        execution_stop_conditions_raw = execution_contract.get("stop_conditions", [])
        execution_stop_conditions = (
            [str(item).strip().lower() for item in execution_stop_conditions_raw if str(item).strip()]
            if isinstance(execution_stop_conditions_raw, list)
            else []
        )
        retry_after_s = cls._coerce_float(
            contract_diagnostic.get("retry_after_s", external.get("retry_after_s", 0.0)),
            minimum=0.0,
            maximum=86_400.0,
            default=0.0,
        )
        cooldown_rows_raw = contract_diagnostic.get("cooldown_providers", [])
        cooldown_rows = [dict(row) for row in cooldown_rows_raw if isinstance(row, dict)] if isinstance(cooldown_rows_raw, list) else []
        outage_rows_raw = contract_diagnostic.get("outage_providers", [])
        outage_rows = [dict(row) for row in outage_rows_raw if isinstance(row, dict)] if isinstance(outage_rows_raw, list) else []
        blocked_providers_raw = contract_diagnostic.get("blocked_providers", [])
        blocked_providers = (
            [str(item).strip().lower() for item in blocked_providers_raw if str(item).strip()]
            if isinstance(blocked_providers_raw, list)
            else []
        )
        requested_provider = str(contract_diagnostic.get("requested_provider", "")).strip().lower()
        allowed_providers_raw = contract_diagnostic.get("allowed_providers", [])
        allowed_providers = (
            [str(item).strip().lower() for item in allowed_providers_raw if str(item).strip()]
            if isinstance(allowed_providers_raw, list)
            else []
        )
        missing_fields_raw = contract_diagnostic.get("fields", [])
        missing_fields = (
            [str(item).strip() for item in missing_fields_raw if str(item).strip()]
            if isinstance(missing_fields_raw, list)
            else []
        )
        any_of_raw = contract_diagnostic.get("any_of", [])
        any_of_groups: list[list[str]] = []
        if isinstance(any_of_raw, list):
            for row in any_of_raw[:8]:
                if not isinstance(row, list):
                    continue
                normalized = [str(item).strip() for item in row if str(item).strip()]
                if normalized:
                    any_of_groups.append(normalized[:8])
        auth_rows_raw = auth_preflight.get("auth_rows", [])
        auth_rows = [dict(row) for row in auth_rows_raw if isinstance(row, dict)] if isinstance(auth_rows_raw, list) else []
        auth_blocked_providers: list[str] = []
        for row in auth_rows[:16]:
            status = str(row.get("status", "")).strip().lower()
            provider = str(row.get("provider", "")).strip().lower()
            if status not in {"blocked", "degraded"} or not provider:
                continue
            if provider not in auth_blocked_providers:
                auth_blocked_providers.append(provider)
        remediation_rows: list[Dict[str, Any]] = []
        for row in remediation_hints[:12]:
            if isinstance(row, dict):
                remediation_rows.append(cls._sanitize_replan_args(row))
        request_raw = evidence.get("request")
        request_payload = request_raw if isinstance(request_raw, dict) else {}
        request_args = cls._sanitize_replan_args(request_payload.get("args", {}))

        anchor_raw = evidence.get("desktop_anchor")
        anchor = anchor_raw if isinstance(anchor_raw, dict) else {}
        anchor_chain = anchor.get("chain")
        anchor_chain_list = anchor_chain if isinstance(anchor_chain, list) else []
        anchor_fallback_used = bool(anchor.get("fallback_used", False))
        anchor_confidence = cls._coerce_float(
            anchor.get("confidence", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        anchor_required = bool(anchor.get("required", False))
        anchor_action = str(anchor.get("action", "")).strip().lower()

        return {
            "last_failure_action": failed.action,
            "last_failure_error": failed.error or "Execution failed.",
            "last_failure_category": last_category,
            "last_failure_attempt": attempt,
            "last_failure_retry_count": retry_count,
            "last_failure_recovery": {
                "retry_count": retry_count,
                "last_category": last_category,
                "retry_history": retry_history,
            },
            "last_failure_confirm_policy": {
                "mode": str(confirm_policy.get("mode", "")).strip().lower(),
                "required": bool(confirm_policy.get("required", True)),
                "satisfied": bool(confirm_policy.get("satisfied", False)),
                "success_count": confirm_success,
                "total_count": confirm_total,
            },
            "last_failure_desktop_state": {
                "state_changed": desktop_state_changed,
                "change_count": desktop_change_count,
                "state_hash": str(desktop_state.get("state_hash", "")).strip(),
                "window_transition": desktop_window_transition,
                "app_transition": desktop_app_transition,
            },
            "last_failure_external_reliability": {
                "preflight_status": preflight_status,
                "strategy": route_strategy,
                "selected_provider": selected_provider,
                "selected_health_score": selected_health,
                "selected_effective_score": selected_effective,
                "blocked_provider_count": blocked_provider_count,
                "dropped_provider_count": dropped_provider_count,
                "operation_class": operation_class,
                "retry_hint": retry_hint_payload,
                "retry_after_s": round(retry_after_s, 3),
                "contract_code": str(contract_diagnostic.get("code", "")).strip().lower(),
                "blocked_ratio": round(blocked_ratio, 6),
                "runtime_blocked_providers": blocked_providers[:12],
                "cooldown_provider_count": len(cooldown_rows),
                "outage_provider_count": len(outage_rows),
                "severity_score": round(severity_score, 6),
                "blocking_class": blocking_class,
                "estimated_recovery_s": int(estimated_recovery_s),
                "automation_tier": automation_tier,
                "execution_mode": execution_mode,
                "execution_max_retry_attempts": int(execution_max_retry_attempts),
                "execution_stop_conditions": execution_stop_conditions[:12],
                "allow_provider_reroute": bool(execution_allow_provider_reroute),
            },
            "last_failure_external_contract": {
                "preflight_status": preflight_status,
                "code": str(contract_diagnostic.get("code", "")).strip().lower(),
                "severity": str(contract_diagnostic.get("severity", "")).strip().lower(),
                "message": str(contract_diagnostic.get("message", external.get("message", ""))).strip(),
                "severity_score": round(severity_score, 6),
                "blocking_class": blocking_class,
                "estimated_recovery_s": int(estimated_recovery_s),
                "automation_tier": automation_tier,
                "missing_fields": missing_fields[:20],
                "any_of": any_of_groups[:8],
                "requested_provider": requested_provider,
                "allowed_providers": allowed_providers[:8],
                "auth_blocked_providers": auth_blocked_providers[:8],
                "blocked_providers": blocked_providers[:12],
                "retry_after_s": round(retry_after_s, 3),
                "blocked_ratio": round(blocked_ratio, 6),
                "checks": contract_checks,
                "diagnostics": cls._sanitize_replan_args(contract_runtime_diag),
                "remediation_plan": remediation_plan[:8],
                "auth_required_min_ttl_s": cls._coerce_int(
                    auth_preflight.get("required_min_ttl_s", 0),
                    minimum=0,
                    maximum=86400,
                    default=0,
                ),
                "execution_contract": {
                    "mode": execution_mode,
                    "max_retry_attempts": int(execution_max_retry_attempts),
                    "allow_provider_reroute": bool(execution_allow_provider_reroute),
                    "stop_conditions": execution_stop_conditions[:12],
                },
                "remediation_hints": remediation_rows,
            },
            "last_failure_request": {
                "source": str(request_payload.get("source", "")).strip().lower(),
                "args": request_args,
            },
            "last_failure_desktop_anchor": {
                "action": anchor_action,
                "confidence": anchor_confidence,
                "required": anchor_required,
                "fallback_used": anchor_fallback_used,
                "chain_length": len(anchor_chain_list),
            },
        }

    @staticmethod
    def _classify_failure_category(message: str) -> str:
        lowered = str(message or "").strip().lower()
        if not lowered:
            return ""
        if "rate limit" in lowered or "429" in lowered:
            return "rate_limited"
        if "timeout" in lowered or "timed out" in lowered:
            return "timeout"
        if any(
            token in lowered
            for token in (
                "approval required",
                "requires explicit user approval",
                "missing required",
                "invalid",
                "not allowed",
                "explicitly denied",
                "non-retryable",
            )
        ):
            return "non_retryable"
        if any(
            token in lowered
            for token in (
                "temporar",
                "unavailable",
                "connection",
                "reset by peer",
                "resource exhausted",
                "service busy",
                "try again",
            )
        ):
            return "transient"
        return "unknown"

    @classmethod
    def _sanitize_replan_args(cls, value: Any, *, depth: int = 0) -> Any:
        if depth >= 4:
            if isinstance(value, (dict, list, tuple)):
                return ""
            if value is None:
                return None
            return str(value)[:240]

        if isinstance(value, dict):
            out: Dict[str, Any] = {}
            for index, (key, item) in enumerate(value.items()):
                if index >= 40:
                    break
                clean_key = str(key).strip()[:120]
                if not clean_key:
                    continue
                out[clean_key] = cls._sanitize_replan_args(item, depth=depth + 1)
            return out

        if isinstance(value, (list, tuple)):
            return [cls._sanitize_replan_args(item, depth=depth + 1) for item in list(value)[:24]]

        if isinstance(value, (str, int, float, bool)) or value is None:
            if isinstance(value, str):
                return value[:2000]
            return value

        return str(value)[:500]

    @staticmethod
    def _merge_memory_hints(
        lexical_hints: list[Dict[str, Any]],
        episodic_hints: list[Dict[str, Any]],
        *,
        limit: int = 10,
    ) -> list[Dict[str, Any]]:
        bounded = max(1, min(int(limit), 50))
        merged: list[Dict[str, Any]] = []
        seen: set[str] = set()

        def _push_rows(rows: list[Dict[str, Any]], source_name: str) -> None:
            for item in rows:
                if len(merged) >= bounded:
                    break
                if not isinstance(item, dict):
                    continue
                memory_id = str(item.get("memory_id", "")).strip()
                key = memory_id or f"{item.get('text', '')}|{item.get('status', '')}|{item.get('source', source_name)}"
                if key in seen:
                    continue
                seen.add(key)
                row = dict(item)
                row["memory_source"] = str(item.get("memory_source", source_name))
                merged.append(row)

        _push_rows(episodic_hints, "episodic_semantic")
        if len(merged) < bounded:
            _push_rows(lexical_hints, "runtime_lexical")
        return merged[:bounded]

    @staticmethod
    def _extract_resume_plan(metadata: Optional[Dict[str, object]]) -> Optional[Dict[str, Any]]:
        payload = metadata if isinstance(metadata, dict) else {}
        raw_plan = payload.get("resume_plan")
        if not isinstance(raw_plan, dict):
            return None
        steps = raw_plan.get("steps")
        if not isinstance(steps, list) or not steps:
            return None
        return dict(raw_plan)

    @classmethod
    def _deserialize_execution_plan(cls, *, goal_id: str, raw_plan: Dict[str, Any]) -> Optional[ExecutionPlan]:
        if not isinstance(raw_plan, dict):
            return None
        raw_steps = raw_plan.get("steps")
        if not isinstance(raw_steps, list) or not raw_steps:
            return None

        plan_steps: list[PlanStep] = []
        seen_step_ids: set[str] = set()
        for index, item in enumerate(raw_steps, start=1):
            step = cls._coerce_plan_step(item, index=index)
            if step is None:
                continue
            if step.step_id in seen_step_ids:
                step.step_id = f"{step.step_id}-{index}"
            seen_step_ids.add(step.step_id)
            plan_steps.append(step)

        if not plan_steps:
            return None

        plan_context = raw_plan.get("context")
        context = dict(plan_context) if isinstance(plan_context, dict) else {}
        context["plan_origin"] = "resume_snapshot"
        return ExecutionPlan(
            plan_id=str(raw_plan.get("plan_id", f"resume-{uuid.uuid4().hex[:8]}")).strip() or f"resume-{uuid.uuid4().hex[:8]}",
            goal_id=str(goal_id or "").strip() or str(raw_plan.get("goal_id", "")) or str(uuid.uuid4()),
            intent=str(raw_plan.get("intent", "resume_plan")).strip() or "resume_plan",
            steps=plan_steps,
            context=context,
            created_at=str(raw_plan.get("created_at", datetime.now(timezone.utc).isoformat())),
        )

    @classmethod
    def _coerce_plan_step(cls, raw: Any, *, index: int) -> Optional[PlanStep]:
        if not isinstance(raw, dict):
            return None
        action = str(raw.get("action", "")).strip()
        if not action:
            return None
        step_id = str(raw.get("step_id", "")).strip() or f"resume-{index}"
        args = raw.get("args", {})
        verify = raw.get("verify", {})
        depends_on = raw.get("depends_on", [])

        max_retries = cls._coerce_int(raw.get("max_retries", 2), minimum=0, maximum=10, default=2)
        timeout_s = cls._coerce_int(raw.get("timeout_s", 30), minimum=1, maximum=300, default=30)
        can_retry = bool(raw.get("can_retry", True))
        return PlanStep(
            step_id=step_id,
            action=action,
            args=args if isinstance(args, dict) else {},
            depends_on=[str(item).strip() for item in depends_on if str(item).strip()] if isinstance(depends_on, list) else [],
            verify=verify if isinstance(verify, dict) else {},
            can_retry=can_retry,
            max_retries=max_retries,
            timeout_s=timeout_s,
        )

    @staticmethod
    def _resolve_goal_budget(source_name: str, metadata: Optional[Dict[str, object]] = None) -> Dict[str, int]:
        source = str(source_name or "").strip().lower()
        data = metadata if isinstance(metadata, dict) else {}

        runtime_default = AgentKernel._coerce_int(
            os.getenv("JARVIS_GOAL_MAX_RUNTIME_S", "180"),
            minimum=10,
            maximum=3600,
            default=180,
        )
        steps_default = AgentKernel._coerce_int(
            os.getenv("JARVIS_GOAL_MAX_STEPS", "24"),
            minimum=1,
            maximum=250,
            default=24,
        )
        runtime = AgentKernel._coerce_int(
            data.get("max_runtime_s", runtime_default),
            minimum=10,
            maximum=3600,
            default=runtime_default,
        )
        steps = AgentKernel._coerce_int(
            data.get("max_steps", steps_default),
            minimum=1,
            maximum=250,
            default=steps_default,
        )

        if source in {"desktop-trigger", "desktop-schedule"}:
            auto_runtime = AgentKernel._coerce_int(
                os.getenv("JARVIS_AUTOMATION_MAX_RUNTIME_S", "120"),
                minimum=10,
                maximum=3600,
                default=120,
            )
            auto_steps = AgentKernel._coerce_int(
                os.getenv("JARVIS_AUTOMATION_MAX_STEPS", "12"),
                minimum=1,
                maximum=250,
                default=12,
            )
            runtime = min(runtime, auto_runtime)
            steps = min(steps, auto_steps)

        return {"max_runtime_s": runtime, "max_steps": steps}

    @staticmethod
    def _default_recovery_profile(policy_profile: str) -> str:
        profile = str(policy_profile or "").strip().lower()
        if profile == "automation_safe":
            return "safe"
        if profile == "automation_power":
            return "aggressive"
        return "balanced"

    @staticmethod
    def _apply_plan_runtime_hints(
        *,
        runtime_budget_s: int,
        step_budget: int,
        plan_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        current_runtime = max(10, min(int(runtime_budget_s), 3600))
        current_steps = max(1, min(int(step_budget), 250))
        context = plan_context if isinstance(plan_context, dict) else {}
        runtime_constraints = context.get("runtime_constraints")
        runtime_constraints_map = runtime_constraints if isinstance(runtime_constraints, dict) else {}

        hinted_runtime = context.get("time_budget_s", runtime_constraints_map.get("time_budget_s"))
        if hinted_runtime is not None and str(hinted_runtime).strip():
            hinted_runtime_value = AgentKernel._coerce_int(hinted_runtime, minimum=1, maximum=3600, default=current_runtime)
            current_runtime = min(current_runtime, hinted_runtime_value)

        deadline_at = str(context.get("deadline_at", runtime_constraints_map.get("deadline_at", ""))).strip()
        if deadline_at:
            parsed_deadline = AgentKernel._parse_iso_utc(deadline_at)
            if parsed_deadline is not None:
                delta_s = max(1, int((parsed_deadline - datetime.now(timezone.utc)).total_seconds()))
                current_runtime = min(current_runtime, delta_s)

        hinted_steps = context.get("max_steps_hint", runtime_constraints_map.get("max_steps_hint"))
        if hinted_steps is not None and str(hinted_steps).strip():
            hinted_steps_value = AgentKernel._coerce_int(hinted_steps, minimum=1, maximum=250, default=current_steps)
            current_steps = min(current_steps, hinted_steps_value)

        strictness = str(context.get("verification_strictness", runtime_constraints_map.get("verification_strictness", ""))).strip().lower()
        if strictness not in {"off", "standard", "strict"}:
            strictness = ""

        return {
            "max_runtime_s": current_runtime,
            "max_steps": current_steps,
            "verification_strictness": strictness,
            "deadline_at": deadline_at,
        }

    @staticmethod
    def _parse_iso_utc(value: object) -> Optional[datetime]:
        raw = str(value or "").strip()
        if not raw:
            return None
        normalized = raw.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except Exception:  # noqa: BLE001
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _coerce_int(value: object, *, minimum: int, maximum: int, default: int) -> int:
        try:
            parsed = int(value)
        except Exception:  # noqa: BLE001
            parsed = default
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _coerce_float(value: object, *, minimum: float, maximum: float, default: float) -> float:
        try:
            parsed = float(value)
        except Exception:  # noqa: BLE001
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
        return AgentKernel._coerce_bool(raw, default=default)

    def _update_schedule_checkpoint(self, goal: GoalRecord) -> None:
        metadata = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
        schedule_id = str(metadata.get(SCHEDULE_METADATA_KEY, "")).strip()
        if not schedule_id:
            return

        if goal.status.value == "completed":
            final_status = "completed"
            reason = ""
        else:
            final_status = "failed" if goal.status.value == "failed" else goal.status.value
            reason = goal.failure_reason or ""

        record = self.schedule_manager.mark_goal_result(
            schedule_id,
            goal_id=goal.goal_id,
            goal_status=final_status,
            failure_reason=reason,
        )
        if record:
            self.telemetry.emit(
                "schedule.updated",
                {
                    "schedule_id": schedule_id,
                    "status": record.status,
                    "goal_id": goal.goal_id,
                    "attempt_count": record.attempt_count,
                },
            )
