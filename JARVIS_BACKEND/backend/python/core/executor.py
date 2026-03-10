import asyncio
import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from .approval_gate import ApprovalGate
from .circuit_breaker import ActionCircuitBreaker
from .contracts import ActionRequest, ActionResult, ExecutionPlan, PlanStep
from .desktop_anchor_memory import DesktopAnchorMemory
from .external_reliability import ExternalReliabilityOrchestrator
from .recovery import RecoveryManager
from .rollback_manager import RollbackManager
from .telemetry import Telemetry
from .tool_registry import ToolRegistry
from .verifier import Verifier
from backend.python.policies.policy_guard import PolicyGuard


class Executor:
    _EXTERNAL_MUTATION_ACTIONS = {
        "external_email_send",
        "external_calendar_create_event",
        "external_calendar_update_event",
        "external_doc_create",
        "external_doc_update",
        "external_task_create",
        "external_task_update",
    }

    _EXTERNAL_HIGH_IMPACT_ACTIONS = {
        "external_email_send",
        "external_calendar_create_event",
        "external_calendar_update_event",
        "external_doc_create",
        "external_doc_update",
        "external_task_create",
        "external_task_update",
    }
    _EXTERNAL_REMEDIATION_TOOL_ALLOWLIST = {
        "oauth_token_refresh",
        "oauth_token_maintain",
        "oauth_token_list",
        "external_connector_status",
        "external_connector_preflight",
    }

    def __init__(
        self,
        registry: ToolRegistry,
        policy_guard: PolicyGuard,
        verifier: Verifier,
        recovery: RecoveryManager,
        telemetry: Telemetry,
        approval_gate: ApprovalGate,
        rollback_manager: RollbackManager | None = None,
        circuit_breaker: ActionCircuitBreaker | None = None,
        desktop_state: Any | None = None,
        desktop_anchor_memory: DesktopAnchorMemory | None = None,
        external_reliability: ExternalReliabilityOrchestrator | None = None,
    ) -> None:
        self.registry = registry
        self.policy_guard = policy_guard
        self.verifier = verifier
        self.recovery = recovery
        self.telemetry = telemetry
        self.approval_gate = approval_gate
        self.rollback_manager = rollback_manager
        self.circuit_breaker = circuit_breaker
        self.desktop_state = desktop_state
        self.desktop_anchor_memory = desktop_anchor_memory
        self.external_reliability = external_reliability
        self.external_mutation_simulation_enabled = self._env_flag(
            "JARVIS_EXTERNAL_MUTATION_SIM_ENABLED",
            default=True,
        )
        self.external_mutation_simulation_high_impact_only = self._env_flag(
            "JARVIS_EXTERNAL_MUTATION_SIM_HIGH_IMPACT_ONLY",
            default=True,
        )
        self.external_mutation_sim_timeout_s = self._coerce_int(
            os.getenv("JARVIS_EXTERNAL_MUTATION_SIM_TIMEOUT_S", "25"),
            minimum=5,
            maximum=120,
            default=25,
        )
        self.external_branch_guard_enabled = self._env_flag(
            "JARVIS_EXTERNAL_BRANCH_GUARD_ENABLED",
            default=True,
        )
        self.external_branch_guard_enforce = self._env_flag(
            "JARVIS_EXTERNAL_BRANCH_GUARD_ENFORCE",
            default=False,
        )
        self.external_remediation_delay_enabled = self._env_flag(
            "JARVIS_EXTERNAL_REMEDIATION_DELAY_ENABLED",
            default=True,
        )
        self.external_remediation_delay_cap_s = self._coerce_float(
            os.getenv("JARVIS_EXTERNAL_REMEDIATION_DELAY_CAP_S", "2.5"),
            minimum=0.0,
            maximum=60.0,
            default=2.5,
        )

    @staticmethod
    def _env_flag(name: str, *, default: bool = False) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return default
        clean = str(raw).strip().lower()
        if clean in {"1", "true", "yes", "on"}:
            return True
        if clean in {"0", "false", "no", "off"}:
            return False
        return bool(default)

    @staticmethod
    def _coerce_int(value: Any, *, minimum: int, maximum: int, default: int) -> int:
        try:
            parsed = int(value)
        except Exception:
            parsed = default
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _coerce_float(value: Any, *, minimum: float, maximum: float, default: float) -> float:
        try:
            parsed = float(value)
        except Exception:
            parsed = default
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _coerce_bool(value: Any, *, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        clean = str(value).strip().lower()
        if clean in {"1", "true", "yes", "on"}:
            return True
        if clean in {"0", "false", "no", "off"}:
            return False
        return default

    async def execute_plan(
        self,
        plan: ExecutionPlan,
        source: str = "planner",
        metadata: Dict[str, Any] | None = None,
        interrupt_check: Callable[[], bool] | None = None,
        interrupt_reason: str = "Goal cancelled by user request.",
        interrupt_reason_provider: Callable[[], str] | None = None,
        on_step_started: Callable[[PlanStep], None] | None = None,
        on_step_result: Callable[[ActionResult], None] | None = None,
    ) -> List[ActionResult]:
        results: List[ActionResult] = []
        metadata = metadata or {}

        allow_parallel = bool(plan.context.get("allow_parallel", False)) if isinstance(plan.context, dict) else False
        if isinstance(plan.context, dict):
            configured_parallel = int(plan.context.get("max_parallel_steps", 0) or 0)
        else:
            configured_parallel = 0
        env_parallel = int(os.getenv("JARVIS_MAX_PARALLEL_STEPS", "2"))
        if isinstance(metadata, dict) and "execution_allow_parallel" in metadata:
            allow_parallel = self._coerce_bool(metadata.get("execution_allow_parallel", allow_parallel), default=allow_parallel)
        if isinstance(metadata, dict) and "execution_max_parallel_steps" in metadata:
            configured_parallel = self._coerce_int(
                metadata.get("execution_max_parallel_steps", configured_parallel or env_parallel),
                minimum=1,
                maximum=6,
                default=configured_parallel or env_parallel,
            )
        effective_parallel = max(1, min(configured_parallel or env_parallel, 6)) if allow_parallel else 1

        step_map: Dict[str, PlanStep] = {}
        dependency_map: Dict[str, List[str]] = {}
        for step in plan.steps:
            step_map[step.step_id] = step
        for step in plan.steps:
            raw_deps = step.depends_on if isinstance(step.depends_on, list) else []
            dependency_map[step.step_id] = [str(dep).strip() for dep in raw_deps if str(dep).strip()]

        pending_ids: List[str] = [step.step_id for step in plan.steps]
        completed_ids: set[str] = set()
        in_flight: Dict[asyncio.Task[ActionResult], PlanStep] = {}
        step_snapshots: Dict[str, Dict[str, Any]] = {}
        action_history: Dict[str, List[Dict[str, Any]]] = {}
        last_snapshot: Dict[str, Any] | None = None
        failed_or_blocked = False

        while pending_ids or in_flight:
            ready_ids = [
                step_id
                for step_id in pending_ids
                if all(dep in completed_ids for dep in dependency_map.get(step_id, []))
            ]

            while ready_ids and len(in_flight) < effective_parallel:
                step_id = ready_ids.pop(0)
                if step_id not in pending_ids:
                    continue
                pending_ids.remove(step_id)
                step = step_map[step_id]
                prepared_step = self._prepare_step_for_execution(
                    step,
                    step_snapshots=step_snapshots,
                    action_history=action_history,
                    last_snapshot=last_snapshot,
                )

                if interrupt_check is not None and interrupt_check():
                    interrupted = self._interrupted_result(
                        prepared_step.action,
                        self._resolve_interrupt_reason(interrupt_reason, interrupt_reason_provider),
                    )
                    results.append(interrupted)
                    if on_step_result is not None:
                        try:
                            on_step_result(interrupted)
                        except Exception:  # noqa: BLE001
                            pass
                    failed_or_blocked = True
                    break

                self.telemetry.emit(
                    "step.started",
                    {
                        "goal_id": plan.goal_id,
                        "plan_id": plan.plan_id,
                        "step_id": prepared_step.step_id,
                        "action": prepared_step.action,
                    },
                )
                if on_step_started is not None:
                    try:
                        on_step_started(prepared_step)
                    except Exception:  # noqa: BLE001
                        pass
                task = asyncio.create_task(
                    self._run_step(
                        prepared_step,
                        source,
                        metadata=metadata,
                        interrupt_check=interrupt_check,
                        interrupt_reason=interrupt_reason,
                        interrupt_reason_provider=interrupt_reason_provider,
                    )
                )
                in_flight[task] = prepared_step

            if failed_or_blocked:
                break

            if not in_flight:
                if pending_ids:
                    first_pending = step_map[pending_ids[0]]
                    unresolved = dependency_map.get(first_pending.step_id, [])
                    missing = [dep for dep in unresolved if dep not in completed_ids]
                    reason = (
                        f"Unresolved step dependencies for {first_pending.action}: {missing}"
                        if missing
                        else "Plan dependency deadlock detected."
                    )
                    results.append(ActionResult(action=first_pending.action, status="failed", error=reason))
                break

            done, _ = await asyncio.wait(in_flight.keys(), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                step = in_flight.pop(task)
                try:
                    result = task.result()
                except Exception as exc:  # noqa: BLE001
                    result = ActionResult(action=step.action, status="failed", error=str(exc))

                results.append(result)
                snapshot = self._result_snapshot(result)
                step_snapshots[step.step_id] = snapshot
                action_history.setdefault(step.action, []).append(snapshot)
                last_snapshot = snapshot
                if on_step_result is not None:
                    try:
                        on_step_result(result)
                    except Exception:  # noqa: BLE001
                        pass
                self.telemetry.emit(
                    "step.finished",
                    {
                        "goal_id": plan.goal_id,
                        "plan_id": plan.plan_id,
                        "step_id": step.step_id,
                        "action": step.action,
                        "status": result.status,
                        "attempt": result.attempt,
                        "duration_ms": result.duration_ms,
                        "error": result.error,
                    },
                )

                if result.status in ("failed", "blocked"):
                    failed_or_blocked = True
                    for pending_task in list(in_flight.keys()):
                        pending_task.cancel()
                    if in_flight:
                        await asyncio.gather(*in_flight.keys(), return_exceptions=True)
                    in_flight.clear()
                    break

                completed_ids.add(step.step_id)
            if failed_or_blocked:
                break

        return results

    def _prepare_step_for_execution(
        self,
        step: PlanStep,
        *,
        step_snapshots: Dict[str, Dict[str, Any]],
        action_history: Dict[str, List[Dict[str, Any]]],
        last_snapshot: Dict[str, Any] | None,
    ) -> PlanStep:
        rendered_args = self._render_plan_payload(
            step.args,
            step=step,
            step_snapshots=step_snapshots,
            action_history=action_history,
            last_snapshot=last_snapshot,
        )
        args = rendered_args if isinstance(rendered_args, dict) else dict(step.args)
        verify = step.verify if isinstance(step.verify, dict) else {}
        return PlanStep(
            step_id=step.step_id,
            action=step.action,
            args=args,
            depends_on=list(step.depends_on),
            verify=verify,
            status=step.status,
            can_retry=step.can_retry,
            max_retries=step.max_retries,
            timeout_s=step.timeout_s,
        )

    def _render_plan_payload(
        self,
        payload: Any,
        *,
        step: PlanStep,
        step_snapshots: Dict[str, Dict[str, Any]],
        action_history: Dict[str, List[Dict[str, Any]]],
        last_snapshot: Dict[str, Any] | None,
    ) -> Any:
        if isinstance(payload, dict):
            return {
                str(key): self._render_plan_payload(
                    value,
                    step=step,
                    step_snapshots=step_snapshots,
                    action_history=action_history,
                    last_snapshot=last_snapshot,
                )
                for key, value in payload.items()
            }
        if isinstance(payload, list):
            return [
                self._render_plan_payload(
                    item,
                    step=step,
                    step_snapshots=step_snapshots,
                    action_history=action_history,
                    last_snapshot=last_snapshot,
                )
                for item in payload
            ]
        if not isinstance(payload, str):
            return payload

        token_match = re.fullmatch(r"\{\{\s*(args|steps|actions|last)\.([a-zA-Z0-9_.-]+)\s*\}\}", payload)
        if not token_match:
            return payload

        source_kind = token_match.group(1)
        source_path = token_match.group(2)
        resolved = self._resolve_plan_token(
            source_kind=source_kind,
            source_path=source_path,
            step=step,
            step_snapshots=step_snapshots,
            action_history=action_history,
            last_snapshot=last_snapshot,
        )
        return resolved if resolved is not None else payload

    def _resolve_plan_token(
        self,
        *,
        source_kind: str,
        source_path: str,
        step: PlanStep,
        step_snapshots: Dict[str, Dict[str, Any]],
        action_history: Dict[str, List[Dict[str, Any]]],
        last_snapshot: Dict[str, Any] | None,
    ) -> Any:
        if source_kind == "args":
            return self._resolve_path(step.args if isinstance(step.args, dict) else {}, source_path)

        if source_kind == "steps":
            step_id, dot, path_tail = source_path.partition(".")
            snapshot = step_snapshots.get(step_id)
            if snapshot is None:
                return None
            if not dot:
                return snapshot
            return self._resolve_path(snapshot, path_tail)

        if source_kind == "actions":
            action_name, dot, path_tail = source_path.partition(".")
            history = action_history.get(action_name, [])
            if not dot:
                return {"count": len(history)}
            if path_tail == "count":
                return len(history)
            if path_tail.startswith("last."):
                if not history:
                    return None
                return self._resolve_path(history[-1], path_tail[len("last.") :])
            return None

        if source_kind == "last":
            if last_snapshot is None:
                return None
            return self._resolve_path(last_snapshot, source_path)

        return None

    @staticmethod
    def _result_snapshot(result: ActionResult) -> Dict[str, Any]:
        return {
            "action": result.action,
            "status": result.status,
            "output": result.output if isinstance(result.output, dict) else {},
            "error": result.error,
            "attempt": result.attempt,
            "duration_ms": result.duration_ms,
            "completed_at": result.completed_at,
            "evidence": result.evidence if isinstance(result.evidence, dict) else {},
        }

    @staticmethod
    def _compact_voice_notification_text(text: str, *, max_chars: int = 220) -> str:
        clean = " ".join(str(text or "").strip().split())
        if len(clean) <= max_chars:
            return clean
        truncated = clean[: max(0, max_chars - 3)].rstrip(" ,;:.")
        return f"{truncated}..." if truncated else clean[:max_chars]

    def _voice_followup_tool_available(
        self,
        action: str,
        *,
        notification_available: bool = False,
    ) -> bool:
        clean_action = str(action or "").strip().lower()
        if not clean_action:
            return False
        if clean_action == "send_notification" and not notification_available:
            return False
        return bool(self.registry.has(clean_action))

    @staticmethod
    def _voice_followup_channel_priority(
        *,
        clean_action: str,
        execution_policy: Dict[str, Any],
        delivery_policy: Dict[str, Any],
        runtime_redirect_action: str,
    ) -> List[str]:
        ordered: List[str] = []

        def _append(value: Any) -> None:
            clean = str(value or "").strip().lower()
            if clean in {"send_notification", "clipboard_write", "open_url", "open_app"} and clean not in ordered:
                ordered.append(clean)

        raw_priority = execution_policy.get("followup_channel_priority", [])
        if isinstance(raw_priority, (list, tuple)):
            for item in raw_priority:
                _append(item)
        elif raw_priority:
            _append(raw_priority)
        _append(runtime_redirect_action)
        _append(delivery_policy.get("fallback_action", ""))
        _append(execution_policy.get("preferred_followup_action", ""))
        _append(clean_action)
        _append("send_notification")
        _append("clipboard_write")
        _append("open_url")
        _append("open_app")
        return ordered

    def _rank_voice_followup_channels(
        self,
        *,
        clean_action: str,
        execution_policy: Dict[str, Any],
        delivery_policy: Dict[str, Any],
        runtime_redirect_action: str,
        notification_available: bool,
    ) -> List[str]:
        base_priority = self._voice_followup_channel_priority(
            clean_action=clean_action,
            execution_policy=execution_policy,
            delivery_policy=delivery_policy,
            runtime_redirect_action=runtime_redirect_action,
        )
        if not base_priority:
            return []

        preferred_action = str(execution_policy.get("preferred_followup_action", "") or "").strip().lower()
        selected_present_action = str(
            execution_policy.get("selected_present_followup_action", "") or ""
        ).strip().lower()
        confirmation_mode = str(execution_policy.get("confirmation_mode", "") or "").strip().lower()
        mission_risk_level = str(execution_policy.get("mission_risk_level", "") or "").strip().lower()
        prefer_notification_followup = bool(execution_policy.get("prefer_notification_followup", False))
        prefer_non_voice_completion = bool(execution_policy.get("prefer_non_voice_completion", False))
        avoid_multi_turn_voice_loop = bool(execution_policy.get("avoid_multi_turn_voice_loop", False))
        local_voice_pressure_score = max(
            0.0,
            min(1.0, float(execution_policy.get("local_voice_pressure_score", 0.0) or 0.0)),
        )
        pause_pressure = max(0.0, min(1.0, float(execution_policy.get("pause_pressure", 0.0) or 0.0)))
        planner_rank_by_action: Dict[str, int] = {}
        raw_planner_candidates = execution_policy.get("planner_followup_candidates", [])
        if isinstance(raw_planner_candidates, list):
            for item in raw_planner_candidates:
                if not isinstance(item, dict):
                    continue
                action = str(item.get("action", "") or "").strip().lower()
                if not action:
                    continue
                rank = int(item.get("rank", 0) or 0)
                if rank <= 0:
                    continue
                current_rank = planner_rank_by_action.get(action)
                if current_rank is None or rank < current_rank:
                    planner_rank_by_action[action] = rank

        scored: List[tuple[str, float, int]] = []
        for index, candidate in enumerate(base_priority):
            score = max(0.0, 6.5 - float(index))
            if candidate == clean_action:
                score += 0.15
            if candidate == preferred_action:
                score += 1.2
            if candidate == selected_present_action:
                score += 1.0
            if candidate == runtime_redirect_action:
                score += 0.85
            planner_rank = planner_rank_by_action.get(candidate)
            if planner_rank is not None:
                score += max(0.0, 1.0 - (float(max(1, planner_rank)) - 1.0) * 0.17)

            if mission_risk_level == "high":
                if candidate == "send_notification":
                    score += 1.75
                elif candidate == "clipboard_write":
                    score += 0.95
                elif candidate == "open_url":
                    score -= 0.05
                elif candidate == "open_app":
                    score -= 1.65
            elif mission_risk_level == "medium":
                if candidate == "send_notification":
                    score += 0.45
                elif candidate == "clipboard_write":
                    score += 0.55
                elif candidate == "open_url":
                    score += 0.28
                elif candidate == "open_app":
                    score -= 0.08
            elif mission_risk_level == "low":
                if candidate == "open_app":
                    score += 0.55
                elif candidate == "open_url":
                    score += 0.35
                elif candidate == "clipboard_write":
                    score += 0.12

            if confirmation_mode == "explicit":
                if candidate == "send_notification":
                    score += 0.55
                elif candidate == "clipboard_write":
                    score += 0.2
                elif candidate == "open_app":
                    score -= 0.25
            elif confirmation_mode == "compact" and candidate == "clipboard_write":
                score += 0.2

            if prefer_notification_followup and candidate == "send_notification":
                score += 0.6
            if prefer_non_voice_completion:
                if candidate == "clipboard_write":
                    score += 0.3
                elif candidate == "open_url":
                    score += 0.18
                elif candidate == "open_app":
                    score += 0.12
            if avoid_multi_turn_voice_loop:
                if candidate == "send_notification":
                    score += 0.3
                elif candidate == "clipboard_write":
                    score += 0.26
                elif candidate == "open_app":
                    score -= 0.18

            if local_voice_pressure_score >= 0.7 or pause_pressure >= 0.45:
                if candidate == "send_notification":
                    score += 0.35
                elif candidate == "clipboard_write":
                    score += 0.25
                elif candidate == "open_app":
                    score -= 0.28
            elif mission_risk_level == "low" and local_voice_pressure_score < 0.45 and pause_pressure < 0.3:
                if candidate == "open_app":
                    score += 0.24
                elif candidate == "open_url":
                    score += 0.15

            if not self._voice_followup_tool_available(
                candidate,
                notification_available=notification_available,
            ):
                score -= 100.0
            scored.append((candidate, score, index))

        scored.sort(key=lambda item: (-item[1], item[2], item[0]))
        ordered = [candidate for candidate, _score, _index in scored]
        return ordered or base_priority

    def _build_voice_followup_override(
        self,
        *,
        target_action: str,
        original_action: str,
        original_args: Dict[str, Any],
        notification_available: bool,
        notification_title: str,
        notification_message_max_chars: int,
        clipboard_text_max_chars: int,
        runtime_redirect_args: Dict[str, Any],
        redirect_reason_code: str,
        redirect_reason: str,
        confirmation_mode: str,
    ) -> Dict[str, Any]:
        clean_target = str(target_action or "").strip().lower()
        if not self._voice_followup_tool_available(
            clean_target,
            notification_available=notification_available,
        ):
            return {}
        base_args = dict(original_args or {}) if isinstance(original_args, dict) else {}
        merged_args = dict(base_args)
        for key, value in (runtime_redirect_args or {}).items():
            if key not in merged_args or merged_args.get(key) in {"", None}:
                merged_args[key] = value
        default_title = notification_title or (
            "JARVIS Voice Confirmation" if confirmation_mode == "explicit" else "JARVIS Voice Follow-up"
        )
        base_title = str(
            merged_args.get("title", "")
            or merged_args.get("label", "")
            or merged_args.get("name", "")
            or merged_args.get("app", "")
            or default_title
        ).strip()
        base_message = str(
            merged_args.get("message", "")
            or merged_args.get("text", "")
            or merged_args.get("description", "")
            or merged_args.get("url", "")
            or merged_args.get("path", "")
            or merged_args.get("name", "")
            or merged_args.get("app", "")
            or "Voice follow-up redirected to a stable non-speech channel."
        ).strip()
        if clean_target == "send_notification":
            notification_message = base_message or "Voice follow-up redirected to notification."
            fallback_url = str(merged_args.get("url", "") or "").strip()
            if fallback_url and fallback_url not in notification_message:
                notification_message = f"{notification_message} {fallback_url}".strip()
            return {
                "action": "send_notification",
                "args": {
                    "title": base_title or default_title,
                    "message": self._compact_voice_notification_text(
                        notification_message,
                        max_chars=max(80, notification_message_max_chars),
                    ),
                },
                "reason_code": redirect_reason_code or "voice_followup_notification_redirect",
                "reason": redirect_reason or "Voice follow-up redirected to notification.",
                "original_action": original_action,
                "original_args": dict(original_args or {}) if isinstance(original_args, dict) else {},
            }
        if clean_target == "clipboard_write":
            clipboard_text = str(
                merged_args.get("text", "")
                or merged_args.get("message", "")
                or merged_args.get("description", "")
                or merged_args.get("url", "")
                or merged_args.get("path", "")
                or base_message
            ).strip()
            if clipboard_text_max_chars > 0:
                clipboard_text = self._compact_voice_notification_text(
                    clipboard_text,
                    max_chars=max(80, clipboard_text_max_chars),
                )
            if not clipboard_text:
                return {}
            return {
                "action": "clipboard_write",
                "args": {"text": clipboard_text},
                "reason_code": redirect_reason_code or "voice_followup_clipboard_redirect",
                "reason": redirect_reason or "Voice follow-up redirected to clipboard.",
                "original_action": original_action,
                "original_args": dict(original_args or {}) if isinstance(original_args, dict) else {},
            }
        if clean_target == "open_url":
            clean_url = str(merged_args.get("url", "") or "").strip()
            if not clean_url:
                return {}
            override_args = dict(merged_args)
            for text_key in ("label", "title", "description", "message"):
                raw_value = str(override_args.get(text_key, "") or "").strip()
                if raw_value and notification_message_max_chars > 0:
                    override_args[text_key] = self._compact_voice_notification_text(
                        raw_value,
                        max_chars=max(80, min(notification_message_max_chars, 220)),
                    )
            return {
                "action": "open_url",
                "args": override_args,
                "reason_code": redirect_reason_code or "voice_followup_open_url_redirect",
                "reason": redirect_reason or "Voice follow-up redirected to browser handoff.",
                "original_action": original_action,
                "original_args": dict(original_args or {}) if isinstance(original_args, dict) else {},
            }
        if clean_target == "open_app":
            if not any(str(merged_args.get(key, "") or "").strip() for key in ("name", "app", "bundle_id", "path")):
                return {}
            override_args = dict(merged_args)
            for text_key in ("name", "app", "label"):
                raw_value = str(override_args.get(text_key, "") or "").strip()
                if raw_value and notification_message_max_chars > 0:
                    override_args[text_key] = self._compact_voice_notification_text(
                        raw_value,
                        max_chars=max(48, min(notification_message_max_chars, 160)),
                    )
            return {
                "action": "open_app",
                "args": override_args,
                "reason_code": redirect_reason_code or "voice_followup_open_app_redirect",
                "reason": redirect_reason or "Voice follow-up redirected to app handoff.",
                "original_action": original_action,
                "original_args": dict(original_args or {}) if isinstance(original_args, dict) else {},
            }
        return {}

    def _resolve_voice_runtime_request_override(
        self,
        *,
        step: PlanStep,
        source: str,
        metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        source_name = str(source or metadata.get("runtime_goal_source", "") or "").strip().lower()
        execution_policy = (
            dict(metadata.get("voice_execution_policy", {}))
            if isinstance(metadata.get("voice_execution_policy", {}), dict)
            else {}
        )
        planner_followup_contract = (
            dict(execution_policy.get("planner_followup_contract", {}))
            if isinstance(execution_policy.get("planner_followup_contract", {}), dict)
            else {}
        )
        if not isinstance(execution_policy.get("planner_followup_candidates", []), list) and isinstance(
            planner_followup_contract.get("candidates", []), list
        ):
            execution_policy["planner_followup_candidates"] = list(planner_followup_contract.get("candidates", []))
        if not str(execution_policy.get("selected_present_followup_action", "") or "").strip():
            selected_from_contract = str(planner_followup_contract.get("selected_followup_action", "") or "").strip()
            if selected_from_contract:
                execution_policy["selected_present_followup_action"] = selected_from_contract
        if not str(execution_policy.get("preferred_followup_action", "") or "").strip():
            preferred_from_contract = str(planner_followup_contract.get("preferred_followup_action", "") or "").strip()
            if preferred_from_contract:
                execution_policy["preferred_followup_action"] = preferred_from_contract
        recovery_handoff_active = bool(
            execution_policy.get("recovery_handoff_active", False)
            or planner_followup_contract.get("recovery_handoff_active", False)
            or metadata.get("voice_recovery_handoff", False)
            or metadata.get("voice_session_id")
        )
        policy_scope = str(
            execution_policy.get("policy_scope", "") or planner_followup_contract.get("policy_scope", "")
        ).strip().lower()
        if not (
            source_name.startswith("voice")
            or "voice" in source_name
            or recovery_handoff_active
            or policy_scope == "voice_recovery_handoff"
        ):
            return {}
        delivery_policy = (
            dict(metadata.get("voice_delivery_policy", {}))
            if isinstance(metadata.get("voice_delivery_policy", {}), dict)
            else {}
        )
        notification_available = bool(delivery_policy.get("notification_fallback_available", False))
        clean_action = str(step.action or "").strip().lower()
        confirmation_mode = str(execution_policy.get("confirmation_mode", "") or "").strip().lower()
        followup_mode = str(execution_policy.get("followup_mode", "") or "").strip().lower()
        notification_title = str(execution_policy.get("notification_title", "") or "").strip()
        notification_message_max_chars = int(execution_policy.get("notification_message_max_chars", 0) or 220)
        clipboard_text_max_chars = int(execution_policy.get("clipboard_text_max_chars", 0) or 0)
        runtime_redirect_action = str(
            execution_policy.get("runtime_redirect_action", "")
            or planner_followup_contract.get("selected_followup_action", "")
            or delivery_policy.get("fallback_action", "")
            or ""
        ).strip().lower()
        runtime_redirect_args = (
            dict(execution_policy.get("runtime_redirect_args", {}))
            if isinstance(execution_policy.get("runtime_redirect_args", {}), dict)
            else {}
        )
        if not runtime_redirect_args and isinstance(planner_followup_contract.get("candidates", []), list):
            selected_action = str(planner_followup_contract.get("selected_followup_action", "") or "").strip().lower()
            for item in planner_followup_contract.get("candidates", []):
                if not isinstance(item, dict):
                    continue
                if str(item.get("action", "") or "").strip().lower() != selected_action:
                    continue
                if isinstance(item.get("args", {}), dict):
                    runtime_redirect_args = dict(item.get("args", {}))
                break
        selected_contract_action = str(
            planner_followup_contract.get("selected_followup_action", "")
            or runtime_redirect_action
            or ""
        ).strip().lower()
        selected_contract_candidate: Dict[str, Any] = {}
        if selected_contract_action and isinstance(planner_followup_contract.get("candidates", []), list):
            for item in planner_followup_contract.get("candidates", []):
                if not isinstance(item, dict):
                    continue
                if str(item.get("action", "") or "").strip().lower() != selected_contract_action:
                    continue
                selected_contract_candidate = dict(item)
                break
        selected_contract_reason = str(
            selected_contract_candidate.get("channel_reason", "")
            or planner_followup_contract.get("handoff_reason", "")
            or ""
        ).strip()
        try:
            selected_contract_score = int(selected_contract_candidate.get("selection_score", 0) or 0)
        except Exception:
            selected_contract_score = 0
        followup_channel_priority = self._rank_voice_followup_channels(
            clean_action=clean_action,
            execution_policy=execution_policy,
            delivery_policy=delivery_policy,
            runtime_redirect_action=runtime_redirect_action,
            notification_available=notification_available,
        )
        should_redirect = bool(
            delivery_policy.get("suppress_tts", False)
            or execution_policy.get("prefer_notification_followup", False)
            or execution_policy.get("prefer_non_voice_completion", False)
            or recovery_handoff_active
        )
        redirect_reason_code = str(
            delivery_policy.get("reason_code", "")
            or execution_policy.get("followup_mode", "")
            or planner_followup_contract.get("policy_scope", "")
            or "voice_runtime_redirect"
        ).strip().lower() or "voice_runtime_redirect"
        redirect_reason = str(
            delivery_policy.get("reason", "")
            or planner_followup_contract.get("handoff_reason", "")
            or "Voice delivery redirected at runtime."
        ).strip()
        original_args = dict(step.args or {}) if isinstance(step.args, dict) else {}

        def _finalize_override(override: Dict[str, Any]) -> Dict[str, Any]:
            if not isinstance(override, dict) or not override:
                return {}
            override["planner_followup_contract"] = {
                "policy_scope": str(
                    planner_followup_contract.get("policy_scope", "") or execution_policy.get("policy_scope", "")
                ).strip().lower(),
                "recovery_handoff_active": bool(recovery_handoff_active),
                "handoff_reason": str(
                    planner_followup_contract.get("handoff_reason", "")
                    or redirect_reason
                    or ""
                ).strip(),
                "preferred_followup_action": str(
                    planner_followup_contract.get("preferred_followup_action", "")
                    or execution_policy.get("preferred_followup_action", "")
                    or ""
                ).strip().lower(),
                "selected_followup_action": selected_contract_action,
                "selection_score": int(selected_contract_score),
                "channel_reason": selected_contract_reason,
                "present_followup_actions": list(execution_policy.get("present_followup_actions", []))
                if isinstance(execution_policy.get("present_followup_actions", []), list)
                else [],
            }
            return override

        def _select_override(*, allow_current_action: bool = True) -> Dict[str, Any]:
            for candidate in followup_channel_priority:
                if not allow_current_action and candidate == clean_action:
                    continue
                override = self._build_voice_followup_override(
                    target_action=candidate,
                    original_action=str(step.action or ""),
                    original_args=original_args,
                    notification_available=notification_available,
                    notification_title=notification_title,
                    notification_message_max_chars=notification_message_max_chars,
                    clipboard_text_max_chars=clipboard_text_max_chars,
                    runtime_redirect_args=runtime_redirect_args,
                    redirect_reason_code=redirect_reason_code,
                    redirect_reason=redirect_reason,
                    confirmation_mode=confirmation_mode,
                )
                if override:
                    return _finalize_override(override)
            return {}

        if clean_action == "send_notification":
            if not (
                should_redirect
                or followup_mode in {"notification", "hybrid"}
                or str(delivery_policy.get("mode", "")).strip().lower().startswith("notification")
            ):
                return {}
            if should_redirect:
                prioritized = _select_override()
                if prioritized and str(prioritized.get("action", "")).strip().lower() != "send_notification":
                    return prioritized
            if not self._voice_followup_tool_available("send_notification", notification_available=notification_available):
                return _select_override(allow_current_action=False)
            current_args = dict(step.args or {}) if isinstance(step.args, dict) else {}
            raw_message = str(
                current_args.get("message", "")
                or current_args.get("text", "")
                or current_args.get("title", "")
                or "Voice follow-up delivered by notification."
            ).strip()
            normalized_title = str(current_args.get("title", "")).strip() or notification_title
            if not normalized_title:
                normalized_title = "JARVIS Voice Confirmation" if confirmation_mode == "explicit" else "JARVIS Voice Follow-up"
            current_args["title"] = normalized_title
            current_args["message"] = self._compact_voice_notification_text(
                raw_message,
                max_chars=max(80, notification_message_max_chars),
            )
            return _finalize_override({
                "action": "send_notification",
                "args": current_args,
                "reason_code": redirect_reason_code or "voice_notification_normalized",
                "reason": redirect_reason or "Voice notification normalized at runtime.",
                "original_action": step.action,
                "original_args": dict(step.args or {}) if isinstance(step.args, dict) else {},
            })
        if clean_action == "open_url":
            if not (
                should_redirect
                or followup_mode in {"notification", "hybrid"}
                or runtime_redirect_action == "open_url"
            ):
                return {}
            if should_redirect:
                prioritized = _select_override()
                if prioritized and str(prioritized.get("action", "")).strip().lower() != "open_url":
                    return prioritized
            if not self._voice_followup_tool_available("open_url", notification_available=notification_available):
                return _select_override(allow_current_action=False)
            current_args = dict(step.args or {}) if isinstance(step.args, dict) else {}
            merged_args = dict(current_args)
            if runtime_redirect_action == "open_url" and runtime_redirect_args:
                for key, value in runtime_redirect_args.items():
                    if key == "url":
                        clean_url = str(value or "").strip()
                        if clean_url:
                            merged_args["url"] = clean_url
                        continue
                    if key not in merged_args or merged_args.get(key) in {"", None}:
                        merged_args[key] = value
            for text_key in ("label", "title", "description", "message"):
                raw_value = str(merged_args.get(text_key, "") or "").strip()
                if raw_value and notification_message_max_chars > 0:
                    merged_args[text_key] = self._compact_voice_notification_text(
                        raw_value,
                        max_chars=max(80, min(notification_message_max_chars, 220)),
                    )
            if not str(merged_args.get("url", "") or "").strip():
                return {}
            return _finalize_override({
                "action": "open_url",
                "args": merged_args,
                "reason_code": redirect_reason_code or "voice_open_url_normalized",
                "reason": redirect_reason or "Voice URL follow-up normalized at runtime.",
                "original_action": step.action,
                "original_args": dict(step.args or {}) if isinstance(step.args, dict) else {},
            })
        if clean_action == "open_app":
            if not (
                should_redirect
                or followup_mode in {"notification", "hybrid"}
                or runtime_redirect_action == "open_app"
            ):
                return {}
            if should_redirect:
                prioritized = _select_override()
                if prioritized and str(prioritized.get("action", "")).strip().lower() != "open_app":
                    return prioritized
            if not self._voice_followup_tool_available("open_app", notification_available=notification_available):
                return _select_override(allow_current_action=False)
            current_args = dict(step.args or {}) if isinstance(step.args, dict) else {}
            merged_args = dict(current_args)
            if runtime_redirect_action == "open_app" and runtime_redirect_args:
                for key, value in runtime_redirect_args.items():
                    if key not in merged_args or merged_args.get(key) in {"", None}:
                        merged_args[key] = value
            for text_key in ("name", "app", "label"):
                raw_value = str(merged_args.get(text_key, "") or "").strip()
                if raw_value and notification_message_max_chars > 0:
                    merged_args[text_key] = self._compact_voice_notification_text(
                        raw_value,
                        max_chars=max(48, min(notification_message_max_chars, 160)),
                    )
            if not any(str(merged_args.get(key, "") or "").strip() for key in ("name", "app", "bundle_id", "path")):
                return {}
            return _finalize_override({
                "action": "open_app",
                "args": merged_args,
                "reason_code": redirect_reason_code or "voice_open_app_normalized",
                "reason": redirect_reason or "Voice app follow-up normalized at runtime.",
                "original_action": step.action,
                "original_args": dict(step.args or {}) if isinstance(step.args, dict) else {},
            })
        if clean_action == "clipboard_write":
            if not (
                should_redirect
                or followup_mode in {"notification", "hybrid"}
                or bool(execution_policy.get("prefer_non_voice_completion", False))
            ):
                return {}
            if should_redirect:
                prioritized = _select_override()
                if prioritized and str(prioritized.get("action", "")).strip().lower() != "clipboard_write":
                    return prioritized
            if not self._voice_followup_tool_available("clipboard_write", notification_available=notification_available):
                return _select_override(allow_current_action=False)
            current_args = dict(step.args or {}) if isinstance(step.args, dict) else {}
            raw_text = str(
                current_args.get("text", "")
                or current_args.get("message", "")
                or "Voice follow-up copied to clipboard."
            ).strip()
            if raw_text and clipboard_text_max_chars > 0:
                current_args["text"] = self._compact_voice_notification_text(
                    raw_text,
                    max_chars=max(80, clipboard_text_max_chars),
                )
            return _finalize_override({
                "action": "clipboard_write",
                "args": current_args,
                "reason_code": redirect_reason_code or "voice_clipboard_normalized",
                "reason": redirect_reason or "Voice clipboard follow-up normalized at runtime.",
                "original_action": step.action,
                "original_args": dict(step.args or {}) if isinstance(step.args, dict) else {},
            })
        if clean_action != "tts_speak":
            if recovery_handoff_active and selected_contract_action in {"send_notification", "clipboard_write", "open_url", "open_app"}:
                selected_override = self._build_voice_followup_override(
                    target_action=selected_contract_action,
                    original_action=str(step.action or ""),
                    original_args={},
                    notification_available=notification_available,
                    notification_title=notification_title,
                    notification_message_max_chars=notification_message_max_chars,
                    clipboard_text_max_chars=clipboard_text_max_chars,
                    runtime_redirect_args=runtime_redirect_args,
                    redirect_reason_code=redirect_reason_code,
                    redirect_reason=redirect_reason,
                    confirmation_mode=confirmation_mode,
                )
                if selected_override:
                    return _finalize_override(selected_override)
                return _select_override(allow_current_action=False)
            return {}
        if not should_redirect:
            return {}
        prioritized = _select_override(allow_current_action=False)
        if prioritized:
            return prioritized
        return {}

    async def _run_step(
        self,
        step: PlanStep,
        source: str,
        metadata: Dict[str, Any] | None = None,
        interrupt_check: Callable[[], bool] | None = None,
        interrupt_reason: str = "Goal cancelled by user request.",
        interrupt_reason_provider: Callable[[], str] | None = None,
    ) -> ActionResult:
        attempt = 1
        metadata = metadata or {}
        policy_profile = str(metadata.get("policy_profile", "")).strip().lower()
        recovery_profile = str(metadata.get("recovery_profile", "")).strip().lower()
        verification_strictness = str(metadata.get("verification_strictness", "")).strip().lower()
        retry_history: List[Dict[str, Any]] = []
        last_recovery_category = ""
        last_recovery_reason = ""
        last_external_repair: Dict[str, Any] = {}
        last_external_remediation: Dict[str, Any] = {}
        while True:
            circuit_scope = self._resolve_circuit_scope(step=step, metadata=metadata)
            external_mutation_simulation: Dict[str, Any] = {}
            rollback_profile: Dict[str, Any] = {}
            branch_context: Dict[str, Any] = {}
            if interrupt_check is not None and interrupt_check():
                interrupted = self._interrupted_result(
                    step.action,
                    self._resolve_interrupt_reason(interrupt_reason, interrupt_reason_provider),
                    attempt=attempt,
                )
                self._attach_recovery_evidence(
                    interrupted,
                    step=step,
                    attempt=attempt,
                    retry_history=retry_history,
                    last_category=last_recovery_category,
                    last_reason=last_recovery_reason,
                )
                self._record_desktop_anchor_outcome(step=step, metadata=metadata, result=interrupted)
                self._record_external_outcome(step=step, metadata=metadata, result=interrupted)
                return interrupted

            breaker_blocked = self._check_circuit_breaker(step=step, attempt=attempt, scope=circuit_scope)
            if breaker_blocked is not None:
                self._attach_recovery_evidence(
                    breaker_blocked,
                    step=step,
                    attempt=attempt,
                    retry_history=retry_history,
                    last_category="transient",
                    last_reason=breaker_blocked.error or "Circuit breaker open.",
                )
                self._record_desktop_anchor_outcome(step=step, metadata=metadata, result=breaker_blocked)
                self._record_external_outcome(step=step, metadata=metadata, result=breaker_blocked)
                return breaker_blocked

            pre_desktop_hash = ""
            pre_desktop_context: Dict[str, Any] = {}
            if self.desktop_state is not None:
                pre_desktop_hash, pre_desktop_context = self._read_pre_desktop_context(step=step)
                if pre_desktop_hash:
                    metadata["__desktop_pre_state_hash"] = pre_desktop_hash
                pre_window = str(pre_desktop_context.get("window_title", "")).strip()
                pre_app = str(pre_desktop_context.get("app", "")).strip()
                if pre_window:
                    metadata["__desktop_pre_window_title"] = pre_window
                if pre_app:
                    metadata["__desktop_pre_app"] = pre_app

            preflight = self._external_preflight(step=step, metadata=metadata)
            preflight_status = str(preflight.get("status", "")).strip().lower() if isinstance(preflight, dict) else ""
            if preflight_status in {"blocked", "error"}:
                failure_category = str(preflight.get("failure_category", "transient")).strip().lower() if isinstance(preflight, dict) else "transient"
                retryable_preflight = self._is_retryable_external_preflight(
                    failure_category=failure_category,
                    status=preflight_status,
                )
                preflight_message = str(preflight.get("message", "External reliability preflight failed.")) if isinstance(preflight, dict) else "External reliability preflight failed."
                if retryable_preflight:
                    lowered_message = preflight_message.strip().lower()
                    if "temporar" not in lowered_message and "retry" not in lowered_message:
                        preflight_message = f"{preflight_message.rstrip('.')} Temporarily blocked by external preflight."
                blocked = ActionResult(
                    action=step.action,
                    status="failed" if retryable_preflight else ("blocked" if preflight_status == "blocked" else "failed"),
                    error=preflight_message,
                    output={"status": "error", "external_reliability": preflight},
                    attempt=attempt,
                )
                blocked.evidence["external_reliability"] = preflight
                retry_hint = preflight.get("retry_hint") if isinstance(preflight, dict) else {}
                retry_contract = preflight.get("retry_contract") if isinstance(preflight, dict) else {}
                if (isinstance(retry_hint, dict) and retry_hint) or (isinstance(retry_contract, dict) and retry_contract):
                    self._apply_retry_hint(
                        step=step,
                        retry_hint=retry_hint if isinstance(retry_hint, dict) else {},
                        retry_contract=retry_contract if isinstance(retry_contract, dict) else {},
                    )
                if isinstance(retry_contract, dict) and retry_contract:
                    strategy_overrides = self._apply_retry_contract_runtime_strategy(
                        step=step,
                        retry_contract=retry_contract,
                        metadata=metadata,
                        policy_profile=policy_profile,
                        recovery_profile=recovery_profile,
                        verification_strictness=verification_strictness,
                    )
                    if strategy_overrides:
                        policy_profile = str(strategy_overrides.get("policy_profile", policy_profile)).strip().lower()
                        recovery_profile = str(strategy_overrides.get("recovery_profile", recovery_profile)).strip().lower()
                        verification_strictness = str(
                            strategy_overrides.get("verification_strictness", verification_strictness)
                        ).strip().lower()
                self._attach_recovery_evidence(
                    blocked,
                    step=step,
                    attempt=attempt,
                    retry_history=retry_history,
                    last_category=failure_category,
                    last_reason=preflight_message,
                )
                self._record_circuit_breaker(
                    action=step.action,
                    status=blocked.status,
                    failure_category=failure_category or "transient",
                    error=blocked.error or "",
                    scope=circuit_scope,
                )
                self._record_desktop_anchor_outcome(step=step, metadata=metadata, result=blocked)
                self._record_external_outcome(step=step, metadata=metadata, result=blocked)
                if retryable_preflight:
                    decision = self.recovery.decide(
                        step,
                        blocked,
                        attempt,
                        metadata=metadata,
                        policy_profile=policy_profile,
                        recovery_profile=recovery_profile,
                        verification_strictness=verification_strictness,
                    )
                    if decision.retry:
                        repair_patch, repair_details = self._derive_external_repair_patch_from_payload(
                            step=step,
                            payload=preflight if isinstance(preflight, dict) else {},
                            error=preflight_message,
                            metadata=metadata,
                        )
                        if repair_patch and isinstance(step.args, dict):
                            step.args.update(repair_patch)
                            repair_payload = {
                                "source": "preflight",
                                "applied_patch": dict(repair_patch),
                                "details": repair_details,
                            }
                            blocked.evidence["external_repair"] = repair_payload
                            last_external_repair = dict(repair_payload)
                            self.telemetry.emit(
                                "step.external_repair_patch_applied",
                                {
                                    "step_id": step.step_id,
                                    "action": step.action,
                                    "attempt": attempt,
                                    "source": "preflight",
                                    "patch_keys": list(repair_patch.keys())[:10],
                                },
                            )
                        remediation_payload = await self._run_external_remediation_actions(
                            step=step,
                            payloads=[preflight if isinstance(preflight, dict) else {}],
                            metadata=metadata,
                            source=source,
                            attempt=attempt,
                        )
                        recovered_preflight = False
                        if isinstance(remediation_payload, dict):
                            remediation_status = str(remediation_payload.get("status", "")).strip().lower()
                            if remediation_status and remediation_status != "skip":
                                blocked.evidence["external_remediation"] = remediation_payload
                                last_external_remediation = dict(remediation_payload)
                            if remediation_status == "success":
                                rechecked_preflight = self._external_preflight(step=step, metadata=metadata)
                                rechecked_status = (
                                    str(rechecked_preflight.get("status", "")).strip().lower()
                                    if isinstance(rechecked_preflight, dict)
                                    else ""
                                )
                                if rechecked_status not in {"blocked", "error"}:
                                    recovered_preflight = True
                                    self.telemetry.emit(
                                        "step.external_preflight_recovered",
                                        {
                                            "step_id": step.step_id,
                                            "action": step.action,
                                            "attempt": attempt,
                                            "rechecked_status": rechecked_status or "ok",
                                        },
                                    )
                        if recovered_preflight:
                            continue
                        preflight_retry_after = 0.0
                        if isinstance(preflight, dict):
                            try:
                                preflight_retry_after = float(preflight.get("retry_after_s", 0.0) or 0.0)
                            except Exception:
                                preflight_retry_after = 0.0
                        retry_contract_delay = 0.0
                        if isinstance(retry_contract, dict):
                            retry_contract_delay = self._retry_contract_delay_floor(retry_contract=retry_contract)
                        sleep_s = max(
                            float(decision.delay_s),
                            min(60.0, max(0.0, preflight_retry_after)),
                            min(75.0, max(0.0, retry_contract_delay)),
                        )
                        retry_history.append(
                            {
                                "attempt": attempt,
                                "delay_s": float(sleep_s),
                                "reason": decision.reason or "External preflight retry",
                                "category": failure_category or decision.category or "transient",
                            }
                        )
                        self.telemetry.emit(
                            "step.external_preflight_retry",
                            {
                                "step_id": step.step_id,
                                "action": step.action,
                                "attempt": attempt,
                                "delay_s": float(sleep_s),
                                "category": failure_category or decision.category or "transient",
                                "reason": decision.reason or "External preflight retry",
                            },
                        )
                        attempt += 1
                        if interrupt_check is not None and interrupt_check():
                            interrupted = self._interrupted_result(
                                step.action,
                                self._resolve_interrupt_reason(interrupt_reason, interrupt_reason_provider),
                                attempt=attempt,
                            )
                            self._attach_recovery_evidence(
                                interrupted,
                                step=step,
                                attempt=attempt,
                                retry_history=retry_history,
                                last_category=failure_category,
                                last_reason=preflight_message,
                            )
                            self._record_desktop_anchor_outcome(step=step, metadata=metadata, result=interrupted)
                            self._record_external_outcome(step=step, metadata=metadata, result=interrupted)
                            return interrupted
                        await asyncio.sleep(sleep_s)
                        continue
                return blocked
            if isinstance(preflight, dict):
                args_patch = preflight.get("args_patch")
                if isinstance(args_patch, dict) and args_patch and isinstance(step.args, dict):
                    step.args.update(args_patch)
                    preflight["applied_args_patch"] = dict(args_patch)
                    routing = preflight.get("provider_routing")
                    selected_provider = ""
                    if isinstance(routing, dict):
                        selected_provider = str(routing.get("selected_provider", "")).strip().lower()
                    self.telemetry.emit(
                        "external_reliability.provider_routed",
                        {
                            "step_id": step.step_id,
                            "action": step.action,
                            "selected_provider": selected_provider,
                            "args_patch": dict(args_patch),
                            "strategy": str(routing.get("strategy", "")) if isinstance(routing, dict) else "",
                        },
                    )
                    circuit_scope = self._resolve_circuit_scope(step=step, metadata=metadata)
                retry_hint = preflight.get("retry_hint")
                retry_contract = preflight.get("retry_contract")
                if (isinstance(retry_hint, dict) and retry_hint) or (isinstance(retry_contract, dict) and retry_contract):
                    self._apply_retry_hint(
                        step=step,
                        retry_hint=retry_hint if isinstance(retry_hint, dict) else {},
                        retry_contract=retry_contract if isinstance(retry_contract, dict) else {},
                    )
                if isinstance(retry_contract, dict) and retry_contract:
                    strategy_overrides = self._apply_retry_contract_runtime_strategy(
                        step=step,
                        retry_contract=retry_contract,
                        metadata=metadata,
                        policy_profile=policy_profile,
                        recovery_profile=recovery_profile,
                        verification_strictness=verification_strictness,
                    )
                    if strategy_overrides:
                        policy_profile = str(strategy_overrides.get("policy_profile", policy_profile)).strip().lower()
                        recovery_profile = str(strategy_overrides.get("recovery_profile", recovery_profile)).strip().lower()
                        verification_strictness = str(
                            strategy_overrides.get("verification_strictness", verification_strictness)
                        ).strip().lower()

            anchor = await self._prepare_desktop_anchor(step=step, source=source, metadata=metadata, attempt=attempt)
            anchor_result = anchor.get("result") if isinstance(anchor, dict) else None
            anchor_evidence = anchor.get("evidence", {}) if isinstance(anchor, dict) else {}
            if isinstance(anchor_result, ActionResult):
                self._attach_recovery_evidence(
                    anchor_result,
                    step=step,
                    attempt=attempt,
                    retry_history=retry_history,
                    last_category=str(anchor.get("failure_category", "non_retryable")),
                    last_reason=anchor_result.error or "Desktop anchor precondition failed.",
                )
                self._record_desktop_anchor_outcome(step=step, metadata=metadata, result=anchor_result)
                self._record_external_outcome(step=step, metadata=metadata, result=anchor_result)
                return anchor_result

            if self.rollback_manager is not None:
                try:
                    profile_payload = self.rollback_manager.rollback_profile(
                        action=step.action,
                        args=step.args if isinstance(step.args, dict) else {},
                    )
                    if isinstance(profile_payload, dict):
                        rollback_profile = profile_payload
                except Exception:
                    rollback_profile = {}

            branch_blocked, branch_context = self._enforce_external_branch_guard(
                step=step,
                metadata=metadata,
                rollback_profile=rollback_profile,
                attempt=attempt,
            )
            if branch_blocked is not None:
                if not isinstance(branch_blocked.evidence, dict):
                    branch_blocked.evidence = {}
                if branch_context:
                    branch_blocked.evidence["rollback_branch"] = branch_context
                if rollback_profile:
                    branch_blocked.evidence["rollback_profile"] = rollback_profile
                self._attach_recovery_evidence(
                    branch_blocked,
                    step=step,
                    attempt=attempt,
                    retry_history=retry_history,
                    last_category="non_retryable",
                    last_reason=branch_blocked.error or "External branch guard blocked action.",
                )
                self._record_circuit_breaker(
                    action=step.action,
                    status=branch_blocked.status,
                    failure_category="non_retryable",
                    error=branch_blocked.error or "",
                    scope=circuit_scope,
                )
                self._record_desktop_anchor_outcome(step=step, metadata=metadata, result=branch_blocked)
                self._record_external_outcome(step=step, metadata=metadata, result=branch_blocked)
                return branch_blocked

            simulation_blocked, external_mutation_simulation = await self._run_external_mutation_simulation(
                step=step,
                source=source,
                metadata=metadata,
                attempt=attempt,
                rollback_profile=rollback_profile,
                branch_context=branch_context,
            )
            if simulation_blocked is not None:
                self._attach_recovery_evidence(
                    simulation_blocked,
                    step=step,
                    attempt=attempt,
                    retry_history=retry_history,
                    last_category="non_retryable",
                    last_reason=simulation_blocked.error or "External mutation simulation failed.",
                )
                self._record_circuit_breaker(
                    action=step.action,
                    status=simulation_blocked.status,
                    failure_category="non_retryable",
                    error=simulation_blocked.error or "",
                    scope=circuit_scope,
                )
                self._record_desktop_anchor_outcome(step=step, metadata=metadata, result=simulation_blocked)
                self._record_external_outcome(step=step, metadata=metadata, result=simulation_blocked)
                return simulation_blocked
            if external_mutation_simulation:
                circuit_scope = self._resolve_circuit_scope(step=step, metadata=metadata)

            voice_runtime_override = self._resolve_voice_runtime_request_override(
                step=step,
                source=source,
                metadata=metadata,
            )
            request_action = str(voice_runtime_override.get("action", "") or step.action).strip() or step.action
            override_args = voice_runtime_override.get("args") if voice_runtime_override else None
            request_args = dict(override_args) if isinstance(override_args, dict) else step.args
            request = ActionRequest(action=request_action, args=request_args, source=source, metadata=metadata)
            pre_state: Dict[str, Any] = {}
            if self.rollback_manager is not None:
                try:
                    pre_state = self.rollback_manager.capture_pre_state(action=request.action, args=request.args)
                except Exception:
                    pre_state = {}

            approval_result = self._enforce_approval(request)
            if approval_result is not None:
                approval_result.attempt = attempt
                if not isinstance(approval_result.evidence, dict):
                    approval_result.evidence = {}
                if rollback_profile:
                    approval_result.evidence["rollback_profile"] = rollback_profile
                if branch_context:
                    approval_result.evidence["rollback_branch"] = branch_context
                if external_mutation_simulation:
                    approval_result.evidence["external_mutation_simulation"] = external_mutation_simulation
                self._record_circuit_breaker(
                    action=step.action,
                    status=approval_result.status,
                    failure_category="non_retryable",
                    error=approval_result.error or "",
                    scope=circuit_scope,
                )
                self._attach_recovery_evidence(
                    approval_result,
                    step=step,
                    attempt=attempt,
                    retry_history=retry_history,
                    last_category="non_retryable",
                    last_reason=approval_result.error or "Approval required.",
                )
                self._record_desktop_anchor_outcome(step=step, metadata=metadata, result=approval_result)
                self._record_external_outcome(step=step, metadata=metadata, result=approval_result)
                return approval_result

            allowed, reason = self.policy_guard.authorize(request)
            if not allowed:
                blocked = ActionResult(action=step.action, status="blocked", error=reason, attempt=attempt)
                if rollback_profile:
                    blocked.evidence["rollback_profile"] = rollback_profile
                if branch_context:
                    blocked.evidence["rollback_branch"] = branch_context
                if external_mutation_simulation:
                    blocked.evidence["external_mutation_simulation"] = external_mutation_simulation
                self._record_circuit_breaker(
                    action=step.action,
                    status="blocked",
                    failure_category="non_retryable",
                    error=reason,
                    scope=circuit_scope,
                )
                self._attach_recovery_evidence(
                    blocked,
                    step=step,
                    attempt=attempt,
                    retry_history=retry_history,
                    last_category="non_retryable",
                    last_reason=reason,
                )
                self._record_desktop_anchor_outcome(step=step, metadata=metadata, result=blocked)
                self._record_external_outcome(step=step, metadata=metadata, result=blocked)
                return blocked

            result = await self.registry.execute(request, timeout_s=step.timeout_s)
            result.attempt = attempt
            if not isinstance(result.evidence, dict):
                result.evidence = {}
            if voice_runtime_override:
                result.evidence["voice_execution_redirect"] = {
                    "original_action": str(voice_runtime_override.get("original_action", "")).strip(),
                    "executed_action": request.action,
                    "reason_code": str(voice_runtime_override.get("reason_code", "")).strip().lower(),
                    "reason": str(voice_runtime_override.get("reason", "")).strip(),
                }
                if isinstance(voice_runtime_override.get("planner_followup_contract", {}), dict):
                    result.evidence["voice_execution_redirect"]["planner_followup_contract"] = dict(
                        voice_runtime_override.get("planner_followup_contract", {})
                    )
            if not circuit_scope and isinstance(result.output, dict):
                scope_from_output = self._resolve_circuit_scope_from_output(result.output)
                if scope_from_output:
                    circuit_scope = scope_from_output
            result.evidence.setdefault("step_id", step.step_id)
            result.evidence.setdefault(
                "request",
                {
                    "action": request.action,
                    "args": request.args if isinstance(request.args, dict) else {},
                    "source": source,
                },
            )
            if isinstance(preflight, dict) and preflight:
                result.evidence["external_reliability_preflight"] = preflight
            if isinstance(anchor_evidence, dict) and anchor_evidence:
                result.evidence["desktop_anchor"] = anchor_evidence
            if rollback_profile:
                result.evidence["rollback_profile"] = rollback_profile
            if branch_context:
                result.evidence["rollback_branch"] = branch_context
            if external_mutation_simulation:
                result.evidence["external_mutation_simulation"] = external_mutation_simulation
            if self.rollback_manager is not None and result.status == "success":
                try:
                    rollback_entry = self.rollback_manager.record_success(
                        action=step.action,
                        args=step.args if isinstance(step.args, dict) else {},
                        result=result,
                        source=source,
                        goal_id=str(metadata.get("__goal_id", "")).strip(),
                        metadata={"step_id": step.step_id, "attempt": attempt},
                        pre_state=pre_state,
                    )
                    if isinstance(rollback_entry, dict):
                        result.evidence["rollback"] = {
                            "rollback_id": rollback_entry.get("rollback_id", ""),
                            "goal_id": rollback_entry.get("goal_id", ""),
                        }
                except Exception:
                    pass

            verify_context: Dict[str, Any] = {}
            policy_context: Dict[str, Any] = {}
            if policy_profile:
                verify_context["policy_profile"] = policy_profile
                policy_context["profile"] = policy_profile

            strictness = str(metadata.get("verification_strictness", "")).strip().lower()
            if strictness in {"off", "standard", "strict"}:
                policy_context["strictness"] = strictness
            verification_pressure = self._coerce_float(
                metadata.get("runtime_verification_pressure", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            if verification_pressure > 0.0:
                verify_context["verification_pressure"] = verification_pressure
                current_policy_strictness = str(policy_context.get("strictness", "standard")).strip().lower() or "standard"
                if verification_pressure >= 0.66 and current_policy_strictness != "strict":
                    policy_context["strictness"] = "strict"
                elif verification_pressure >= 0.38 and current_policy_strictness == "off":
                    policy_context["strictness"] = "standard"
            if policy_context:
                verify_context["policy"] = policy_context

            desktop_context = self._capture_desktop_state(
                step=step,
                source=source,
                metadata=metadata,
                result=result,
                pre_hash=pre_desktop_hash,
                pre_snapshot=pre_desktop_context,
            )
            if desktop_context:
                verify_context["desktop_state"] = desktop_context
                state_hash = str(desktop_context.get("state_hash", "")).strip()
                if state_hash:
                    metadata["__desktop_post_state_hash"] = state_hash

            confirm_results = await self._run_confirm_checks(step=step, result=result, source=source, attempt=attempt)
            if confirm_results:
                primary_confirm = confirm_results[0]
                verify_context["confirm"] = primary_confirm.output
                verify_context["confirm_meta"] = {"status": primary_confirm.status, "error": primary_confirm.error}
                verify_context["confirm_results"] = [
                    {
                        "action": row.action,
                        "status": row.status,
                        "error": row.error,
                        "output": row.output if isinstance(row.output, dict) else {},
                    }
                    for row in confirm_results
                ]
                result.evidence["confirm_action"] = {
                    "action": primary_confirm.action,
                    "status": primary_confirm.status,
                    "error": primary_confirm.error,
                    "output": primary_confirm.output,
                }
                result.evidence["confirm_actions"] = list(verify_context["confirm_results"])

                policy = self._resolve_confirm_policy(step.verify if isinstance(step.verify, dict) else {}, confirm_results)
                gated_results = policy.get("gated_results", [])
                if isinstance(gated_results, list):
                    success_count = sum(1 for row in gated_results if isinstance(row, ActionResult) and row.status == "success")
                    total_count = len(gated_results)
                else:
                    success_count = 0
                    total_count = 0
                min_success = int(policy.get("min_success", 0) or 0)
                mode = str(policy.get("mode", "all")).strip().lower() or "all"
                required = bool(policy.get("required", True))
                policy_satisfied = self._confirm_policy_satisfied(
                    mode=mode,
                    success_count=success_count,
                    total_count=total_count,
                    min_success=min_success,
                    required=required,
                )
                verify_context["confirm_policy"] = {
                    "mode": mode,
                    "required": required,
                    "min_success": min_success,
                    "success_count": success_count,
                    "total_count": total_count,
                    "satisfied": policy_satisfied,
                }
                if not policy_satisfied:
                    result.status = "failed"
                    result.error = (
                        f"Verification confirm policy failed: mode={mode}, "
                        f"success={success_count}/{total_count}, min_success={min_success}."
                    )

            verified, verify_reason = self.verifier.verify(step, result, context=verify_context)
            if verified:
                if last_external_repair and not result.evidence.get("external_repair"):
                    result.evidence["external_repair"] = dict(last_external_repair)
                if last_external_remediation and not result.evidence.get("external_remediation"):
                    result.evidence["external_remediation"] = dict(last_external_remediation)
                self._record_circuit_breaker(action=step.action, status="success", scope=circuit_scope)
                self._attach_recovery_evidence(
                    result,
                    step=step,
                    attempt=attempt,
                    retry_history=retry_history,
                    last_category=last_recovery_category,
                    last_reason=last_recovery_reason,
                )
                self._record_desktop_anchor_outcome(step=step, metadata=metadata, result=result)
                self._record_external_outcome(step=step, metadata=metadata, result=result)
                return result

            if result.status == "success":
                # Verification failed despite successful call.
                result.status = "failed"
                result.error = verify_reason

            if self._should_attempt_desktop_fallback(step=step, result=result, metadata=metadata):
                result = await self._run_desktop_diagnostic_fallback(
                    step=step,
                    result=result,
                    source=source,
                    metadata=metadata,
                    attempt=attempt,
                )

            self._record_desktop_guardrail_feedback(
                step=step,
                metadata=metadata,
                result=result,
                verify_context=verify_context,
                verify_reason=verify_reason,
                attempt=attempt,
            )

            decision = self.recovery.decide(
                step,
                result,
                attempt,
                metadata=metadata,
                policy_profile=policy_profile,
                recovery_profile=recovery_profile,
                verification_strictness=verification_strictness,
            )
            if decision.category:
                last_recovery_category = str(decision.category).strip().lower()
            if decision.reason:
                last_recovery_reason = str(decision.reason).strip()
            if not decision.retry:
                if not result.error:
                    result.error = decision.reason
                if last_external_repair and not result.evidence.get("external_repair"):
                    result.evidence["external_repair"] = dict(last_external_repair)
                if last_external_remediation and not result.evidence.get("external_remediation"):
                    result.evidence["external_remediation"] = dict(last_external_remediation)
                self._record_circuit_breaker(
                    action=step.action,
                    status=result.status,
                    failure_category=last_recovery_category or self._classify_error_category(result.error or ""),
                    error=result.error or "",
                    scope=circuit_scope,
                )
                self._attach_recovery_evidence(
                    result,
                    step=step,
                    attempt=attempt,
                    retry_history=retry_history,
                    last_category=last_recovery_category,
                    last_reason=last_recovery_reason,
                )
                self._record_desktop_anchor_outcome(step=step, metadata=metadata, result=result)
                self._record_external_outcome(step=step, metadata=metadata, result=result)
                return result

            repair_patch, repair_details = self._derive_external_repair_patch(
                step=step,
                result=result,
                metadata=metadata,
            )
            if repair_patch and isinstance(step.args, dict):
                step.args.update(repair_patch)
                repair_payload = {
                    "source": "retry_loop",
                    "applied_patch": dict(repair_patch),
                    "details": repair_details,
                }
                result.evidence["external_repair"] = repair_payload
                last_external_repair = dict(repair_payload)
                self.telemetry.emit(
                    "step.external_repair_patch_applied",
                    {
                        "step_id": step.step_id,
                        "action": step.action,
                        "attempt": attempt,
                        "source": "retry_loop",
                        "patch_keys": list(repair_patch.keys())[:10],
                    },
                )

            remediation_payload = await self._run_external_remediation_actions(
                step=step,
                payloads=[
                    result.output if isinstance(result.output, dict) else {},
                    result.evidence.get("external_reliability_preflight", {})
                    if isinstance(result.evidence, dict)
                    else {},
                    result.evidence.get("external_reliability", {})
                    if isinstance(result.evidence, dict)
                    else {},
                ],
                metadata=metadata,
                source=source,
                attempt=attempt,
            )
            if isinstance(remediation_payload, dict):
                remediation_status = str(remediation_payload.get("status", "")).strip().lower()
                if remediation_status and remediation_status != "skip":
                    result.evidence["external_remediation"] = remediation_payload
                    last_external_remediation = dict(remediation_payload)

            retry_history.append(
                {
                    "attempt": attempt,
                    "delay_s": float(decision.delay_s),
                    "reason": decision.reason,
                    "category": decision.category,
                }
            )
            self._record_circuit_breaker(
                action=step.action,
                status=result.status,
                failure_category=decision.category or self._classify_error_category(result.error or ""),
                error=result.error or "",
                scope=circuit_scope,
            )
            self.telemetry.emit(
                "step.retry",
                {
                    "step_id": step.step_id,
                    "attempt": attempt,
                    "delay_s": decision.delay_s,
                    "reason": decision.reason,
                    "category": decision.category,
                    "profile": getattr(decision, "profile", ""),
                },
            )
            self._record_desktop_anchor_outcome(step=step, metadata=metadata, result=result)
            self._record_external_outcome(step=step, metadata=metadata, result=result)
            attempt += 1
            if interrupt_check is not None and interrupt_check():
                interrupted = self._interrupted_result(
                    step.action,
                    self._resolve_interrupt_reason(interrupt_reason, interrupt_reason_provider),
                    attempt=attempt,
                )
                self._attach_recovery_evidence(
                    interrupted,
                    step=step,
                    attempt=attempt,
                    retry_history=retry_history,
                    last_category=last_recovery_category,
                    last_reason=last_recovery_reason,
                )
                self._record_desktop_anchor_outcome(step=step, metadata=metadata, result=interrupted)
                self._record_external_outcome(step=step, metadata=metadata, result=interrupted)
                return interrupted
            await asyncio.sleep(decision.delay_s)

    def _read_pre_desktop_context(self, *, step: PlanStep) -> tuple[str, Dict[str, Any]]:
        if self.desktop_state is None:
            return ("", {})
        try:
            latest_state = self.desktop_state.latest()
            if not isinstance(latest_state, dict):
                return ("", {})
            pre_desktop_hash = str(latest_state.get("state_hash", "")).strip()
            normalized_latest = latest_state.get("normalized", {})
            normalized = normalized_latest if isinstance(normalized_latest, dict) else {}
            context = {
                "window_title": str(self._resolve_path(normalized, "window.title") or "").strip(),
                "window_hwnd": self._resolve_path(normalized, "window.hwnd"),
                "app": str(self._resolve_path(normalized, "app") or "").strip(),
            }
            return (pre_desktop_hash, context)
        except Exception as exc:  # noqa: BLE001
            self.telemetry.emit(
                "step.desktop_state_read_error",
                {
                    "step_id": step.step_id,
                    "action": step.action,
                    "message": str(exc),
                },
            )
            return ("", {})

    def _capture_desktop_state(
        self,
        *,
        step: PlanStep,
        source: str,
        metadata: Dict[str, Any],
        result: ActionResult,
        pre_hash: str,
        pre_snapshot: Dict[str, Any] | None = None,
    ) -> Optional[Dict[str, Any]]:
        if self.desktop_state is None:
            return None
        output_payload = result.output if isinstance(result.output, dict) else {}
        prior = pre_snapshot if isinstance(pre_snapshot, dict) else {}
        pre_window_title = str(prior.get("window_title", "")).strip()
        pre_window_hwnd = prior.get("window_hwnd")
        pre_app = str(prior.get("app", "")).strip()
        try:
            state_row = self.desktop_state.observe(
                action=step.action,
                output=output_payload,
                goal_id=str(metadata.get("__goal_id", "")).strip(),
                plan_id=str(metadata.get("__plan_id", "")).strip(),
                step_id=str(step.step_id or "").strip(),
                source=source,
            )
            if not isinstance(state_row, dict):
                return None

            state_hash = str(state_row.get("state_hash", "")).strip()
            previous_hash = str(state_row.get("previous_hash", "")).strip()
            changed_paths = state_row.get("changed_paths", [])
            changed_paths_list = [str(item) for item in changed_paths if str(item).strip()] if isinstance(changed_paths, list) else []
            baseline_hash = str(pre_hash or previous_hash).strip()
            state_changed = bool(baseline_hash and state_hash and baseline_hash != state_hash)

            change_count = len(changed_paths_list)
            diff_payload: Dict[str, Any] = {}
            if baseline_hash and state_hash:
                diff_payload = self.desktop_state.diff(from_hash=baseline_hash, to_hash=state_hash)
                if isinstance(diff_payload, dict) and diff_payload.get("status") == "success":
                    diff_paths = diff_payload.get("changed_paths", [])
                    if isinstance(diff_paths, list):
                        changed_paths_list = [str(item) for item in diff_paths if str(item).strip()]
                        change_count = len(changed_paths_list)

            latest_payload = self.desktop_state.latest()
            latest_normalized = {}
            if isinstance(latest_payload, dict):
                normalized_raw = latest_payload.get("normalized", {})
                latest_normalized = normalized_raw if isinstance(normalized_raw, dict) else {}
            post_window_title = str(self._resolve_path(latest_normalized, "window.title") or "").strip()
            post_window_hwnd = self._resolve_path(latest_normalized, "window.hwnd")
            post_app = str(self._resolve_path(latest_normalized, "app") or "").strip()
            window_transition = False
            app_transition = False
            if pre_window_title and post_window_title and pre_window_title != post_window_title:
                window_transition = True
            if pre_window_hwnd is not None and post_window_hwnd is not None and str(pre_window_hwnd) != str(post_window_hwnd):
                window_transition = True
            if pre_app and post_app and pre_app != post_app:
                app_transition = True

            desktop_context: Dict[str, Any] = {
                "state_hash": state_hash,
                "previous_hash": previous_hash,
                "pre_hash": baseline_hash,
                "state_changed": state_changed,
                "change_count": int(change_count),
                "changed_paths": changed_paths_list[:128],
                "window_transition": bool(window_transition),
                "app_transition": bool(app_transition),
                "window_title_before": pre_window_title,
                "window_title_after": post_window_title,
                "window_hwnd_before": pre_window_hwnd,
                "window_hwnd_after": post_window_hwnd,
                "app_before": pre_app,
                "app_after": post_app,
            }
            if isinstance(diff_payload, dict):
                desktop_context["diff_status"] = str(diff_payload.get("status", "")).strip()
                if diff_payload.get("status") == "error":
                    desktop_context["diff_error"] = str(diff_payload.get("message", "")).strip()

            result.evidence["desktop_state"] = desktop_context
            if changed_paths_list:
                self.telemetry.emit(
                    "step.desktop_state_updated",
                    {
                        "step_id": step.step_id,
                        "action": step.action,
                        "state_hash": state_hash,
                        "change_count": len(changed_paths_list),
                        "changed_paths": changed_paths_list[:12],
                    },
                )
            if window_transition or app_transition:
                self.telemetry.emit(
                    "step.desktop_window_transition",
                    {
                        "step_id": step.step_id,
                        "action": step.action,
                        "window_transition": bool(window_transition),
                        "app_transition": bool(app_transition),
                        "window_title_before": pre_window_title,
                        "window_title_after": post_window_title,
                        "app_before": pre_app,
                        "app_after": post_app,
                    },
                )
            return desktop_context
        except Exception as exc:  # noqa: BLE001
            warnings = result.evidence.setdefault("nonfatal_warnings", [])
            if isinstance(warnings, list):
                warnings.append(f"desktop_state_capture_failed: {exc}")
            self.telemetry.emit(
                "step.desktop_state_capture_error",
                {
                    "step_id": step.step_id,
                    "action": step.action,
                    "message": str(exc),
                },
            )
            return None

    def _record_desktop_guardrail_feedback(
        self,
        *,
        step: PlanStep,
        metadata: Dict[str, Any],
        result: ActionResult,
        verify_context: Dict[str, Any],
        verify_reason: str,
        attempt: int,
    ) -> None:
        if not isinstance(metadata, dict):
            return
        clean_action = str(step.action or "").strip().lower()
        desktop_context = verify_context.get("desktop_state", {}) if isinstance(verify_context, dict) else {}
        desktop_state = desktop_context if isinstance(desktop_context, dict) else {}
        if clean_action not in {"computer_click_target", "computer_click_text", "accessibility_invoke_element"} and not desktop_state:
            return

        error_text = str(result.error or "").strip()
        verify_text = str(verify_reason or "").strip()
        lowered_error = error_text.lower()
        lowered_verify = verify_text.lower()
        reason_tags: List[str] = []

        if "confirm policy failed" in lowered_error:
            reason_tags.append("confirm_policy_failed")
        if "verification" in lowered_error or "verification" in lowered_verify:
            reason_tags.append("verification_failed")
        if "desktop anchor" in lowered_error:
            reason_tags.append("anchor_precondition_failed")
        if "fallback probe chain failed" in lowered_error:
            reason_tags.append("anchor_fallback_failed")

        state_changed = self._coerce_bool(desktop_state.get("state_changed", False), default=False)
        change_count = self._coerce_int(desktop_state.get("change_count", 0), minimum=0, maximum=10_000, default=0)
        window_transition = self._coerce_bool(desktop_state.get("window_transition", False), default=False)
        app_transition = self._coerce_bool(desktop_state.get("app_transition", False), default=False)
        if window_transition:
            reason_tags.append("window_transition")
        if app_transition:
            reason_tags.append("app_transition")
        if clean_action in {"computer_click_target", "computer_click_text", "accessibility_invoke_element"} and not state_changed and change_count <= 0:
            reason_tags.append("no_state_change")
        confirm_meta = verify_context.get("confirm_meta", {}) if isinstance(verify_context, dict) else {}
        if isinstance(confirm_meta, dict) and str(confirm_meta.get("status", "")).strip().lower() in {"failed", "blocked"}:
            reason_tags.append("confirm_check_failed")
        changed_paths_raw = desktop_state.get("changed_paths", []) if isinstance(desktop_state, dict) else []
        changed_paths = [
            str(item).strip().lower()
            for item in changed_paths_raw
            if str(item).strip()
        ][:24] if isinstance(changed_paths_raw, list) else []

        reason_tags = sorted({str(tag).strip().lower() for tag in reason_tags if str(tag).strip()})
        if not reason_tags:
            return

        severity = "soft"
        if any(tag in reason_tags for tag in ("window_transition", "app_transition", "anchor_precondition_failed")):
            severity = "hard"
        elif "confirm_policy_failed" in reason_tags and "no_state_change" in reason_tags:
            severity = "hard"

        row = {
            "action": clean_action,
            "attempt": max(1, int(attempt)),
            "status": str(result.status or "").strip().lower(),
            "severity": severity,
            "reason_tags": reason_tags[:12],
            "error": error_text[:360],
            "verify_reason": verify_text[:360],
            "state_hash": str(desktop_state.get("state_hash", "")).strip().lower(),
            "pre_hash": str(desktop_state.get("pre_hash", "")).strip().lower(),
            "window_before": str(desktop_state.get("window_title_before", "")).strip().lower(),
            "window_after": str(desktop_state.get("window_title_after", "")).strip().lower(),
            "app_before": str(desktop_state.get("app_before", "")).strip().lower(),
            "app_after": str(desktop_state.get("app_after", "")).strip().lower(),
            "changed_paths": changed_paths,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }
        pre_hash = str(row.get("pre_hash", "")).strip().lower()
        post_hash = str(row.get("state_hash", "")).strip().lower()
        transition_signature = "|".join(
            [
                f"{pre_hash[:16]}->{post_hash[:16]}",
                f"{str(row.get('window_before', ''))[:72]}->{str(row.get('window_after', ''))[:72]}",
                f"{str(row.get('app_before', ''))[:48]}->{str(row.get('app_after', ''))[:48]}",
            ]
        )
        row["transition_signature"] = transition_signature
        existing = metadata.get("__desktop_guardrail_feedback")
        rows = existing if isinstance(existing, list) else []
        rows.append(row)
        metadata["__desktop_guardrail_feedback"] = rows[-18:]

    @classmethod
    def _is_external_mutation_action(cls, action: str) -> bool:
        clean_action = str(action or "").strip().lower()
        return clean_action in cls._EXTERNAL_MUTATION_ACTIONS

    @classmethod
    def _is_high_impact_external_action(cls, action: str) -> bool:
        clean_action = str(action or "").strip().lower()
        return clean_action in cls._EXTERNAL_HIGH_IMPACT_ACTIONS

    def _should_run_external_mutation_simulation(
        self,
        *,
        step: PlanStep,
        metadata: Dict[str, Any],
    ) -> bool:
        if not self.external_mutation_simulation_enabled:
            return False
        clean_action = str(step.action or "").strip().lower()
        if not self._is_external_mutation_action(clean_action):
            return False
        if self.external_mutation_simulation_high_impact_only and not self._is_high_impact_external_action(clean_action):
            return False
        if self._coerce_bool(metadata.get("skip_external_mutation_simulation", False), default=False):
            return False
        if not self._coerce_bool(metadata.get("external_mutation_simulation_enabled", True), default=True):
            return False
        if self._coerce_bool(metadata.get("__external_mutation_simulation", False), default=False):
            return False
        args = step.args if isinstance(step.args, dict) else {}
        if self._coerce_bool(args.get("dry_run", False), default=False):
            return False
        return True

    @staticmethod
    def _extract_args_patch_from_simulation(output: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(output, dict):
            return {}
        direct_patch = output.get("args_patch")
        if isinstance(direct_patch, dict) and direct_patch:
            return dict(direct_patch)
        simulation = output.get("simulation")
        if isinstance(simulation, dict):
            for key in ("args_patch", "recommended_args_patch"):
                row = simulation.get(key)
                if isinstance(row, dict) and row:
                    return dict(row)
        return {}

    @staticmethod
    def _simulation_failure_should_block(*, status: str, error: str, output: Dict[str, Any]) -> bool:
        clean_status = str(status or "").strip().lower()
        if clean_status == "blocked":
            return True
        payload = output if isinstance(output, dict) else {}
        if bool(payload.get("dry_run", False)):
            return True
        if isinstance(payload.get("simulation"), dict):
            return True
        remediation = payload.get("remediation_hints")
        if isinstance(remediation, list) and remediation:
            return True
        lowered = str(error or "").strip().lower()
        if not lowered:
            return False
        if any(
            token in lowered
            for token in (
                "contract",
                "invalid",
                "missing required",
                "unsupported provider",
                "credentials",
                "oauth",
                "auth",
                "dry-run",
                "dry run",
                "preflight",
                "non-reversible",
            )
        ):
            return True
        if any(
            token in lowered
            for token in (
                "timed out",
                "timeout",
                "temporar",
                "unavailable",
                "connection",
                "try again",
                "service busy",
            )
        ):
            return False
        return False

    @staticmethod
    def _is_external_or_oauth_action(action: str) -> bool:
        clean_action = str(action or "").strip().lower()
        return clean_action.startswith("external_") or clean_action.startswith("oauth_token_")

    def _derive_external_repair_patch(
        self,
        *,
        step: PlanStep,
        result: ActionResult,
        metadata: Dict[str, Any],
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        clean_action = str(step.action or "").strip().lower()
        if not self._is_external_or_oauth_action(clean_action):
            return ({}, {})
        error_text = str(result.error or "").strip()
        output = result.output if isinstance(result.output, dict) else {}
        evidence = result.evidence if isinstance(result.evidence, dict) else {}

        patch: Dict[str, Any] = {}
        diagnostics: List[Dict[str, Any]] = []
        payloads: List[Dict[str, Any]] = []
        for candidate in (
            output,
            evidence.get("external_reliability_preflight", {}),
            evidence.get("external_reliability", {}),
            evidence.get("external_mutation_simulation", {}),
        ):
            if isinstance(candidate, dict) and candidate:
                payloads.append(candidate)

        for payload in payloads:
            current_patch, details = self._derive_external_repair_patch_from_payload(
                step=step,
                payload=payload,
                error=error_text,
                metadata=metadata,
            )
            if current_patch:
                for key, value in current_patch.items():
                    if key not in patch:
                        patch[key] = value
            if details:
                diagnostics.append(details)

        memory_patch, memory_details = self._memory_repair_patch(step=step, metadata=metadata)
        if memory_patch:
            for key, value in memory_patch.items():
                if key not in patch:
                    patch[key] = value
        if memory_details:
            diagnostics.append({"source": "runtime_memory", **memory_details})

        safe_patch = self._sanitize_external_repair_patch(step=step, patch=patch)
        details_payload: Dict[str, Any] = {
            "diagnostics": diagnostics[:8],
            "original_error": error_text[:400],
        }
        if patch and not safe_patch:
            details_payload["dropped_patch_keys"] = list(patch.keys())[:12]
        return (safe_patch, details_payload)

    def _derive_external_repair_patch_from_payload(
        self,
        *,
        step: PlanStep,
        payload: Dict[str, Any],
        error: str,
        metadata: Dict[str, Any],
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        clean_action = str(step.action or "").strip().lower()
        if not self._is_external_or_oauth_action(clean_action):
            return ({}, {})
        args = step.args if isinstance(step.args, dict) else {}
        patch: Dict[str, Any] = {}
        details: Dict[str, Any] = {"source": "payload", "signals": []}

        direct_args_patch = payload.get("args_patch")
        if isinstance(direct_args_patch, dict) and direct_args_patch:
            patch.update(dict(direct_args_patch))
            details["signals"].append("args_patch")

        simulation = payload.get("simulation")
        simulation_payload = simulation if isinstance(simulation, dict) else {}
        for key in ("recommended_args_patch", "args_patch"):
            row = simulation_payload.get(key)
            if isinstance(row, dict) and row:
                patch.update(dict(row))
                details["signals"].append(f"simulation.{key}")
                break

        route_payload = payload.get("provider_routing")
        route = route_payload if isinstance(route_payload, dict) else {}
        selected_provider = str(route.get("selected_provider", "")).strip().lower()
        if selected_provider and str(args.get("provider", "")).strip().lower() in {"", "auto"}:
            patch["provider"] = selected_provider
            details["signals"].append("provider_routing.selected_provider")

        fallback_payload = payload.get("resilience")
        fallback = fallback_payload if isinstance(fallback_payload, dict) else {}
        provider_fallback = fallback.get("provider_fallback")
        if isinstance(provider_fallback, dict):
            selected_fallback = str(provider_fallback.get("selected_provider", "")).strip().lower()
            if selected_fallback and str(args.get("provider", "")).strip().lower() in {"", "auto"}:
                patch["provider"] = selected_fallback
                details["signals"].append("resilience.provider_fallback")

        contract_diag_payload = payload.get("contract_diagnostic")
        contract_diag = contract_diag_payload if isinstance(contract_diag_payload, dict) else {}
        if not contract_diag and simulation_payload:
            nested_diag = simulation_payload.get("contract_diagnostic")
            if isinstance(nested_diag, dict):
                contract_diag = nested_diag
        if contract_diag:
            allowed_raw = contract_diag.get("allowed_providers", [])
            allowed = [
                str(item).strip().lower()
                for item in (allowed_raw if isinstance(allowed_raw, list) else [])
                if str(item).strip()
            ]
            requested = str(contract_diag.get("requested_provider", "")).strip().lower()
            current_provider = str(args.get("provider", "")).strip().lower() or requested
            if allowed and (not current_provider or current_provider not in allowed):
                patch["provider"] = allowed[0]
                details["signals"].append("contract_diagnostic.allowed_providers")
            contract_stage = str(contract_diag.get("contract_stage", "")).strip().lower()
            diagnostic_id = str(contract_diag.get("diagnostic_id", "")).strip().lower()
            if contract_stage:
                details["contract_stage"] = contract_stage
            if diagnostic_id:
                details["diagnostic_id"] = diagnostic_id
            details["contract_code"] = str(contract_diag.get("code", "")).strip().lower()
            remediation_hints = contract_diag.get("remediation_hints", [])
            remediation_contract = contract_diag.get("remediation_contract", {})
            rem_patch, rem_signal = self._external_repair_patch_from_remediation(
                remediation_hints=remediation_hints if isinstance(remediation_hints, list) else [],
                remediation_contract=remediation_contract if isinstance(remediation_contract, dict) else {},
            )
            if rem_patch:
                patch.update(rem_patch)
                details["signals"].append(rem_signal)
            remediation_plan = contract_diag.get("remediation_plan", [])
            rem_plan_patch, rem_plan_signal = self._external_repair_patch_from_remediation_plan(
                remediation_plan=remediation_plan if isinstance(remediation_plan, list) else [],
            )
            if rem_plan_patch:
                patch.update(rem_plan_patch)
                details["signals"].append(rem_plan_signal)

        top_level_hints = payload.get("remediation_hints")
        top_level_contract = payload.get("remediation_contract")
        top_level_plan = payload.get("remediation_plan")
        if (
            isinstance(top_level_hints, list)
            or isinstance(top_level_contract, dict)
        ):
            rem_patch, rem_signal = self._external_repair_patch_from_remediation(
                remediation_hints=top_level_hints if isinstance(top_level_hints, list) else [],
                remediation_contract=top_level_contract if isinstance(top_level_contract, dict) else {},
            )
            if rem_patch:
                patch.update(rem_patch)
                details["signals"].append(f"payload.{rem_signal}")
        if isinstance(top_level_plan, list):
            rem_plan_patch, rem_plan_signal = self._external_repair_patch_from_remediation_plan(
                remediation_plan=top_level_plan,
            )
            if rem_plan_patch:
                patch.update(rem_plan_patch)
                details["signals"].append(f"payload.{rem_plan_signal}")

        hinted_provider = str(simulation_payload.get("selected_provider", "")).strip().lower()
        if hinted_provider and str(args.get("provider", "")).strip().lower() in {"", "auto"}:
            patch["provider"] = hinted_provider
            details["signals"].append("simulation.selected_provider")

        lowered_error = str(error or "").strip().lower()
        if "timeout" in lowered_error or "timed out" in lowered_error:
            max_results_raw = args.get("max_results")
            try:
                max_results_value = int(max_results_raw)
            except Exception:
                max_results_value = 0
            if max_results_value > 20:
                reduced = max(8, int(round(max_results_value * 0.6)))
                if reduced < max_results_value:
                    patch["max_results"] = reduced
                    details["signals"].append("timeout.max_results_reduce")

        if "provider" in lowered_error and "must be one of" in lowered_error:
            if "provider" not in patch:
                provider = str(args.get("provider", "")).strip().lower()
                if provider in {"", "auto"}:
                    for candidate in ("google", "graph", "smtp"):
                        patch["provider"] = candidate
                        details["signals"].append("error.provider_default")
                        break

        auth_state = metadata.get("external_auth_state")
        if isinstance(auth_state, dict) and "provider" in patch:
            providers = auth_state.get("providers", {})
            if isinstance(providers, dict):
                chosen = str(patch.get("provider", "")).strip().lower()
                provider_row = providers.get(chosen, {})
                if isinstance(provider_row, dict) and not bool(provider_row.get("has_credentials", False)):
                    fallback_provider = ""
                    for candidate, row in providers.items():
                        if not isinstance(row, dict):
                            continue
                        if bool(row.get("has_credentials", False)):
                            fallback_provider = str(candidate).strip().lower()
                            break
                    if fallback_provider:
                        patch["provider"] = fallback_provider
                        details["signals"].append("auth_state.credential_fallback")

        return (patch, details)

    @staticmethod
    def _external_repair_patch_from_remediation(
        *,
        remediation_hints: List[Dict[str, Any]],
        remediation_contract: Dict[str, Any],
    ) -> tuple[Dict[str, Any], str]:
        ranked: List[Dict[str, Any]] = []
        for index, hint in enumerate(remediation_hints):
            if not isinstance(hint, dict):
                continue
            args_patch = hint.get("args_patch")
            if not isinstance(args_patch, dict) or not args_patch:
                continue
            try:
                confidence = float(hint.get("confidence", 0.0) or 0.0)
            except Exception:
                confidence = 0.0
            try:
                priority = int(hint.get("priority", index + 1) or (index + 1))
            except Exception:
                priority = index + 1
            ranked.append(
                {
                    "patch": dict(args_patch),
                    "priority": max(1, priority),
                    "confidence": max(0.0, min(1.0, confidence)),
                    "source": "remediation_hints.args_patch",
                }
            )

        strategies = remediation_contract.get("strategies", []) if isinstance(remediation_contract, dict) else []
        if isinstance(strategies, list):
            for index, strategy in enumerate(strategies):
                if not isinstance(strategy, dict):
                    continue
                if str(strategy.get("type", "")).strip().lower() != "args_patch":
                    continue
                args_patch = strategy.get("args_patch")
                if not isinstance(args_patch, dict) or not args_patch:
                    continue
                try:
                    confidence = float(strategy.get("confidence", 0.0) or 0.0)
                except Exception:
                    confidence = 0.0
                try:
                    priority = int(strategy.get("priority", index + 1) or (index + 1))
                except Exception:
                    priority = index + 1
                ranked.append(
                    {
                        "patch": dict(args_patch),
                        "priority": max(1, priority),
                        "confidence": max(0.0, min(1.0, confidence)),
                        "source": "remediation_contract.strategy",
                    }
                )

        ranked.sort(
            key=lambda item: (
                int(item.get("priority", 999)),
                -float(item.get("confidence", 0.0)),
            )
        )
        if not ranked:
            return ({}, "")
        selected = ranked[0]
        patch = selected.get("patch", {})
        if not isinstance(patch, dict) or not patch:
            return ({}, "")
        return (dict(patch), str(selected.get("source", "remediation")))

    @staticmethod
    def _external_repair_patch_from_remediation_plan(
        *,
        remediation_plan: List[Dict[str, Any]],
    ) -> tuple[Dict[str, Any], str]:
        rows = remediation_plan if isinstance(remediation_plan, list) else []
        ranked: List[Dict[str, Any]] = []
        phase_rank = {
            "normalize_args": 1,
            "repair_dependency": 2,
            "diagnose": 3,
            "retry": 4,
        }
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            args_patch = row.get("args_patch")
            if not isinstance(args_patch, dict) or not args_patch:
                continue
            phase = str(row.get("phase", "")).strip().lower()
            try:
                confidence = float(row.get("confidence", 0.0) or 0.0)
            except Exception:
                confidence = 0.0
            ranked.append(
                {
                    "patch": dict(args_patch),
                    "phase": phase,
                    "priority": phase_rank.get(phase, 9),
                    "confidence": max(0.0, min(1.0, confidence)),
                    "source": f"remediation_plan.{phase or 'phase'}",
                    "index": index,
                }
            )
        ranked.sort(
            key=lambda item: (
                int(item.get("priority", 99)),
                -float(item.get("confidence", 0.0)),
                int(item.get("index", 0)),
            )
        )
        if not ranked:
            return ({}, "")
        selected = ranked[0]
        patch = selected.get("patch", {})
        if not isinstance(patch, dict) or not patch:
            return ({}, "")
        return (dict(patch), str(selected.get("source", "remediation_plan")))

    @classmethod
    def _is_allowed_external_remediation_action(cls, action: str) -> bool:
        clean = str(action or "").strip().lower()
        if not clean:
            return False
        return clean in cls._EXTERNAL_REMEDIATION_TOOL_ALLOWLIST

    def _extract_external_remediation_actions_from_payload(
        self,
        *,
        step: PlanStep,
        payload: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return []
        roots: List[Dict[str, Any]] = [payload]
        contract_diag = payload.get("contract_diagnostic")
        if isinstance(contract_diag, dict):
            roots.append(contract_diag)

        rows: List[Dict[str, Any]] = []
        for root in roots:
            contract_stage = str(root.get("contract_stage", "")).strip().lower()
            diagnostic_id = str(root.get("diagnostic_id", "")).strip().lower()
            remediation_hints = root.get("remediation_hints", [])
            if isinstance(remediation_hints, list):
                for index, hint in enumerate(remediation_hints):
                    if not isinstance(hint, dict):
                        continue
                    hint_priority = self._coerce_int(hint.get("priority", index + 2), minimum=1, maximum=9999, default=index + 2)
                    hint_confidence = self._coerce_float(hint.get("confidence", 0.0), minimum=0.0, maximum=1.0, default=0.0)
                    tool_action = hint.get("tool_action")
                    if not isinstance(tool_action, dict):
                        pass
                    else:
                        action_name = str(tool_action.get("action", "")).strip().lower()
                        args_payload = tool_action.get("args", {})
                        args = dict(args_payload) if isinstance(args_payload, dict) else {}
                        if action_name:
                            rows.append(
                                {
                                    "action": action_name,
                                    "args": args,
                                    "priority": hint_priority,
                                    "confidence": hint_confidence,
                                    "source": "remediation_hints.tool_action",
                                    "phase": self._infer_remediation_phase(
                                        action=action_name,
                                        source="remediation_hints.tool_action",
                                    ),
                                    "contract_stage": contract_stage,
                                    "diagnostic_id": diagnostic_id,
                                }
                            )

                    remediation_payload = hint.get("remediation")
                    remediation = remediation_payload if isinstance(remediation_payload, dict) else {}
                    remediation_type = str(remediation.get("type", "")).strip().lower()
                    schedule_rows_raw = remediation.get("schedule", hint.get("retry_schedule", []))
                    schedule_rows = schedule_rows_raw if isinstance(schedule_rows_raw, list) else []
                    if remediation_type == "staggered_provider_retry" and schedule_rows:
                        for schedule_index, schedule_row in enumerate(schedule_rows[:4]):
                            if not isinstance(schedule_row, dict):
                                continue
                            provider = str(schedule_row.get("provider", "")).strip().lower()
                            if not provider:
                                provider = str(step.args.get("provider", "auto")).strip().lower() if isinstance(step.args, dict) else "auto"
                            delay_s = self._coerce_float(
                                schedule_row.get("delay_s", 0.0),
                                minimum=0.0,
                                maximum=600.0,
                                default=0.0,
                            )
                            args = {
                                "action": str(step.action or "").strip().lower(),
                                "provider": provider or "auto",
                            }
                            rows.append(
                                {
                                    "action": "external_connector_preflight",
                                    "args": args,
                                    "priority": max(1, hint_priority + schedule_index),
                                    "confidence": max(0.0, min(1.0, hint_confidence - (float(schedule_index) * 0.03))),
                                    "source": "remediation_hints.retry_schedule",
                                    "phase": self._infer_remediation_phase(
                                        action="external_connector_preflight",
                                        source="remediation_hints.retry_schedule",
                                        phase="diagnose",
                                    ),
                                    "contract_stage": contract_stage,
                                    "diagnostic_id": diagnostic_id,
                                    "plan_phase": "diagnose",
                                    "delay_s": delay_s,
                                    "schedule_rank": schedule_index + 1,
                                }
                            )

            remediation_contract = root.get("remediation_contract", {})
            strategies = remediation_contract.get("strategies", []) if isinstance(remediation_contract, dict) else []
            if isinstance(strategies, list):
                for index, strategy in enumerate(strategies):
                    if not isinstance(strategy, dict):
                        continue
                    if str(strategy.get("type", "")).strip().lower() != "tool_action":
                        continue
                    tool_action = strategy.get("tool_action")
                    if not isinstance(tool_action, dict):
                        continue
                    action_name = str(tool_action.get("action", "")).strip().lower()
                    args_payload = tool_action.get("args", {})
                    args = dict(args_payload) if isinstance(args_payload, dict) else {}
                    if not action_name:
                        continue
                    rows.append(
                        {
                            "action": action_name,
                            "args": args,
                            "priority": self._coerce_int(strategy.get("priority", index + 1), minimum=1, maximum=9999, default=index + 1),
                            "confidence": self._coerce_float(strategy.get("confidence", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                            "source": "remediation_contract.tool_action",
                            "phase": self._infer_remediation_phase(
                                action=action_name,
                                source="remediation_contract.tool_action",
                            ),
                            "contract_stage": contract_stage,
                            "diagnostic_id": diagnostic_id,
                        }
                    )
            remediation_plan = root.get("remediation_plan", [])
            if isinstance(remediation_plan, list):
                phase_rank = {
                    "repair_dependency": 1,
                    "normalize_args": 2,
                    "diagnose": 3,
                    "retry": 4,
                }
                for index, phase in enumerate(remediation_plan):
                    if not isinstance(phase, dict):
                        continue
                    tool_action = phase.get("tool_action")
                    if not isinstance(tool_action, dict):
                        continue
                    action_name = str(tool_action.get("action", "")).strip().lower()
                    args_payload = tool_action.get("args", {})
                    args = dict(args_payload) if isinstance(args_payload, dict) else {}
                    if not action_name:
                        continue
                    phase_name = str(phase.get("phase", "")).strip().lower()
                    try:
                        confidence = float(phase.get("confidence", 0.66) or 0.66)
                    except Exception:
                        confidence = 0.66
                    rows.append(
                        {
                            "action": action_name,
                            "args": args,
                            "priority": max(1, phase_rank.get(phase_name, 5) + index),
                            "confidence": max(0.0, min(1.0, confidence)),
                            "source": "remediation_plan.tool_action",
                            "plan_phase": phase_name,
                            "phase": self._infer_remediation_phase(
                                action=action_name,
                                source="remediation_plan.tool_action",
                                phase=phase_name,
                            ),
                            "contract_stage": contract_stage,
                            "diagnostic_id": diagnostic_id,
                        }
                    )

        step_args = step.args if isinstance(step.args, dict) else {}
        default_provider = str(step_args.get("provider", "")).strip().lower()
        for row in rows:
            args = row.get("args")
            if not isinstance(args, dict):
                row["args"] = {}
                args = row["args"]
            action_name = str(row.get("action", "")).strip().lower()
            provider = str(args.get("provider", "")).strip().lower()
            if action_name.startswith("oauth_token_") and not provider and default_provider and default_provider != "auto":
                args["provider"] = default_provider
            if action_name == "oauth_token_maintain" and "refresh_window_s" not in args:
                args["refresh_window_s"] = 900

        dedup: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            action_name = str(row.get("action", "")).strip().lower()
            args_payload = row.get("args", {})
            args = args_payload if isinstance(args_payload, dict) else {}
            fingerprint = f"{action_name}|{json.dumps(args, ensure_ascii=True, sort_keys=True, separators=(',', ':'))}"
            existing = dedup.get(fingerprint)
            if existing is None:
                dedup[fingerprint] = dict(row)
                continue
            if (
                self._coerce_int(row.get("priority", 999), minimum=1, maximum=9999, default=999)
                < self._coerce_int(existing.get("priority", 999), minimum=1, maximum=9999, default=999)
            ) or (
                self._coerce_int(row.get("priority", 999), minimum=1, maximum=9999, default=999)
                == self._coerce_int(existing.get("priority", 999), minimum=1, maximum=9999, default=999)
                and self._coerce_float(row.get("confidence", 0.0), minimum=0.0, maximum=1.0, default=0.0)
                > self._coerce_float(existing.get("confidence", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            ):
                dedup[fingerprint] = dict(row)

        ranked = list(dedup.values())
        ranked.sort(
            key=lambda item: (
                self._remediation_phase_rank(str(item.get("phase", ""))),
                self._coerce_int(item.get("priority", 999), minimum=1, maximum=9999, default=999),
                -self._coerce_float(item.get("confidence", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                str(item.get("action", "")),
            )
        )
        return ranked[:12]

    @staticmethod
    def _remediation_phase_rank(phase: str) -> int:
        clean = str(phase or "").strip().lower()
        phase_rank = {
            "repair_dependency": 1,
            "diagnose": 2,
            "normalize_args": 3,
            "retry": 4,
        }
        return int(phase_rank.get(clean, 9))

    @classmethod
    def _infer_remediation_phase(cls, *, action: str, source: str, phase: str = "") -> str:
        clean_phase = str(phase or "").strip().lower()
        if clean_phase in {"repair_dependency", "diagnose", "normalize_args", "retry"}:
            return clean_phase
        clean_action = str(action or "").strip().lower()
        clean_source = str(source or "").strip().lower()
        if clean_action.startswith("oauth_token_"):
            return "repair_dependency"
        if clean_action in {"external_connector_status", "external_connector_preflight"}:
            if "remediation_plan" in clean_source:
                return "diagnose"
            return "diagnose"
        if "remediation_plan" in clean_source:
            return "normalize_args"
        return "repair_dependency"

    @staticmethod
    def _normalize_execution_contract_mode(value: Any) -> str:
        clean = str(value or "").strip().lower()
        if clean in {"manual", "assisted", "automated"}:
            return clean
        if clean in {"auto", "automatic"}:
            return "automated"
        return ""

    def _external_remediation_execution_profile(
        self,
        *,
        metadata: Dict[str, Any],
        payloads: List[Dict[str, Any]],
        planned_count: int,
    ) -> Dict[str, Any]:
        runtime_meta = metadata if isinstance(metadata, dict) else {}
        rows = [row for row in payloads if isinstance(row, dict)]
        mode_rank = {"automated": 0, "assisted": 1, "manual": 2}
        explicit_mode = self._normalize_execution_contract_mode(
            runtime_meta.get("external_remediation_execution_mode", "")
        )
        mode = explicit_mode or "automated"
        source_count = 0
        max_retry_attempts = self._coerce_int(
            runtime_meta.get("external_remediation_max_retry_attempts", 2),
            minimum=1,
            maximum=8,
            default=2,
        )
        allow_provider_reroute = self._coerce_bool(
            runtime_meta.get("external_remediation_allow_provider_reroute", True),
            default=True,
        )
        estimated_recovery_s = 0
        blocking_class = ""
        stop_conditions: List[str] = []
        phases: List[Dict[str, Any]] = []

        def _ingest_contract(contract: Dict[str, Any]) -> None:
            nonlocal mode, source_count, max_retry_attempts, allow_provider_reroute, estimated_recovery_s, blocking_class
            if not isinstance(contract, dict) or not contract:
                return
            source_count += 1
            execution_contract = contract.get("execution_contract", {})
            execution_row = execution_contract if isinstance(execution_contract, dict) else {}
            contract_mode = self._normalize_execution_contract_mode(contract.get("automation_tier", ""))
            if not contract_mode:
                contract_mode = self._normalize_execution_contract_mode(execution_row.get("mode", ""))
            if not contract_mode:
                contract_mode = self._normalize_execution_contract_mode(contract.get("automation_mode", ""))

            contract_max_retry = self._coerce_int(
                execution_row.get("max_retry_attempts", contract.get("max_retry_attempts", max_retry_attempts)),
                minimum=1,
                maximum=8,
                default=max_retry_attempts,
            )
            max_retry_attempts = min(max_retry_attempts, contract_max_retry)

            verification = execution_row.get("verification", {})
            verify_row = verification if isinstance(verification, dict) else {}
            if "allow_provider_reroute" in verify_row:
                allow_provider_reroute = bool(verify_row.get("allow_provider_reroute", True))

            phases_raw = execution_row.get("phases", [])
            if isinstance(phases_raw, list):
                for phase in phases_raw[:10]:
                    if isinstance(phase, dict):
                        phases.append(dict(phase))

            stop_conditions_raw = execution_row.get("stop_conditions", [])
            if isinstance(stop_conditions_raw, list):
                for row in stop_conditions_raw[:8]:
                    item = str(row or "").strip().lower()
                    if item and item not in stop_conditions:
                        stop_conditions.append(item)

            if contract_mode and mode_rank.get(contract_mode, 0) > mode_rank.get(mode, 0):
                mode = contract_mode
            if not blocking_class:
                blocking_class = str(contract.get("blocking_class", "")).strip().lower()
            try:
                estimated = int(contract.get("estimated_recovery_s", 0) or 0)
            except Exception:
                estimated = 0
            if estimated > estimated_recovery_s:
                estimated_recovery_s = estimated

        for row in rows:
            root_contract = row.get("remediation_contract", {})
            if isinstance(root_contract, dict):
                _ingest_contract(root_contract)
            contract_diag = row.get("contract_diagnostic", {})
            diag_row = contract_diag if isinstance(contract_diag, dict) else {}
            diag_contract = diag_row.get("remediation_contract", {})
            if isinstance(diag_contract, dict):
                _ingest_contract(diag_contract)

        if blocking_class == "auth" and mode == "automated":
            mode = "assisted"
        if estimated_recovery_s >= 900 and mode == "automated":
            mode = "assisted"
        if estimated_recovery_s >= 1800:
            max_retry_attempts = max(1, min(max_retry_attempts, 1))
            if "risk_budget_exceeded" not in stop_conditions:
                stop_conditions.append("risk_budget_exceeded")
        if mode == "manual":
            max_retry_attempts = 1
            if "manual_escalation" not in stop_conditions:
                stop_conditions.append("manual_escalation")
        if planned_count <= 1 and mode == "assisted":
            max_retry_attempts = max(1, min(max_retry_attempts, 1))

        require_diagnose_before_repair = False
        if phases:
            required_phase_names = {
                str(phase.get("phase", "")).strip().lower()
                for phase in phases
                if isinstance(phase, dict) and bool(phase.get("required", False))
            }
            if "diagnose" in required_phase_names and "repair_dependency" in required_phase_names:
                require_diagnose_before_repair = True
        if require_diagnose_before_repair and "diagnose_before_repair" not in stop_conditions:
            stop_conditions.append("diagnose_before_repair")
        if not allow_provider_reroute and "provider_reroute_locked" not in stop_conditions:
            stop_conditions.append("provider_reroute_locked")

        return {
            "mode": mode,
            "max_retry_attempts": max_retry_attempts,
            "allow_provider_reroute": bool(allow_provider_reroute),
            "estimated_recovery_s": int(max(0, min(3600, estimated_recovery_s))),
            "blocking_class": blocking_class or "generic",
            "stop_conditions": stop_conditions[:8],
            "phases": phases[:10],
            "require_diagnose_before_repair": bool(require_diagnose_before_repair),
            "source_count": int(source_count),
        }

    def _external_remediation_budget_profile(
        self,
        *,
        metadata: Dict[str, Any],
        payloads: List[Dict[str, Any]],
        planned_count: int,
    ) -> Dict[str, Any]:
        runtime_meta = metadata if isinstance(metadata, dict) else {}
        rows = [row for row in payloads if isinstance(row, dict)]
        base_budget = self._coerce_int(
            runtime_meta.get("external_remediation_max_actions", 2),
            minimum=1,
            maximum=8,
            default=2,
        )
        base_total = self._coerce_int(
            runtime_meta.get("external_remediation_max_total_actions", 6),
            minimum=2,
            maximum=24,
            default=6,
        )
        mission_feedback = runtime_meta.get("mission_feedback", {})
        mission_row = mission_feedback if isinstance(mission_feedback, dict) else {}
        mission_trend = runtime_meta.get("mission_trend_feedback", {})
        trend_row = mission_trend if isinstance(mission_trend, dict) else {}
        pressure = self._coerce_float(
            trend_row.get("trend_pressure", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        mode = str(trend_row.get("mode", "")).strip().lower()
        risk_trend = str(trend_row.get("risk_trend", "")).strip().lower()
        quality_trend = str(trend_row.get("quality_trend", "")).strip().lower()
        if mode == "worsening" or risk_trend == "worsening" or quality_trend == "degrading":
            pressure = min(1.0, pressure + 0.14)
        if mode == "improving" or risk_trend == "improving" or quality_trend == "improving":
            pressure = max(0.0, pressure - 0.1)
        risk_level = str(mission_row.get("risk_level", "")).strip().lower()
        quality_level = str(mission_row.get("quality_level", "")).strip().lower()
        if risk_level == "high":
            pressure = min(1.0, pressure + 0.11)
        if quality_level == "low":
            pressure = min(1.0, pressure + 0.08)

        external_contract_pressure = runtime_meta.get("external_contract_pressure", {})
        if isinstance(external_contract_pressure, dict):
            pressure = min(
                1.0,
                max(
                    pressure,
                    self._coerce_float(
                        external_contract_pressure.get("pressure", 0.0),
                        minimum=0.0,
                        maximum=1.0,
                        default=0.0,
                    ),
                ),
            )
        contract_risk = self._external_contract_risk_score(metadata=runtime_meta, payloads=rows)
        pressure = min(1.0, max(pressure, self._coerce_float(contract_risk.get("risk", 0.0), minimum=0.0, maximum=1.0, default=0.0)))

        mission_profile = str(runtime_meta.get("external_route_profile", "")).strip().lower()
        if not mission_profile:
            for row in rows:
                route = row.get("provider_routing", {})
                if isinstance(route, dict):
                    profile = str(route.get("mission_profile", "")).strip().lower()
                    if profile:
                        mission_profile = profile
                        break

        high_impact_codes = {
            "auth_preflight_failed",
            "no_provider_candidates_after_contract",
            "provider_not_supported_for_action",
            "provider_outage_blocked",
            "provider_cooldown_blocked",
        }
        for row in rows:
            diag_payload = row.get("contract_diagnostic", {})
            diag = diag_payload if isinstance(diag_payload, dict) else {}
            code = str(diag.get("code", "")).strip().lower()
            if code in high_impact_codes:
                pressure = min(1.0, pressure + 0.16)
            elif code:
                pressure = min(1.0, pressure + 0.06)

        budget = base_budget
        max_total = base_total
        if pressure >= 0.78:
            budget += 2
            max_total += 4
        elif pressure >= 0.54:
            budget += 1
            max_total += 2
        elif pressure <= 0.2:
            budget -= 1
            max_total -= 1

        if mission_profile in {"defensive", "cautious"}:
            budget += 1
            max_total += 1
        elif mission_profile == "throughput":
            budget -= 1
        risk_level = str(contract_risk.get("risk_level", "")).strip().lower()
        if risk_level == "critical":
            budget += 2
            max_total += 4
        elif risk_level == "high":
            budget += 1
            max_total += 2
        elif risk_level == "low":
            budget -= 1

        budget = self._coerce_int(budget, minimum=1, maximum=8, default=base_budget)
        max_total = self._coerce_int(max_total, minimum=2, maximum=24, default=base_total)
        budget = min(max(1, planned_count), budget)

        return {
            "budget": budget,
            "max_total": max_total,
            "pressure": round(pressure, 6),
            "mission_profile": mission_profile or "balanced",
            "mode": mode,
            "contract_risk": round(
                self._coerce_float(contract_risk.get("risk", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                6,
            ),
            "contract_risk_level": str(contract_risk.get("risk_level", "")).strip().lower() or "low",
            "contract_codes": contract_risk.get("contract_codes", []),
            "contract_stages": contract_risk.get("contract_stages", []),
        }

    def _external_contract_risk_score(
        self,
        *,
        metadata: Dict[str, Any],
        payloads: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        runtime_meta = metadata if isinstance(metadata, dict) else {}
        rows = [row for row in payloads if isinstance(row, dict)]
        risk = self._coerce_float(
            runtime_meta.get("external_contract_pressure", {}).get("pressure", 0.0)
            if isinstance(runtime_meta.get("external_contract_pressure", {}), dict)
            else 0.0,
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        risk_floor = self._coerce_float(
            runtime_meta.get("external_remediation_contract_risk_floor", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        risk = max(risk, risk_floor)
        contract_codes: List[str] = []
        contract_stages: List[str] = []
        failed_checks = 0
        warning_checks = 0
        severity_score_values: List[float] = []
        recovery_estimates: List[int] = []
        blocking_classes: List[str] = []
        automation_tiers: List[str] = []
        execution_modes: List[str] = []
        stop_condition_hits: List[str] = []
        retry_attempt_caps: List[int] = []
        provider_reroute_locked = False

        def _ingest_execution_contract(execution_contract: Dict[str, Any], *, fallback_mode: str = "") -> None:
            nonlocal risk, provider_reroute_locked
            if not isinstance(execution_contract, dict) or not execution_contract:
                return
            execution_mode = self._normalize_execution_contract_mode(
                execution_contract.get("mode", fallback_mode)
            )
            if execution_mode:
                execution_modes.append(execution_mode)
                if execution_mode == "manual":
                    risk = min(1.0, risk + 0.08)
                elif execution_mode == "assisted":
                    risk = min(1.0, risk + 0.03)
                elif execution_mode == "automated":
                    risk = max(0.0, risk - 0.02)

            max_retry_attempts = self._coerce_int(
                execution_contract.get("max_retry_attempts", 0),
                minimum=0,
                maximum=8,
                default=0,
            )
            if max_retry_attempts > 0:
                retry_attempt_caps.append(max_retry_attempts)
                if max_retry_attempts <= 1:
                    risk = min(1.0, risk + 0.04)
                elif max_retry_attempts >= 4:
                    risk = max(0.0, risk - 0.02)

            verification = execution_contract.get("verification", {})
            verify_row = verification if isinstance(verification, dict) else {}
            if "allow_provider_reroute" in verify_row and not bool(verify_row.get("allow_provider_reroute", True)):
                provider_reroute_locked = True
                risk = min(1.0, risk + 0.03)

            stop_conditions_raw = execution_contract.get("stop_conditions", [])
            if isinstance(stop_conditions_raw, list):
                for item in stop_conditions_raw[:12]:
                    stop_condition = str(item or "").strip().lower()
                    if not stop_condition:
                        continue
                    stop_condition_hits.append(stop_condition)
                    if stop_condition in {"manual_escalation", "checkpoint_failure", "risk_budget_exceeded"}:
                        risk = min(1.0, risk + 0.05)
                    elif stop_condition in {"no_progress", "diagnose_before_repair"}:
                        risk = min(1.0, risk + 0.03)

            phase_rows = execution_contract.get("phases", [])
            if isinstance(phase_rows, list):
                required_count = 0
                for phase in phase_rows[:12]:
                    if isinstance(phase, dict) and bool(phase.get("required", False)):
                        required_count += 1
                if required_count >= 2:
                    risk = min(1.0, risk + 0.04)
        for row in rows:
            roots: List[Dict[str, Any]] = [row]
            contract_diag = row.get("contract_diagnostic")
            if isinstance(contract_diag, dict):
                roots.append(contract_diag)
            for root in roots:
                if not isinstance(root, dict):
                    continue
                code = str(root.get("code", "")).strip().lower()
                if code and code not in contract_codes:
                    contract_codes.append(code)
                stage = str(root.get("contract_stage", "")).strip().lower()
                if stage and stage not in contract_stages:
                    contract_stages.append(stage)
                severity = str(root.get("severity", "")).strip().lower()
                if severity in {"critical", "error"}:
                    risk = min(1.0, risk + 0.18)
                elif severity == "warning":
                    risk = min(1.0, risk + 0.08)
                severity_score = self._coerce_float(
                    root.get("severity_score", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                if severity_score > 0.0:
                    severity_score_values.append(severity_score)
                    risk = min(1.0, risk + (severity_score * 0.22))
                blocking_class = str(root.get("blocking_class", "")).strip().lower()
                if blocking_class:
                    blocking_classes.append(blocking_class)
                    class_pressure = {
                        "auth": 0.12,
                        "provider": 0.1,
                        "reliability": 0.08,
                        "contract": 0.06,
                    }.get(blocking_class, 0.03)
                    risk = min(1.0, risk + class_pressure)
                automation_tier = str(root.get("automation_tier", "")).strip().lower()
                if automation_tier in {"manual", "assisted", "automated"}:
                    automation_tiers.append(automation_tier)
                    if automation_tier == "manual":
                        risk = min(1.0, risk + 0.08)
                    elif automation_tier == "assisted":
                        risk = min(1.0, risk + 0.03)
                    elif automation_tier == "automated":
                        risk = max(0.0, risk - 0.02)
                estimated_recovery_s = self._coerce_int(
                    root.get("estimated_recovery_s", 0),
                    minimum=0,
                    maximum=3600,
                    default=0,
                )
                if estimated_recovery_s > 0:
                    recovery_estimates.append(estimated_recovery_s)
                    risk = min(1.0, risk + (min(1.0, float(estimated_recovery_s) / 900.0) * 0.16))
                direct_execution_contract = root.get("execution_contract", {})
                if isinstance(direct_execution_contract, dict):
                    _ingest_execution_contract(
                        direct_execution_contract,
                        fallback_mode=str(root.get("automation_tier", "")),
                    )
                checks = root.get("checks", [])
                if isinstance(checks, list):
                    for check in checks[:24]:
                        if not isinstance(check, dict):
                            continue
                        status = str(check.get("status", "")).strip().lower()
                        level = str(check.get("severity", "")).strip().lower()
                        if status == "failed":
                            if level in {"error", "critical"}:
                                failed_checks += 1
                                risk = min(1.0, risk + 0.06)
                            else:
                                warning_checks += 1
                                risk = min(1.0, risk + 0.03)
                        elif status == "warning":
                            warning_checks += 1
                            risk = min(1.0, risk + 0.02)
                remediation_plan = root.get("remediation_plan", [])
                if isinstance(remediation_plan, list):
                    for phase in remediation_plan[:12]:
                        if not isinstance(phase, dict):
                            continue
                        phase_name = str(phase.get("phase", "")).strip().lower()
                        if phase_name == "repair_dependency":
                            risk = min(1.0, risk + 0.08)
                        elif phase_name == "normalize_args":
                            risk = min(1.0, risk + 0.03)
                remediation_contract = root.get("remediation_contract", {})
                contract_row = remediation_contract if isinstance(remediation_contract, dict) else {}
                if contract_row:
                    execution_contract = contract_row.get("execution_contract", {})
                    execution_row = execution_contract if isinstance(execution_contract, dict) else {}
                    contract_mode = str(
                        contract_row.get("automation_tier", execution_row.get("mode", ""))
                    ).strip().lower()
                    if contract_mode in {"manual", "assisted", "automated"}:
                        automation_tiers.append(contract_mode)
                        if contract_mode == "manual":
                            risk = min(1.0, risk + 0.08)
                        elif contract_mode == "assisted":
                            risk = min(1.0, risk + 0.03)
                        elif contract_mode == "automated":
                            risk = max(0.0, risk - 0.03)
                    contract_blocking_class = str(contract_row.get("blocking_class", "")).strip().lower()
                    if contract_blocking_class:
                        blocking_classes.append(contract_blocking_class)
                    contract_recovery_s = self._coerce_int(
                        contract_row.get("estimated_recovery_s", 0),
                        minimum=0,
                        maximum=3600,
                        default=0,
                    )
                    if contract_recovery_s > 0:
                        recovery_estimates.append(contract_recovery_s)
                        risk = min(1.0, risk + (min(1.0, float(contract_recovery_s) / 900.0) * 0.1))
                    _ingest_execution_contract(
                        execution_row,
                        fallback_mode=str(contract_row.get("automation_tier", "")),
                    )
        severe_codes = {
            "auth_preflight_failed",
            "no_provider_candidates_after_contract",
            "provider_not_supported_for_action",
            "provider_outage_blocked",
            "provider_cooldown_blocked",
        }
        if any(code in severe_codes for code in contract_codes):
            risk = min(1.0, risk + 0.16)
        if failed_checks >= 5:
            risk = min(1.0, risk + 0.08)
        elif failed_checks <= 1 and warning_checks == 0 and contract_codes:
            risk = max(0.0, risk - 0.05)
        if severity_score_values:
            severity_avg = sum(severity_score_values) / float(len(severity_score_values))
            if severity_avg >= 0.78:
                risk = min(1.0, risk + 0.08)
            elif severity_avg <= 0.32 and failed_checks <= 1:
                risk = max(0.0, risk - 0.04)
        if recovery_estimates:
            recovery_peak = max(recovery_estimates)
            if recovery_peak >= 1800:
                risk = min(1.0, risk + 0.06)
        if blocking_classes:
            if "auth" in blocking_classes:
                risk = min(1.0, risk + 0.06)
            if "provider" in blocking_classes:
                risk = min(1.0, risk + 0.04)
        if execution_modes:
            manual_count = sum(1 for mode in execution_modes if mode == "manual")
            if manual_count >= max(1, len(execution_modes) // 2):
                risk = min(1.0, risk + 0.05)
        if provider_reroute_locked:
            risk = min(1.0, risk + 0.04)
        if stop_condition_hits:
            critical_stop_hits = sum(
                1
                for condition in stop_condition_hits
                if condition in {"manual_escalation", "checkpoint_failure", "risk_budget_exceeded"}
            )
            if critical_stop_hits >= 2:
                risk = min(1.0, risk + 0.06)
        if retry_attempt_caps:
            retry_floor = min(retry_attempt_caps)
            if retry_floor <= 1:
                risk = min(1.0, risk + 0.03)
        if automation_tiers:
            manual_count = sum(1 for tier in automation_tiers if tier == "manual")
            automated_count = sum(1 for tier in automation_tiers if tier == "automated")
            if manual_count >= max(1, len(automation_tiers) // 2):
                risk = min(1.0, risk + 0.06)
            elif automated_count == len(automation_tiers):
                risk = max(0.0, risk - 0.03)

        risk_level = "low"
        if risk >= 0.78:
            risk_level = "critical"
        elif risk >= 0.54:
            risk_level = "high"
        elif risk >= 0.32:
            risk_level = "moderate"

        return {
            "risk": round(risk, 6),
            "risk_level": risk_level,
            "contract_codes": contract_codes[:12],
            "contract_stages": contract_stages[:8],
            "failed_checks": int(failed_checks),
            "warning_checks": int(warning_checks),
            "severity_score_avg": round(
                (sum(severity_score_values) / float(len(severity_score_values))) if severity_score_values else 0.0,
                6,
            ),
            "estimated_recovery_s": int(max(recovery_estimates) if recovery_estimates else 0),
            "blocking_classes": sorted({item for item in blocking_classes if item})[:8],
            "automation_tiers": sorted({item for item in automation_tiers if item})[:4],
            "execution_modes": sorted({item for item in execution_modes if item})[:4],
            "stop_conditions": sorted({item for item in stop_condition_hits if item})[:12],
            "retry_attempt_floor": int(min(retry_attempt_caps) if retry_attempt_caps else 0),
            "provider_reroute_locked": bool(provider_reroute_locked),
        }

    async def _run_external_remediation_actions(
        self,
        *,
        step: PlanStep,
        payloads: List[Dict[str, Any]],
        metadata: Dict[str, Any],
        source: str,
        attempt: int,
    ) -> Dict[str, Any]:
        if not self._is_external_or_oauth_action(step.action):
            return {"status": "skip", "actions": []}
        rows = [row for row in payloads if isinstance(row, dict) and row]
        if not rows:
            return {"status": "skip", "actions": []}

        planned: List[Dict[str, Any]] = []
        for row in rows:
            planned.extend(self._extract_external_remediation_actions_from_payload(step=step, payload=row))
        if not planned:
            return {"status": "skip", "actions": []}

        execution_profile = self._external_remediation_execution_profile(
            metadata=metadata,
            payloads=rows,
            planned_count=len(planned),
        )
        execution_mode = str(execution_profile.get("mode", "automated")).strip().lower() or "automated"
        execution_retry_cap = self._coerce_int(
            execution_profile.get("max_retry_attempts", 2),
            minimum=1,
            maximum=8,
            default=2,
        )
        allow_provider_reroute = bool(execution_profile.get("allow_provider_reroute", True))
        stop_conditions_raw = execution_profile.get("stop_conditions", [])
        stop_conditions = [
            str(item).strip().lower()
            for item in stop_conditions_raw
            if str(item).strip()
        ] if isinstance(stop_conditions_raw, list) else []
        stop_condition_set = set(stop_conditions)
        require_diagnose_before_repair = bool(execution_profile.get("require_diagnose_before_repair", False))
        metadata["__external_remediation_execution_profile"] = dict(execution_profile)
        metadata["__external_remediation_execution_mode"] = execution_mode
        metadata["__external_remediation_allow_provider_reroute"] = bool(allow_provider_reroute)

        if execution_mode == "manual":
            return {
                "status": "manual_required",
                "attempted": 0,
                "success_count": 0,
                "actions": [],
                "reason": "execution_contract_manual_mode",
                "execution_profile": execution_profile,
            }

        budget_profile = self._external_remediation_budget_profile(
            metadata=metadata,
            payloads=rows,
            planned_count=len(planned),
        )
        budget = self._coerce_int(
            budget_profile.get("budget", 2),
            minimum=1,
            maximum=8,
            default=2,
        )
        max_total = self._coerce_int(
            budget_profile.get("max_total", 6),
            minimum=2,
            maximum=24,
            default=6,
        )
        cache_raw = metadata.get("__external_remediation_cache")
        cache = cache_raw if isinstance(cache_raw, dict) else {}
        attempted_total_so_far = self._coerce_int(
            metadata.get("__external_remediation_attempted_total", 0),
            minimum=0,
            maximum=10_000,
            default=0,
        )
        remaining_total = max(0, max_total - attempted_total_so_far)
        if remaining_total <= 0:
            return {
                "status": "skip",
                "attempted": 0,
                "success_count": 0,
                "actions": [],
                "reason": "remediation_total_budget_exhausted",
                "budget_profile": budget_profile,
                "execution_profile": execution_profile,
            }
        budget = max(1, min(budget, remaining_total))
        budget = max(1, min(budget, execution_retry_cap))
        contract_risk = self._coerce_float(
            budget_profile.get("contract_risk", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        checkpoint_enforcement = self._resolve_remediation_checkpoint_mode(
            metadata=metadata,
            budget_profile=budget_profile,
            planned=planned,
        )
        planned.sort(
            key=lambda row: (
                self._remediation_phase_rank(str(row.get("phase", ""))),
                self._coerce_int(row.get("priority", 999), minimum=1, maximum=9999, default=999),
                -self._coerce_float(row.get("confidence", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                str(row.get("action", "")),
            )
        )
        actions: List[Dict[str, Any]] = []
        checkpoints: List[Dict[str, Any]] = []
        attempted = 0
        success_count = 0
        current_phase = ""
        phase_attempted = 0
        checkpoint_blocked = False
        diagnose_success = False
        stop_condition_reason = ""
        locked_provider = ""
        if isinstance(step.args, dict):
            locked_provider = str(step.args.get("provider", "")).strip().lower()
        if not locked_provider:
            locked_provider = str(metadata.get("__external_selected_provider", "")).strip().lower()
        if locked_provider in {"", "auto"}:
            locked_provider = ""

        for row in planned:
            if attempted >= budget:
                break
            if "risk_budget_exceeded" in stop_condition_set and contract_risk >= 0.92 and attempted > 0:
                stop_condition_reason = "risk_budget_exceeded"
                break
            phase_name = self._infer_remediation_phase(
                action=str(row.get("action", "")),
                source=str(row.get("source", "")),
                phase=str(row.get("phase", "")),
            )
            if current_phase and phase_name != current_phase and phase_attempted > 0:
                checkpoint = await self._run_external_remediation_checkpoint(
                    step=step,
                    metadata=metadata,
                    source=source,
                    attempt=attempt,
                    phase=current_phase,
                    contract_risk=contract_risk,
                    strict=checkpoint_enforcement == "strict",
                    provider=self._remediation_checkpoint_provider(step=step, action_row=row, metadata=metadata),
                )
                checkpoints.append(checkpoint)
                if checkpoint_enforcement == "strict" and str(checkpoint.get("status", "")).strip().lower() != "success":
                    checkpoint_blocked = True
                    if "checkpoint_failure" in stop_condition_set:
                        stop_condition_reason = "checkpoint_failure"
                    break
                phase_attempted = 0
            current_phase = phase_name or current_phase
            if (
                require_diagnose_before_repair
                and phase_name == "repair_dependency"
                and not diagnose_success
            ):
                actions.append(
                    {
                        "action": str(row.get("action", "")).strip().lower(),
                        "status": "skipped",
                        "reason": "diagnose_required_before_repair",
                        "source": str(row.get("source", "")),
                        "phase": phase_name,
                    }
                )
                if "diagnose_before_repair" in stop_condition_set:
                    stop_condition_reason = "diagnose_before_repair"
                continue

            action_name = str(row.get("action", "")).strip().lower()
            args_payload = row.get("args", {})
            args = dict(args_payload) if isinstance(args_payload, dict) else {}
            if execution_mode == "assisted" and phase_name == "repair_dependency":
                actions.append(
                    {
                        "action": action_name,
                        "status": "skipped",
                        "reason": "execution_contract_assisted_repair_needs_manual",
                        "source": str(row.get("source", "")),
                        "phase": phase_name,
                    }
                )
                if "manual_escalation" in stop_condition_set:
                    stop_condition_reason = "manual_escalation"
                continue
            if not allow_provider_reroute and action_name == "external_connector_preflight" and locked_provider:
                args["provider"] = locked_provider
            row_confidence = self._coerce_float(row.get("confidence", 0.0), minimum=0.0, maximum=1.0, default=0.0)
            if contract_risk < 0.22 and row_confidence < 0.55:
                actions.append(
                    {
                        "action": action_name,
                        "status": "skipped",
                        "reason": "low_contract_risk_low_confidence",
                        "source": str(row.get("source", "")),
                        "phase": phase_name,
                    }
                )
                continue
            if not self._is_allowed_external_remediation_action(action_name):
                actions.append(
                    {
                        "action": action_name,
                        "status": "skipped",
                        "reason": "action_not_allowlisted",
                        "source": str(row.get("source", "")),
                        "phase": phase_name,
                    }
                )
                continue
            fingerprint = f"{action_name}|{json.dumps(args, ensure_ascii=True, sort_keys=True, separators=(',', ':'))}"
            runs = self._coerce_int(cache.get(fingerprint, 0), minimum=0, maximum=1000, default=0)
            if runs >= execution_retry_cap:
                actions.append(
                    {
                        "action": action_name,
                        "status": "skipped",
                        "reason": "remediation_run_budget_exhausted",
                        "source": str(row.get("source", "")),
                        "phase": phase_name,
                    }
                )
                continue

            delay_requested_s = self._coerce_float(
                row.get("delay_s", 0.0),
                minimum=0.0,
                maximum=600.0,
                default=0.0,
            )
            delay_cap_s = self._coerce_float(
                metadata.get("external_remediation_delay_cap_s", self.external_remediation_delay_cap_s),
                minimum=0.0,
                maximum=60.0,
                default=self.external_remediation_delay_cap_s,
            )
            delay_applied_s = 0.0
            if bool(self.external_remediation_delay_enabled) and delay_requested_s > 0.0:
                delay_applied_s = self._coerce_float(
                    min(delay_requested_s, delay_cap_s),
                    minimum=0.0,
                    maximum=60.0,
                    default=0.0,
                )
                if delay_applied_s >= 0.02:
                    self.telemetry.emit(
                        "step.external_remediation_delay",
                        {
                            "step_id": step.step_id,
                            "parent_action": step.action,
                            "action": action_name,
                            "phase": phase_name,
                            "requested_delay_s": round(delay_requested_s, 3),
                            "applied_delay_s": round(delay_applied_s, 3),
                        },
                    )
                    await asyncio.sleep(delay_applied_s)

            aux_metadata = dict(metadata)
            aux_metadata["__skip_approval"] = True
            aux_metadata["__external_remediation"] = True
            aux_metadata["__external_remediation_parent_action"] = str(step.action or "").strip().lower()
            aux_metadata["__external_remediation_contract_risk"] = contract_risk
            aux_metadata["__external_remediation_diagnostic_id"] = str(row.get("diagnostic_id", "")).strip().lower()
            aux_metadata["__external_remediation_execution_mode"] = execution_mode
            aux_metadata["__external_remediation_allow_provider_reroute"] = bool(allow_provider_reroute)
            if delay_applied_s > 0.0:
                aux_metadata["__external_remediation_delay_s"] = round(delay_applied_s, 6)
            result = await self._execute_aux_action(
                action=action_name,
                args=args,
                source=f"{source}:external-remediation",
                metadata=aux_metadata,
                timeout_s=20 if contract_risk >= 0.66 else 16,
                attempt=attempt,
            )
            cache[fingerprint] = runs + 1
            attempted += 1
            phase_attempted += 1
            if result.status == "success":
                success_count += 1
                if phase_name == "diagnose":
                    diagnose_success = True
            action_row = {
                "action": action_name,
                "status": str(result.status or "").strip().lower(),
                "error": str(result.error or "").strip(),
                "source": str(row.get("source", "")),
                "args": args,
                "output": result.output if isinstance(result.output, dict) else {},
                "phase": phase_name,
                "contract_stage": str(row.get("contract_stage", "")).strip().lower(),
                "diagnostic_id": str(row.get("diagnostic_id", "")).strip().lower(),
                "plan_phase": str(row.get("plan_phase", "")).strip().lower(),
                "confidence": row_confidence,
                "schedule_rank": self._coerce_int(row.get("schedule_rank", 0), minimum=0, maximum=1000, default=0),
                "delay_requested_s": round(delay_requested_s, 3),
                "delay_applied_s": round(delay_applied_s, 3),
            }
            actions.append(action_row)
            self.telemetry.emit(
                "step.external_remediation_action",
                {
                    "step_id": step.step_id,
                    "parent_action": step.action,
                    "action": action_name,
                    "status": action_row["status"],
                    "attempt": attempt,
                    "source": action_row["source"],
                    "phase": action_row["phase"],
                },
            )
            if (
                "no_progress" in stop_condition_set
                and attempted >= max(2, min(4, execution_retry_cap))
                and success_count <= 0
            ):
                stop_condition_reason = "no_progress"
                break
            if (
                "manual_escalation" in stop_condition_set
                and execution_mode in {"manual", "assisted"}
                and action_row["status"] not in {"success", "skipped"}
            ):
                stop_condition_reason = "manual_escalation"
                break

        if not checkpoint_blocked and not stop_condition_reason and current_phase and phase_attempted > 0:
            terminal_row = actions[-1] if actions else {}
            checkpoint = await self._run_external_remediation_checkpoint(
                step=step,
                metadata=metadata,
                source=source,
                attempt=attempt,
                phase=current_phase,
                contract_risk=contract_risk,
                strict=checkpoint_enforcement == "strict",
                provider=self._remediation_checkpoint_provider(step=step, action_row=terminal_row, metadata=metadata),
            )
            checkpoints.append(checkpoint)
            if checkpoint_enforcement == "strict" and str(checkpoint.get("status", "")).strip().lower() != "success":
                checkpoint_blocked = True
                if "checkpoint_failure" in stop_condition_set:
                    stop_condition_reason = "checkpoint_failure"

        metadata["__external_remediation_cache"] = cache
        metadata["__external_remediation_attempted_total"] = attempted_total_so_far + attempted
        result_status = "success" if attempted > 0 else "skip"
        if stop_condition_reason:
            result_status = "partial" if attempted > 0 else "blocked"
        if checkpoint_blocked:
            result_status = "partial" if attempted > 0 else "blocked"
        return {
            "status": result_status,
            "attempted": attempted,
            "success_count": success_count,
            "budget": budget,
            "max_total": max_total,
            "attempted_total": attempted_total_so_far + attempted,
            "budget_profile": budget_profile,
            "contract_risk": round(contract_risk, 6),
            "checkpoint_mode": checkpoint_enforcement,
            "checkpoint_blocked": bool(checkpoint_blocked),
            "checkpoints": checkpoints[:10],
            "contract_codes": budget_profile.get("contract_codes", []),
            "contract_stages": budget_profile.get("contract_stages", []),
            "execution_mode": execution_mode,
            "execution_profile": execution_profile,
            "stop_conditions": stop_conditions[:12],
            "stop_condition_reason": stop_condition_reason,
            "allow_provider_reroute": bool(allow_provider_reroute),
            "actions": actions[:16],
        }

    def _resolve_remediation_checkpoint_mode(
        self,
        *,
        metadata: Dict[str, Any],
        budget_profile: Dict[str, Any],
        planned: List[Dict[str, Any]],
    ) -> str:
        runtime_meta = metadata if isinstance(metadata, dict) else {}
        mode_raw = str(runtime_meta.get("external_remediation_checkpoint_mode", "auto")).strip().lower()
        if mode_raw in {"off", "none", "disabled"}:
            return "off"
        if mode_raw in {"strict", "enforce"}:
            return "strict"
        if mode_raw in {"standard", "on", "enabled"}:
            return "standard"

        contract_risk = self._coerce_float(
            budget_profile.get("contract_risk", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        mission_profile = str(budget_profile.get("mission_profile", "")).strip().lower()
        if contract_risk >= 0.7 or mission_profile in {"defensive"}:
            return "strict"
        if contract_risk >= 0.34 or len(planned) >= 2:
            return "standard"
        return "off"

    def _remediation_checkpoint_provider(
        self,
        *,
        step: PlanStep,
        action_row: Dict[str, Any],
        metadata: Dict[str, Any],
    ) -> str:
        row = action_row if isinstance(action_row, dict) else {}
        args_payload = row.get("args", {})
        args = args_payload if isinstance(args_payload, dict) else {}
        provider = str(args.get("provider", "")).strip().lower()
        if provider and provider != "auto":
            return provider
        step_args = step.args if isinstance(step.args, dict) else {}
        provider = str(step_args.get("provider", "")).strip().lower()
        if provider and provider != "auto":
            return provider
        provider = str(metadata.get("__external_selected_provider", "")).strip().lower() if isinstance(metadata, dict) else ""
        if provider and provider != "auto":
            return provider
        return ""

    async def _run_external_remediation_checkpoint(
        self,
        *,
        step: PlanStep,
        metadata: Dict[str, Any],
        source: str,
        attempt: int,
        phase: str,
        contract_risk: float,
        strict: bool,
        provider: str = "",
    ) -> Dict[str, Any]:
        args: Dict[str, Any] = {"action": str(step.action or "").strip().lower()}
        clean_provider = str(provider or "").strip().lower()
        if clean_provider and clean_provider != "auto":
            args["provider"] = clean_provider
        else:
            args["provider"] = "auto"
        checkpoint_meta = dict(metadata if isinstance(metadata, dict) else {})
        checkpoint_meta["__skip_approval"] = True
        checkpoint_meta["__external_remediation"] = True
        checkpoint_meta["__external_remediation_checkpoint"] = True
        checkpoint_meta["__external_remediation_phase"] = str(phase or "").strip().lower()
        timeout_s = 16 if strict else 12
        result = await self._execute_aux_action(
            action="external_connector_preflight",
            args=args,
            source=f"{source}:external-remediation-checkpoint",
            metadata=checkpoint_meta,
            timeout_s=timeout_s,
            attempt=attempt,
        )
        output = result.output if isinstance(result.output, dict) else {}
        payload = {
            "action": "external_connector_preflight",
            "phase": str(phase or "").strip().lower(),
            "provider": clean_provider,
            "status": str(result.status or "").strip().lower(),
            "error": str(result.error or "").strip(),
            "strict": bool(strict),
            "contract_risk": round(max(0.0, min(1.0, float(contract_risk))), 6),
            "output": output,
        }
        self.telemetry.emit(
            "step.external_remediation_checkpoint",
            {
                "step_id": step.step_id,
                "parent_action": step.action,
                "phase": payload["phase"],
                "provider": clean_provider,
                "status": payload["status"],
                "strict": bool(strict),
            },
        )
        return payload

    def _memory_repair_patch(
        self,
        *,
        step: PlanStep,
        metadata: Dict[str, Any],
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        clean_action = str(step.action or "").strip().lower()
        if not self._is_external_or_oauth_action(clean_action):
            return ({}, {})
        rows = metadata.get("repair_memory_hints")
        if not isinstance(rows, list) or not rows:
            return ({}, {})
        best_patch: Dict[str, Any] = {}
        best_score = -1.0
        best_meta: Dict[str, Any] = {}
        for row in rows[:12]:
            if not isinstance(row, dict):
                continue
            signals = row.get("signals", [])
            if not isinstance(signals, list):
                continue
            for signal in signals:
                if not isinstance(signal, dict):
                    continue
                action_name = str(signal.get("action", "")).strip().lower()
                if action_name != clean_action:
                    continue
                args_payload = signal.get("args", {})
                if not isinstance(args_payload, dict) or not args_payload:
                    continue
                status = str(signal.get("status", "")).strip().lower()
                base_score = 0.0
                if status == "success":
                    base_score += 0.65
                elif status == "failed":
                    base_score += 0.14
                else:
                    base_score += 0.24
                base_score += self._coerce_float(signal.get("score", 0.0), minimum=0.0, maximum=2.0, default=0.0)
                if base_score > best_score:
                    best_score = base_score
                    best_patch = dict(args_payload)
                    best_meta = {
                        "memory_id": str(row.get("memory_id", "")).strip(),
                        "signal_status": status,
                        "signal_score": round(base_score, 6),
                    }
        safe_patch = self._sanitize_external_repair_patch(step=step, patch=best_patch)
        return (safe_patch, best_meta)

    @staticmethod
    def _sanitize_external_repair_patch(*, step: PlanStep, patch: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(patch, dict) or not patch:
            return {}
        args = step.args if isinstance(step.args, dict) else {}
        allowed_keys = set(args.keys()) | {
            "provider",
            "max_results",
            "query",
            "calendar_id",
            "tasklist_id",
            "list_id",
            "todo_list_id",
            "timezone",
            "time_min",
            "time_max",
            "endpoint",
            "metadata_endpoint",
            "content_endpoint",
        }
        safe: Dict[str, Any] = {}
        for key, value in patch.items():
            clean_key = str(key).strip()
            if not clean_key or clean_key not in allowed_keys:
                continue
            if value is None:
                continue
            current = args.get(clean_key)
            if current == value:
                continue
            safe[clean_key] = value
        return safe

    def _enforce_external_branch_guard(
        self,
        *,
        step: PlanStep,
        metadata: Dict[str, Any],
        rollback_profile: Dict[str, Any],
        attempt: int,
    ) -> tuple[ActionResult | None, Dict[str, Any]]:
        clean_action = str(step.action or "").strip().lower()
        if not self.external_branch_guard_enabled or not self._is_external_mutation_action(clean_action):
            return (None, {})

        high_impact = self._is_high_impact_external_action(clean_action)
        profile_requires = bool(rollback_profile.get("requires_branch", False)) if isinstance(rollback_profile, dict) else False
        if not high_impact and not profile_requires:
            return (None, {})

        strategy = str(metadata.get("external_branch_strategy", "warn")).strip().lower() or "warn"
        enforce = bool(self.external_branch_guard_enforce or strategy in {"enforce", "strict", "required"})
        branch_id = str(
            metadata.get("external_branch_id")
            or metadata.get("__mission_id")
            or metadata.get("__goal_id")
            or ""
        ).strip()
        acknowledged = self._coerce_bool(metadata.get("external_branch_ack", False), default=False)

        context: Dict[str, Any] = {
            "action": clean_action,
            "requires_branch": True,
            "high_impact": bool(high_impact),
            "strategy": strategy,
            "enforced": bool(enforce),
            "branch_id": branch_id,
            "acknowledged": bool(acknowledged),
            "rollback_supported": bool(rollback_profile.get("rollback_supported", False))
            if isinstance(rollback_profile, dict)
            else False,
            "reversible": bool(rollback_profile.get("reversible", False))
            if isinstance(rollback_profile, dict)
            else False,
            "branch_reason": str(rollback_profile.get("branch_reason", "")).strip()
            if isinstance(rollback_profile, dict)
            else "",
        }

        if branch_id:
            context["status"] = "attached"
            return (None, context)

        if acknowledged and not enforce:
            context["status"] = "acknowledged"
            return (None, context)

        if enforce and not acknowledged:
            message = (
                f"External branch guard blocked '{clean_action}' because the action is non-reversible/high-impact. "
                "Set metadata.external_branch_ack=true or provide metadata.external_branch_id to continue."
            )
            blocked = ActionResult(
                action=clean_action,
                status="blocked",
                error=message,
                output={
                    "status": "error",
                    "message": message,
                    "branch_guard": context,
                },
                attempt=attempt,
            )
            context["status"] = "blocked"
            self.telemetry.emit(
                "step.external_branch_guard_blocked",
                {
                    "step_id": step.step_id,
                    "action": clean_action,
                    "strategy": strategy,
                    "high_impact": bool(high_impact),
                    "branch_reason": context.get("branch_reason", ""),
                },
            )
            return (blocked, context)

        context["status"] = "warning"
        self.telemetry.emit(
            "step.external_branch_guard_warning",
            {
                "step_id": step.step_id,
                "action": clean_action,
                "strategy": strategy,
                "high_impact": bool(high_impact),
                "branch_reason": context.get("branch_reason", ""),
            },
        )
        return (None, context)

    async def _run_external_mutation_simulation(
        self,
        *,
        step: PlanStep,
        source: str,
        metadata: Dict[str, Any],
        attempt: int,
        rollback_profile: Dict[str, Any],
        branch_context: Dict[str, Any],
    ) -> tuple[ActionResult | None, Dict[str, Any]]:
        if not self._should_run_external_mutation_simulation(step=step, metadata=metadata):
            return (None, {})

        step_args = step.args if isinstance(step.args, dict) else {}
        sim_args = dict(step_args)
        sim_args["dry_run"] = True
        sim_metadata = dict(metadata)
        sim_metadata["__skip_approval"] = True
        sim_metadata["__external_mutation_simulation"] = True
        sim_request = ActionRequest(
            action=step.action,
            args=sim_args,
            source=f"{source}:external-simulation",
            metadata=sim_metadata,
        )

        allowed, reason = self.policy_guard.authorize(sim_request)
        if not allowed:
            message = f"External mutation simulation policy block: {reason}"
            blocked = ActionResult(
                action=step.action,
                status="blocked",
                error=message,
                output={"status": "error", "message": message},
                attempt=attempt,
            )
            blocked.evidence["external_mutation_simulation_only"] = True
            blocked.evidence["external_mutation_simulation"] = {
                "status": "blocked",
                "dry_run": True,
                "policy_blocked": True,
                "message": message,
            }
            if rollback_profile:
                blocked.evidence["rollback_profile"] = rollback_profile
            if branch_context:
                blocked.evidence["rollback_branch"] = branch_context
            self.telemetry.emit(
                "step.external_mutation_simulation",
                {
                    "step_id": step.step_id,
                    "action": step.action,
                    "status": "blocked",
                    "policy_blocked": True,
                },
            )
            return (blocked, blocked.evidence.get("external_mutation_simulation", {}))

        timeout_s = min(
            int(step.timeout_s or 30),
            int(self.external_mutation_sim_timeout_s or 25),
        )
        timeout_s = max(4, min(timeout_s, 120))
        sim_result = await self.registry.execute(sim_request, timeout_s=timeout_s)
        sim_result.attempt = attempt
        sim_output = sim_result.output if isinstance(sim_result.output, dict) else {}
        sim_payload: Dict[str, Any] = {
            "status": sim_result.status,
            "dry_run": True,
            "duration_ms": max(0, int(sim_result.duration_ms or 0)),
            "provider": str(sim_output.get("provider", "")).strip().lower(),
        }
        if isinstance(sim_output.get("simulation"), dict):
            sim_payload["simulation"] = dict(sim_output.get("simulation", {}))

        args_patch = self._extract_args_patch_from_simulation(sim_output)
        if args_patch:
            if isinstance(step.args, dict):
                step.args.update(args_patch)
            sim_payload["applied_args_patch"] = dict(args_patch)
        elif isinstance(sim_payload.get("simulation"), dict):
            nested = sim_payload["simulation"]
            if isinstance(nested, dict):
                nested_patch = self._extract_args_patch_from_simulation({"simulation": nested})
                if nested_patch:
                    if isinstance(step.args, dict):
                        step.args.update(nested_patch)
                    sim_payload["applied_args_patch"] = dict(nested_patch)

        if sim_result.status != "success":
            message = str(sim_result.error or sim_output.get("message") or "External mutation simulation failed.").strip()
            blocked_status = "blocked" if sim_result.status == "blocked" else "failed"
            should_block = self._simulation_failure_should_block(
                status=sim_result.status,
                error=message,
                output=sim_output if isinstance(sim_output, dict) else {},
            )
            if not should_block:
                sim_payload["status"] = "bypass"
                sim_payload["bypass_reason"] = message
                self.telemetry.emit(
                    "step.external_mutation_simulation",
                    {
                        "step_id": step.step_id,
                        "action": step.action,
                        "status": "bypass",
                        "duration_ms": sim_payload.get("duration_ms", 0),
                        "provider": sim_payload.get("provider", ""),
                    },
                )
                return (None, sim_payload)
            blocked = ActionResult(
                action=step.action,
                status=blocked_status,
                error=f"External mutation simulation blocked commit: {message}",
                output={
                    "status": "error",
                    "message": f"External mutation simulation blocked commit: {message}",
                    "external_mutation_simulation": sim_output if isinstance(sim_output, dict) else {},
                },
                attempt=attempt,
            )
            blocked.evidence["external_mutation_simulation_only"] = True
            blocked.evidence["external_mutation_simulation"] = dict(sim_payload)
            if rollback_profile:
                blocked.evidence["rollback_profile"] = rollback_profile
            if branch_context:
                blocked.evidence["rollback_branch"] = branch_context
            self.telemetry.emit(
                "step.external_mutation_simulation",
                {
                    "step_id": step.step_id,
                    "action": step.action,
                    "status": blocked_status,
                    "duration_ms": sim_payload.get("duration_ms", 0),
                    "provider": sim_payload.get("provider", ""),
                },
            )
            return (blocked, sim_payload)

        self.telemetry.emit(
            "step.external_mutation_simulation",
            {
                "step_id": step.step_id,
                "action": step.action,
                "status": "success",
                "duration_ms": sim_payload.get("duration_ms", 0),
                "provider": sim_payload.get("provider", ""),
                "patched": bool(sim_payload.get("applied_args_patch")),
            },
        )
        return (None, sim_payload)

    def _external_preflight(self, *, step: PlanStep, metadata: Dict[str, Any]) -> Dict[str, Any]:
        if self.external_reliability is None:
            return {"status": "skip"}
        try:
            return self.external_reliability.preflight(
                action=step.action,
                args=step.args if isinstance(step.args, dict) else {},
                metadata=metadata if isinstance(metadata, dict) else {},
            )
        except Exception as exc:  # noqa: BLE001
            self.telemetry.emit(
                "external_reliability.preflight_error",
                {
                    "action": step.action,
                    "step_id": step.step_id,
                    "message": str(exc),
                },
            )
            return {"status": "skip"}

    def _record_external_outcome(self, *, step: PlanStep, metadata: Dict[str, Any], result: ActionResult) -> None:
        if self.external_reliability is None:
            return
        try:
            runtime_metadata = metadata if isinstance(metadata, dict) else {}
            reliability_metadata: Dict[str, Any] = dict(runtime_metadata)
            reliability_metadata["__result_duration_ms"] = max(0, int(result.duration_ms or 0))
            reliability_metadata["__result_attempt"] = max(1, int(result.attempt or 1))
            evidence = result.evidence if isinstance(result.evidence, dict) else {}
            if bool(evidence.get("external_mutation_simulation_only", False)):
                return
            preflight_evidence = evidence.get("external_reliability_preflight", {})
            if isinstance(preflight_evidence, dict):
                routing = preflight_evidence.get("provider_routing", {})
                if isinstance(routing, dict):
                    reliability_metadata["__external_route_strategy"] = str(routing.get("strategy", "")).strip().lower()
                    reliability_metadata["__external_selected_provider"] = (
                        str(routing.get("selected_provider", "")).strip().lower()
                    )
                    reliability_metadata["__external_selected_health_score"] = routing.get("selected_health_score")
                retry_contract = preflight_evidence.get("retry_contract", {})
                if isinstance(retry_contract, dict):
                    reliability_metadata["__external_retry_contract_mode"] = str(
                        retry_contract.get("mode", "")
                    ).strip().lower()
                    try:
                        reliability_metadata["__external_retry_contract_risk"] = float(
                            retry_contract.get("risk_score", 0.0) or 0.0
                        )
                    except Exception:
                        reliability_metadata["__external_retry_contract_risk"] = 0.0
                    budget = retry_contract.get("budget", {})
                    budget_row = budget if isinstance(budget, dict) else {}
                    try:
                        reliability_metadata["__external_retry_contract_max_attempts"] = int(
                            budget_row.get("max_attempts", 0) or 0
                        )
                    except Exception:
                        reliability_metadata["__external_retry_contract_max_attempts"] = 0
                    try:
                        reliability_metadata["__external_retry_contract_cooldown_s"] = float(
                            budget_row.get("cooldown_recommendation_s", 0.0) or 0.0
                        )
                    except Exception:
                        reliability_metadata["__external_retry_contract_cooldown_s"] = 0.0
            confirm_policy = evidence.get("confirm_policy", {})
            if isinstance(confirm_policy, dict):
                reliability_metadata["__confirm_policy_mode"] = str(confirm_policy.get("mode", "")).strip().lower()
                reliability_metadata["__confirm_policy_satisfied"] = bool(confirm_policy.get("satisfied", False))
                reliability_metadata["__confirm_policy_total_count"] = int(confirm_policy.get("total_count", 0) or 0)

            output_payload = result.output if isinstance(result.output, dict) else {}
            if reliability_metadata["__result_duration_ms"] > 0 and "duration_ms" not in output_payload:
                output_payload = dict(output_payload)
                output_payload["duration_ms"] = reliability_metadata["__result_duration_ms"]
            payload = self.external_reliability.record_outcome(
                action=step.action,
                args=step.args if isinstance(step.args, dict) else {},
                status=str(result.status or "").strip().lower(),
                error=result.error or "",
                output=output_payload,
                metadata=reliability_metadata,
            )
            if isinstance(payload, dict):
                cooldown_rows = payload.get("cooldowns", [])
                if isinstance(cooldown_rows, list):
                    for row in cooldown_rows:
                        if not isinstance(row, dict):
                            continue
                        self.telemetry.emit(
                            "external_reliability.cooldown",
                            {
                                "action": step.action,
                                "provider": str(row.get("provider", "")),
                                "cooldown_s": int(row.get("cooldown_s", 0) or 0),
                                "category": str(row.get("category", "")),
                                "failure_ema": float(row.get("failure_ema", 0.0) or 0.0),
                                "consecutive_failures": int(row.get("consecutive_failures", 0) or 0),
                            },
                        )
        except Exception as exc:  # noqa: BLE001
            self.telemetry.emit(
                "external_reliability.record_error",
                {
                    "action": step.action,
                    "step_id": step.step_id,
                    "message": str(exc),
                },
            )

    async def _prepare_desktop_anchor(
        self,
        *,
        step: PlanStep,
        source: str,
        metadata: Dict[str, Any],
        attempt: int,
    ) -> Dict[str, Any]:
        if step.action not in {"computer_click_target", "computer_click_text", "accessibility_invoke_element"}:
            return {}

        query = ""
        for key in ("query", "text", "target"):
            value = step.args.get(key) if isinstance(step.args, dict) else ""
            if isinstance(value, str) and value.strip():
                query = value.strip()
                break

        memory_evidence: Dict[str, Any] = {}
        memory_hint = self._lookup_desktop_anchor_memory(step=step, query=query, metadata=metadata)
        force_probe_from_memory = False
        transition_contract = self._desktop_transition_contract_for_action(
            action=step.action,
            metadata=metadata,
            limit=16,
        )
        preferred_probe = str(transition_contract.get("preferred_probe", "hybrid")).strip().lower() if isinstance(transition_contract, dict) else "hybrid"
        require_dual_probe = bool(transition_contract.get("require_dual_probe", False)) if isinstance(transition_contract, dict) else False
        if isinstance(transition_contract, dict) and transition_contract:
            memory_evidence["transition_contract"] = {
                "volatility_score": self._coerce_float(
                    transition_contract.get("volatility_score", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                "preferred_probe": preferred_probe,
                "require_dual_probe": bool(require_dual_probe),
                "force_probe": bool(transition_contract.get("force_probe", False)),
            }
            if bool(transition_contract.get("force_probe", False)):
                force_probe_from_memory = True
        if isinstance(memory_hint, dict) and memory_hint:
            patch = memory_hint.get("patch")
            if isinstance(patch, dict) and patch:
                step.args.update(patch)
                memory_evidence["args_patch"] = dict(patch)
            row = memory_hint.get("row")
            if isinstance(row, dict):
                memory_evidence["row"] = row
            invalidation = memory_hint.get("invalidation")
            if isinstance(invalidation, dict):
                memory_evidence["invalidation"] = dict(invalidation)
            if "score" in memory_hint:
                memory_evidence["score"] = memory_hint.get("score")
            memory_viability_policy = str(memory_hint.get("viability_policy", "")).strip().lower()
            if memory_viability_policy:
                memory_evidence["viability_policy"] = memory_viability_policy
            memory_risk_score = self._coerce_float(
                memory_hint.get("risk_score", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            if memory_risk_score > 0.0:
                memory_evidence["risk_score"] = round(memory_risk_score, 6)
            invalidation_flags_raw = memory_hint.get("invalidation_flags", [])
            invalidation_flags = [
                str(item).strip().lower()
                for item in invalidation_flags_raw
                if str(item).strip()
            ] if isinstance(invalidation_flags_raw, list) else []
            if invalidation_flags:
                memory_evidence["invalidation_flags"] = invalidation_flags[:16]
            force_probe_from_memory = bool(memory_hint.get("force_probe", False))
            if force_probe_from_memory:
                memory_evidence["force_probe"] = True

        verify_cfg = step.verify if isinstance(step.verify, dict) else {}
        raw_anchor = verify_cfg.get("desktop_anchor")
        anchor_cfg = raw_anchor if isinstance(raw_anchor, dict) else {}
        guardrail_cfg = verify_cfg.get("guardrail")
        guardrail_level = str(guardrail_cfg.get("level", "")).strip().lower() if isinstance(guardrail_cfg, dict) else ""
        runtime_guardrail_level = str(metadata.get("guardrail_recommended_level", "")).strip().lower()
        strict_guardrail = guardrail_level in {"strict", "high", "critical"} or runtime_guardrail_level in {"high", "critical"}
        if not anchor_cfg and not strict_guardrail and not force_probe_from_memory:
            if memory_evidence:
                return {"evidence": {"memory_hint": memory_evidence}}
            return {}

        enabled = True
        if isinstance(anchor_cfg, dict) and "enabled" in anchor_cfg:
            enabled = bool(anchor_cfg.get("enabled"))
        if not enabled and not force_probe_from_memory:
            if memory_evidence:
                return {"evidence": {"memory_hint": memory_evidence}}
            return {}

        required = bool(anchor_cfg.get("required", strict_guardrail)) if isinstance(anchor_cfg, dict) else strict_guardrail
        min_confidence = 0.58 if required else 0.42
        relaxed_min_confidence = 0.32 if required else 0.24
        if strict_guardrail:
            min_confidence = max(min_confidence, 0.64)
            relaxed_min_confidence = max(relaxed_min_confidence, 0.42)
        transition_volatility = self._coerce_float(
            transition_contract.get("volatility_score", 0.0) if isinstance(transition_contract, dict) else 0.0,
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        if transition_volatility >= 0.52:
            min_confidence = max(min_confidence, 0.66)
            relaxed_min_confidence = max(relaxed_min_confidence, 0.44)
            required = True
        if force_probe_from_memory:
            min_confidence = max(min_confidence, 0.52)
            relaxed_min_confidence = max(relaxed_min_confidence, 0.35)
        if isinstance(anchor_cfg, dict):
            try:
                min_confidence = float(anchor_cfg.get("min_confidence", min_confidence))
            except Exception:
                pass
            try:
                relaxed_min_confidence = float(anchor_cfg.get("fallback_min_confidence", relaxed_min_confidence))
            except Exception:
                pass
        min_confidence = max(0.0, min(min_confidence, 1.0))
        relaxed_min_confidence = max(0.0, min(relaxed_min_confidence, 1.0))
        if isinstance(anchor_cfg, dict):
            query = str(anchor_cfg.get("query", "")).strip()
        if not query:
            for key in ("query", "text", "target"):
                value = step.args.get(key) if isinstance(step.args, dict) else ""
                if isinstance(value, str) and value.strip():
                    query = value.strip()
                    break
        query_token = re.fullmatch(r"\{\{\s*args\.([a-zA-Z0-9_.-]+)\s*\}\}", query)
        if query_token and isinstance(step.args, dict):
            resolved_query = self._resolve_path(step.args, query_token.group(1))
            if isinstance(resolved_query, str) and resolved_query.strip():
                query = resolved_query.strip()
        if not query:
            if memory_evidence:
                return {"evidence": {"memory_hint": memory_evidence}}
            return {}

        configured_action = str(anchor_cfg.get("action", "")).strip() if isinstance(anchor_cfg, dict) else ""
        action_name = configured_action or "accessibility_find_element"
        if not configured_action:
            if preferred_probe == "ocr":
                action_name = "computer_find_text_targets"
            elif preferred_probe == "accessibility":
                action_name = "accessibility_find_element"
        if action_name not in {"accessibility_find_element", "computer_find_text_targets"}:
            action_name = "accessibility_find_element"
        timeout_s = int(anchor_cfg.get("timeout_s", min(step.timeout_s, 12))) if isinstance(anchor_cfg, dict) else min(step.timeout_s, 12)
        timeout_s = max(2, min(timeout_s, 30))
        if require_dual_probe:
            timeout_s = max(timeout_s, 10)

        window_title = str(step.args.get("window_title", "")).strip() if isinstance(step.args, dict) else ""
        control_type = str(step.args.get("control_type", "")).strip() if isinstance(step.args, dict) else ""
        probe_chain: List[Dict[str, Any]] = []
        probe_specs: List[Dict[str, Any]] = [
            self._build_anchor_probe(
                action_name=action_name,
                query=query,
                window_title=window_title,
                control_type=control_type,
                step=step,
                timeout_s=timeout_s,
            )
        ]
        alternate = "computer_find_text_targets" if action_name == "accessibility_find_element" else "accessibility_find_element"
        if required or strict_guardrail or force_probe_from_memory or require_dual_probe:
            probe_specs.append(
                self._build_anchor_probe(
                    action_name=alternate,
                    query=query,
                    window_title=window_title,
                    control_type=control_type,
                    step=step,
                    timeout_s=max(2, min(timeout_s + 2, 16)),
                )
            )

        accepted_patch: Dict[str, Any] = {}
        accepted_confidence = 0.0
        accepted_action = ""
        accepted_output: Dict[str, Any] = {}
        accepted_status = ""
        accepted_error = ""
        accepted_probe_hits = 0
        preferred_probe_action = "computer_find_text_targets" if preferred_probe == "ocr" else "accessibility_find_element"
        for index, probe in enumerate(probe_specs):
            probe_action = str(probe.get("action", "")).strip()
            probe_args = probe.get("args", {})
            probe_timeout = int(probe.get("timeout_s", timeout_s) or timeout_s)
            if not probe_action or not isinstance(probe_args, dict):
                continue
            anchor_result = await self._execute_aux_action(
                action=probe_action,
                args=probe_args,
                source=f"{source}:anchor",
                metadata=metadata,
                timeout_s=max(2, min(probe_timeout, 30)),
                attempt=attempt,
            )
            output = anchor_result.output if isinstance(anchor_result.output, dict) else {}
            patch, confidence = self._extract_anchor_patch(
                step_action=step.action,
                probe_action=probe_action,
                output=output,
            )
            probe_row: Dict[str, Any] = {
                "action": probe_action,
                "status": anchor_result.status,
                "error": anchor_result.error,
                "query": query,
                "confidence": round(confidence, 6),
                "output": output,
            }
            if patch:
                probe_row["args_patch"] = dict(patch)
            probe_chain.append(probe_row)
            if anchor_result.status != "success":
                if not accepted_status:
                    accepted_status = anchor_result.status
                    accepted_error = anchor_result.error
                continue
            min_required = min_confidence if index == 0 else relaxed_min_confidence
            if patch and confidence >= min_required:
                accepted_probe_hits += 1
                prefer_current = bool(
                    preferred_probe in {"ocr", "accessibility"}
                    and probe_action == preferred_probe_action
                )
                if prefer_current and confidence >= max(0.0, accepted_confidence - 0.08):
                    accepted_patch = dict(patch)
                    accepted_confidence = confidence
                    accepted_action = probe_action
                    accepted_output = output
                    accepted_status = anchor_result.status
                    accepted_error = anchor_result.error
                elif confidence > accepted_confidence:
                    accepted_patch = dict(patch)
                    accepted_confidence = confidence
                    accepted_action = probe_action
                    accepted_output = output
                    accepted_status = anchor_result.status
                    accepted_error = anchor_result.error
                if not require_dual_probe or accepted_probe_hits >= 2:
                    break
            if patch and confidence > accepted_confidence:
                accepted_patch = dict(patch)
                accepted_confidence = confidence
                accepted_action = probe_action
                accepted_output = output
                accepted_status = anchor_result.status
                accepted_error = anchor_result.error

        evidence: Dict[str, Any] = {
            "status": accepted_status or ("failed" if required else "degraded"),
            "action": accepted_action or action_name,
            "query": query,
            "error": accepted_error,
            "output": accepted_output if isinstance(accepted_output, dict) else {},
            "chain": probe_chain,
            "confidence": round(accepted_confidence, 6),
            "required": bool(required),
            "strict_guardrail": bool(strict_guardrail),
            "probe_forced": bool(force_probe_from_memory),
            "transition_contract": transition_contract if isinstance(transition_contract, dict) else {},
            "dual_probe_required": bool(require_dual_probe),
            "dual_probe_hits": int(accepted_probe_hits),
            "min_confidence": round(min_confidence, 6),
            "fallback_min_confidence": round(relaxed_min_confidence, 6),
            "pre_state_hash": str(metadata.get("__desktop_pre_state_hash", "")).strip().lower(),
        }
        if memory_evidence:
            evidence["memory_hint"] = memory_evidence
        dual_probe_satisfied = bool(not require_dual_probe or accepted_probe_hits >= 2)
        high_confidence_override = bool(
            accepted_confidence >= max(0.92, min(0.98, min_confidence + 0.12))
        )
        if accepted_patch and (not required or accepted_confidence >= relaxed_min_confidence) and (
            dual_probe_satisfied or high_confidence_override
        ):
            step.args.update(accepted_patch)
            evidence["args_patch"] = dict(accepted_patch)
            evidence["status"] = "success"
            evidence["action"] = accepted_action or action_name
            if len(probe_chain) > 1:
                evidence["fallback_used"] = bool(accepted_action and accepted_action != action_name)
            if require_dual_probe and not dual_probe_satisfied and high_confidence_override:
                evidence["dual_probe_override"] = True
            return {"evidence": evidence}
        if require_dual_probe and accepted_patch and not dual_probe_satisfied:
            evidence["dual_probe_unsatisfied"] = True

        memory_score = 0.0
        if isinstance(memory_hint, dict):
            try:
                memory_score = float(memory_hint.get("score", 0.0) or 0.0)
            except Exception:
                memory_score = 0.0
        memory_policy = (
            str(memory_hint.get("viability_policy", "use")).strip().lower()
            if isinstance(memory_hint, dict)
            else "use"
        )
        memory_risk = self._coerce_float(
            memory_hint.get("risk_score", 0.0) if isinstance(memory_hint, dict) else 0.0,
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        memory_threshold = 0.82 if required else 0.64
        if memory_policy in {"use_with_probe", "skip"}:
            memory_threshold = max(memory_threshold, 0.9)
        if force_probe_from_memory:
            memory_threshold = max(memory_threshold, 0.95)
        memory_threshold = min(0.99, memory_threshold + min(0.12, memory_risk * 0.18))
        memory_fallback_allowed = memory_policy not in {"use_with_probe", "skip"} and not force_probe_from_memory
        if memory_score >= memory_threshold and memory_evidence.get("args_patch") and memory_fallback_allowed:
            evidence["memory_fallback_used"] = True
            evidence["status"] = "success"
            return {"evidence": evidence}
        if memory_evidence.get("args_patch") and not memory_fallback_allowed:
            evidence["memory_fallback_blocked"] = True
            evidence["memory_fallback_blocked_reason"] = "viability_policy_requires_probe"
        if required:
            failure_reason = "no actionable anchor found"
            if probe_chain:
                last_error = str(probe_chain[-1].get("error", "")).strip()
                if last_error:
                    failure_reason = last_error
            return {
                "result": ActionResult(
                    action=step.action,
                    status="failed",
                    error=(
                        f"Desktop anchor check failed before action '{step.action}': {failure_reason}. "
                        f"Probe attempts={len(probe_chain)}."
                    ),
                    output={"status": "error", "desktop_anchor": evidence},
                    attempt=attempt,
                ),
                "failure_category": "non_retryable",
                "evidence": evidence,
            }
        return {"evidence": evidence}

    @staticmethod
    def _build_anchor_probe(
        *,
        action_name: str,
        query: str,
        window_title: str,
        control_type: str,
        step: PlanStep,
        timeout_s: int,
    ) -> Dict[str, Any]:
        if action_name == "accessibility_find_element":
            args: Dict[str, Any] = {"query": query, "max_results": 5}
            if window_title:
                args["window_title"] = window_title
            if control_type:
                args["control_type"] = control_type
            return {"action": action_name, "args": args, "timeout_s": timeout_s}
        args = {"query": query, "match_mode": "contains", "max_results": 8}
        if isinstance(step.args, dict) and "region" in step.args:
            args["region"] = step.args.get("region")
        return {"action": "computer_find_text_targets", "args": args, "timeout_s": timeout_s}

    @staticmethod
    def _extract_anchor_patch(
        *,
        step_action: str,
        probe_action: str,
        output: Dict[str, Any],
    ) -> tuple[Dict[str, Any], float]:
        patch: Dict[str, Any] = {}
        confidence = 0.0
        clean_step_action = str(step_action or "").strip().lower()
        if probe_action == "accessibility_find_element":
            items = output.get("items", [])
            first = items[0] if isinstance(items, list) and items and isinstance(items[0], dict) else {}
            if isinstance(first, dict):
                element_id = str(first.get("element_id", "")).strip()
                if element_id:
                    patch["element_id"] = element_id
                match_score = first.get("match_score")
                try:
                    confidence = float(match_score)
                except Exception:
                    confidence = 0.0
                confidence = max(0.0, min(1.0, confidence))
                if clean_step_action == "computer_click_target":
                    patch["target_mode"] = "accessibility"
        elif probe_action == "computer_find_text_targets":
            targets = output.get("targets", [])
            first = targets[0] if isinstance(targets, list) and targets and isinstance(targets[0], dict) else {}
            if isinstance(first, dict):
                try:
                    confidence = float(first.get("confidence", 0.0) or 0.0)
                except Exception:
                    confidence = 0.0
                confidence = max(0.0, min(1.0, confidence))
                if clean_step_action == "computer_click_text":
                    patch["target_index"] = 0
                    if "center_x" in first and "center_y" in first:
                        patch["x"] = first.get("center_x")
                        patch["y"] = first.get("center_y")
                elif clean_step_action == "computer_click_target":
                    patch["target_mode"] = "ocr"
                    patch["target_index"] = 0
        return patch, confidence

    def _lookup_desktop_anchor_memory(
        self,
        *,
        step: PlanStep,
        query: str,
        metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        if self.desktop_anchor_memory is None:
            return {}
        if not query:
            return {}
        try:
            payload = self.desktop_anchor_memory.lookup(
                action=step.action,
                args=step.args if isinstance(step.args, dict) else {},
                metadata=metadata,
                limit=1,
            )
        except Exception as exc:  # noqa: BLE001
            self.telemetry.emit(
                "desktop_anchor.lookup_error",
                {
                    "action": step.action,
                    "step_id": step.step_id,
                    "message": str(exc),
                },
            )
            return {}
        if not isinstance(payload, dict) or str(payload.get("status", "")).strip().lower() != "success":
            return {}
        items = payload.get("items", [])
        if not isinstance(items, list) or not items or not isinstance(items[0], dict):
            return {}
        row = items[0]
        viability_policy = str(row.get("viability_policy", "use")).strip().lower() or "use"
        risk_score = self._coerce_float(
            row.get("risk_score", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        invalidation_flags_raw = row.get("invalidation_flags", [])
        invalidation_flags = [
            str(item).strip().lower()
            for item in invalidation_flags_raw
            if str(item).strip()
        ] if isinstance(invalidation_flags_raw, list) else []
        force_probe = bool(
            viability_policy == "use_with_probe"
            or risk_score >= 0.62
            or any(
                flag in {
                    "transition_profile_high_churn",
                    "transition_profile_low_success",
                    "guardrail_transition_volatility",
                    "guardrail_context_shift",
                    "guardrail_anchor_fallback_failed",
                }
                for flag in invalidation_flags
            )
        )
        patch: Dict[str, Any] = {}
        element_id = str(row.get("element_id", "")).strip()
        if element_id and not str(step.args.get("element_id", "")).strip():
            patch["element_id"] = element_id
        if step.action == "computer_click_target":
            target_mode = str(row.get("target_mode", "")).strip().lower()
            if target_mode in {"accessibility", "ocr_text"}:
                patch["target_mode"] = target_mode
        if step.action == "computer_click_text":
            x_val = row.get("x")
            y_val = row.get("y")
            if x_val is not None and y_val is not None:
                patch["x"] = x_val
                patch["y"] = y_val
                patch["target_index"] = 0
        try:
            raw_score = float(row.get("match_score", 0.0) or 0.0)
        except Exception:
            raw_score = 0.0
        normalized_score = max(0.0, min(1.0, raw_score / 1.2))
        invalidation = self._evaluate_desktop_anchor_memory_invalidation(
            row=row,
            step=step,
            metadata=metadata,
            normalized_score=normalized_score,
        )
        if bool(invalidation.get("invalidated", False)):
            self._record_desktop_anchor_invalidation_feedback(
                step=step,
                metadata=metadata,
                invalidation=invalidation,
            )
            quarantine_payload = self._quarantine_desktop_anchor_memory(
                step=step,
                metadata=metadata,
                invalidation=invalidation,
            )
            if quarantine_payload:
                invalidation = dict(invalidation)
                invalidation["quarantine"] = quarantine_payload
            return {
                "row": row,
                "patch": {},
                "score": 0.0,
                "raw_score": max(0.0, min(5.0, raw_score)),
                "invalidation": invalidation,
                "viability_policy": viability_policy,
                "risk_score": round(risk_score, 6),
                "invalidation_flags": invalidation_flags[:16],
                "force_probe": bool(force_probe),
            }
        return {
            "row": row,
            "patch": patch,
            "score": normalized_score,
            "raw_score": max(0.0, min(5.0, raw_score)),
            "invalidation": invalidation,
            "viability_policy": viability_policy,
            "risk_score": round(risk_score, 6),
            "invalidation_flags": invalidation_flags[:16],
            "force_probe": bool(force_probe),
        }

    @staticmethod
    def _evaluate_desktop_anchor_memory_invalidation(
        *,
        row: Dict[str, Any],
        step: PlanStep,
        metadata: Dict[str, Any],
        normalized_score: float,
    ) -> Dict[str, Any]:
        reasons: List[str] = []
        severity = "none"
        runtime_meta = metadata if isinstance(metadata, dict) else {}
        clean_score = max(0.0, min(1.0, float(normalized_score)))

        samples = int(row.get("samples", 0) or 0)
        success_rate = max(0.0, min(1.0, float(row.get("success_rate", 0.0) or 0.0)))
        consecutive_failures = int(row.get("consecutive_failures", 0) or 0)
        last_status = str(row.get("last_status", "")).strip().lower()
        state_profile_size = int(row.get("state_profile_size", 0) or 0)
        state_profile_raw = row.get("state_profile", {})
        state_profile = state_profile_raw if isinstance(state_profile_raw, dict) else {}
        row_pre_hash = str(row.get("last_pre_state_hash", "")).strip().lower()
        row_post_hash = str(row.get("last_post_state_hash", "")).strip().lower()
        pre_hash = str(runtime_meta.get("__desktop_pre_state_hash", runtime_meta.get("desktop_pre_state_hash", ""))).strip().lower()
        retry_anchor_hash = str(runtime_meta.get("__desktop_retry_anchor_state_hash", "")).strip().lower()
        runtime_window = str(runtime_meta.get("__desktop_pre_window_title", "")).strip().lower()
        runtime_app = str(runtime_meta.get("__desktop_pre_app", "")).strip().lower()
        row_window = str(row.get("window_title", "")).strip().lower()
        row_app = str(row.get("app", "")).strip().lower()
        row_query = str(row.get("query", "")).strip().lower()
        row_element_id = str(row.get("element_id", "")).strip().lower()
        row_control_type = str(row.get("control_type", "")).strip().lower()
        step_args = step.args if isinstance(step.args, dict) else {}
        requested_element_id = str(step_args.get("element_id", "")).strip().lower()
        requested_control_type = str(step_args.get("control_type", "")).strip().lower()
        requested_query = ""
        for query_key in ("query", "text", "target"):
            value = step_args.get(query_key)
            if isinstance(value, str) and value.strip():
                requested_query = value.strip().lower()
                break
        requested_mode = str(step.args.get("target_mode", "")).strip().lower() if isinstance(step.args, dict) else ""
        row_mode = str(row.get("target_mode", "")).strip().lower()
        feedback_rows = runtime_meta.get("__desktop_guardrail_feedback")
        feedback_list = feedback_rows if isinstance(feedback_rows, list) else []
        clean_action = str(step.action or "").strip().lower()
        invalidation_feedback_rows = runtime_meta.get("__desktop_anchor_invalidation_feedback")
        invalidation_feedback_list = invalidation_feedback_rows if isinstance(invalidation_feedback_rows, list) else []
        historical_reason_counter: Dict[str, int] = {}
        historical_transition_signatures: set[str] = set()
        historical_hard_count = 0
        historical_row_count = 0
        for history_row in invalidation_feedback_list[-18:]:
            if not isinstance(history_row, dict):
                continue
            history_action = str(history_row.get("action", "")).strip().lower()
            if history_action and clean_action and history_action != clean_action:
                continue
            historical_row_count += 1
            history_severity = str(history_row.get("severity", "")).strip().lower()
            if history_severity == "hard":
                historical_hard_count += 1
            history_signature = str(history_row.get("transition_signature", "")).strip().lower()
            if history_signature:
                historical_transition_signatures.add(history_signature)
            history_reasons_raw = history_row.get("reasons", [])
            history_reasons = [
                str(item).strip().lower()
                for item in history_reasons_raw
                if str(item).strip()
            ] if isinstance(history_reasons_raw, list) else []
            for history_reason in history_reasons[:16]:
                historical_reason_counter[history_reason] = int(historical_reason_counter.get(history_reason, 0) or 0) + 1
        historical_reason_cluster_count = max(historical_reason_counter.values()) if historical_reason_counter else 0
        historical_transition_signature_count = len(historical_transition_signatures)
        hard_guardrail_signals = 0
        soft_guardrail_signals = 0
        guardrail_hard_severity_count = 0
        guardrail_soft_severity_count = 0
        guardrail_fallback_failed_hits = 0
        guardrail_precondition_failed_hits = 0
        guardrail_no_state_change_hits = 0
        guardrail_recent_failure_hits = 0
        profile_pre_samples = 0
        profile_pre_success_rate = 1.0
        guardrail_repeat_count = 0
        runtime_post_hash = str(
            runtime_meta.get("__desktop_post_state_hash", runtime_meta.get("desktop_post_state_hash", ""))
        ).strip().lower()
        row_last_error = str(row.get("last_error", "")).strip().lower()
        guardrail_transition_signatures: set[str] = set()
        guardrail_changed_path_counter: Dict[str, int] = {}
        guardrail_post_hashes_for_pre: set[str] = set()
        guardrail_visual_churn = 0
        transition_profile_raw = row.get("transition_profile", {})
        transition_profile = transition_profile_raw if isinstance(transition_profile_raw, dict) else {}
        transition_profile_size = len(transition_profile)
        runtime_transition_signature = str(
            runtime_meta.get("__desktop_transition_signature", runtime_meta.get("desktop_transition_signature", ""))
        ).strip().lower()
        if not runtime_transition_signature and pre_hash and runtime_post_hash:
            runtime_transition_signature = f"{pre_hash[:24]}->{runtime_post_hash[:24]}"
        matched_transition_key = ""
        matched_transition_profile: Dict[str, Any] = {}
        transition_profile_samples = 0
        transition_profile_success_rate = 1.0
        transition_profile_guardrail_churn = 0.0
        transition_profile_weighted_success = 1.0
        transition_profile_weighted_churn = 0.0
        row_updated_at = str(row.get("updated_at", "")).strip()
        row_age_s = 0.0
        now_utc_ts = datetime.now(timezone.utc).timestamp()
        if row_updated_at:
            try:
                parsed_updated = datetime.fromisoformat(row_updated_at.replace("Z", "+00:00"))
                if parsed_updated.tzinfo is None:
                    parsed_updated = parsed_updated.replace(tzinfo=timezone.utc)
                row_age_s = max(
                    0.0,
                    now_utc_ts - parsed_updated.astimezone(timezone.utc).timestamp(),
                )
            except Exception:
                row_age_s = 0.0
        freshness_bucket = "fresh"
        if row_age_s >= 86_400.0:
            freshness_bucket = "stale"
        elif row_age_s >= 18_000.0:
            freshness_bucket = "aging"
        elif row_age_s >= 5_400.0:
            freshness_bucket = "warm"

        if samples >= 4 and success_rate < 0.34:
            reasons.append("low_success_rate")
        if consecutive_failures >= 3:
            reasons.append("recent_consecutive_failures")
        if last_status in {"failed", "blocked"} and clean_score < 0.86:
            reasons.append("last_status_unstable")
        if pre_hash and row_pre_hash and pre_hash != row_pre_hash and state_profile_size <= 1 and clean_score < 0.92:
            reasons.append("pre_state_mismatch")
        if retry_anchor_hash and row_post_hash and retry_anchor_hash != row_post_hash:
            reasons.append("retry_state_anchor_mismatch")
        if runtime_post_hash and row_post_hash and runtime_post_hash != row_post_hash and clean_score < 0.965:
            reasons.append("post_state_anchor_mismatch")
        if runtime_window and row_window and runtime_window != row_window and clean_score < 0.94:
            reasons.append("window_context_mismatch")
        if runtime_app and row_app and runtime_app != row_app and clean_score < 0.94:
            reasons.append("app_context_mismatch")
        if row_last_error and any(token in row_last_error for token in ("window", "context", "not visible", "transition")) and clean_score < 0.965:
            reasons.append("historical_context_shift_error")
        if requested_mode and requested_mode not in {"", "auto"} and row_mode and requested_mode != row_mode and clean_score < 0.9:
            reasons.append("target_mode_mismatch")
        if requested_element_id and row_element_id and requested_element_id != row_element_id and clean_score < 0.97:
            reasons.append("element_anchor_mismatch")
        if requested_control_type and row_control_type and requested_control_type != row_control_type and clean_score < 0.95:
            reasons.append("control_type_anchor_mismatch")
        if (
            requested_query
            and row_query
            and requested_query != row_query
            and requested_query not in row_query
            and row_query not in requested_query
            and clean_score < 0.93
        ):
            reasons.append("query_anchor_mismatch")
        if pre_hash and state_profile_size >= 3 and pre_hash not in state_profile and clean_score < 0.95:
            reasons.append("state_profile_unseen_pre_hash")
        if row_age_s >= 86_400.0 and clean_score < 0.99:
            reasons.append("stale_anchor_hard")
        elif row_age_s >= 5_400.0 and samples <= 4 and clean_score < 0.95:
            reasons.append("stale_anchor_soft")
        if pre_hash:
            profile_row_raw = state_profile.get(pre_hash, {})
            profile_row = profile_row_raw if isinstance(profile_row_raw, dict) else {}
            profile_pre_samples = int(profile_row.get("samples", 0) or 0)
            if profile_pre_samples > 0:
                profile_successes = int(profile_row.get("successes", 0) or 0)
                profile_pre_success_rate = max(0.0, min(1.0, float(profile_successes) / max(1.0, float(profile_pre_samples))))
                if profile_pre_samples >= 3 and profile_pre_success_rate < 0.42 and clean_score < 0.97:
                    reasons.append("state_profile_pre_hash_low_success")
        for feedback in feedback_list[-10:]:
            if not isinstance(feedback, dict):
                continue
            feedback_action = str(feedback.get("action", "")).strip().lower()
            if feedback_action and feedback_action != clean_action:
                continue
            feedback_severity = str(feedback.get("severity", "")).strip().lower()
            tags_raw = feedback.get("reason_tags", [])
            tags = [str(item).strip().lower() for item in tags_raw if str(item).strip()] if isinstance(tags_raw, list) else []
            if not tags:
                continue
            if feedback_severity == "hard":
                guardrail_hard_severity_count += 1
            elif feedback_severity == "soft":
                guardrail_soft_severity_count += 1
            signature = str(feedback.get("transition_signature", "")).strip().lower()
            if signature:
                guardrail_transition_signatures.add(signature)
            feedback_pre_hash = str(feedback.get("pre_hash", "")).strip().lower()
            feedback_post_hash = str(
                feedback.get("state_hash", feedback.get("post_hash", ""))
            ).strip().lower()
            if pre_hash and feedback_pre_hash and pre_hash == feedback_pre_hash and feedback_post_hash:
                guardrail_post_hashes_for_pre.add(feedback_post_hash)
            changed_paths_raw = feedback.get("changed_paths", [])
            changed_paths = [
                str(item).strip().lower()
                for item in changed_paths_raw
                if str(item).strip()
            ] if isinstance(changed_paths_raw, list) else []
            recorded_at = str(feedback.get("recorded_at", "")).strip()
            if recorded_at:
                try:
                    parsed_recorded = datetime.fromisoformat(recorded_at.replace("Z", "+00:00"))
                    if parsed_recorded.tzinfo is None:
                        parsed_recorded = parsed_recorded.replace(tzinfo=timezone.utc)
                    age_s = max(0.0, now_utc_ts - parsed_recorded.astimezone(timezone.utc).timestamp())
                    if age_s <= 180.0:
                        guardrail_recent_failure_hits += 1
                except Exception:
                    pass
            for changed_path in changed_paths[:24]:
                guardrail_changed_path_counter[changed_path] = int(guardrail_changed_path_counter.get(changed_path, 0)) + 1
            if any(path.startswith("visual.") or "screen_hash" in path for path in changed_paths):
                guardrail_visual_churn += 1
            if pre_hash and feedback_pre_hash and pre_hash != feedback_pre_hash and clean_score < 0.96:
                reasons.append("guardrail_pre_state_diverged")
                hard_guardrail_signals += 1
            if any(tag in {"window_transition", "app_transition", "anchor_precondition_failed"} for tag in tags):
                reasons.append("guardrail_context_shift")
                hard_guardrail_signals += 1
            if "anchor_fallback_failed" in tags:
                guardrail_fallback_failed_hits += 1
                if clean_score < 0.99:
                    reasons.append("guardrail_anchor_fallback_failed")
                    hard_guardrail_signals += 1
            if "anchor_precondition_failed" in tags:
                guardrail_precondition_failed_hits += 1
            if any(tag in {"confirm_policy_failed", "confirm_check_failed"} for tag in tags) and clean_score < 0.97:
                reasons.append("guardrail_confirm_instability")
                soft_guardrail_signals += 1
            if "no_state_change" in tags and clean_score < 0.98:
                reasons.append("guardrail_no_state_change")
                soft_guardrail_signals += 1
                guardrail_no_state_change_hits += 1
            guardrail_repeat_count += 1

        transition_profile_rows: List[Dict[str, Any]] = []
        for transition_key, transition_row_raw in transition_profile.items():
            if not isinstance(transition_row_raw, dict):
                continue
            transition_key_clean = str(transition_key).strip().lower()
            transition_signature = str(transition_row_raw.get("signature", "")).strip().lower() or transition_key_clean
            transition_samples_raw = Executor._coerce_int(
                transition_row_raw.get("samples", 0),
                minimum=0,
                maximum=10_000_000,
                default=0,
            )
            if transition_samples_raw <= 0:
                continue
            transition_success_rate_raw = Executor._coerce_float(
                transition_row_raw.get("success_rate", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            transition_churn_raw = Executor._coerce_float(
                transition_row_raw.get("guardrail_churn_ema", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            payload = {
                "key": transition_key_clean,
                "signature": transition_signature,
                "samples": int(transition_samples_raw),
                "success_rate": float(transition_success_rate_raw),
                "guardrail_churn_ema": float(transition_churn_raw),
            }
            transition_profile_rows.append(payload)
            if runtime_transition_signature and (
                transition_signature == runtime_transition_signature or transition_key_clean == runtime_transition_signature
            ):
                if transition_samples_raw >= transition_profile_samples:
                    matched_transition_profile = payload
                    matched_transition_key = transition_key_clean or transition_signature
                    transition_profile_samples = int(transition_samples_raw)
                    transition_profile_success_rate = float(transition_success_rate_raw)
                    transition_profile_guardrail_churn = float(transition_churn_raw)

        if transition_profile_rows:
            total_transition_samples = sum(
                Executor._coerce_int(row.get("samples", 0), minimum=0, maximum=10_000_000, default=0)
                for row in transition_profile_rows
                if isinstance(row, dict)
            )
            if total_transition_samples > 0:
                transition_profile_weighted_success = Executor._coerce_float(
                    sum(
                        Executor._coerce_float(
                            row.get("success_rate", 0.0),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        )
                        * float(
                            Executor._coerce_int(
                                row.get("samples", 0),
                                minimum=0,
                                maximum=10_000_000,
                                default=0,
                            )
                        )
                        for row in transition_profile_rows
                        if isinstance(row, dict)
                    )
                    / float(total_transition_samples),
                    minimum=0.0,
                    maximum=1.0,
                    default=1.0,
                )
                transition_profile_weighted_churn = Executor._coerce_float(
                    sum(
                        Executor._coerce_float(
                            row.get("guardrail_churn_ema", 0.0),
                            minimum=0.0,
                            maximum=1.0,
                            default=0.0,
                        )
                        * float(
                            Executor._coerce_int(
                                row.get("samples", 0),
                                minimum=0,
                                maximum=10_000_000,
                                default=0,
                            )
                        )
                        for row in transition_profile_rows
                        if isinstance(row, dict)
                    )
                    / float(total_transition_samples),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
            if matched_transition_profile:
                if transition_profile_samples >= 3 and transition_profile_success_rate < 0.42 and clean_score < 0.985:
                    reasons.append("transition_profile_low_success")
                if transition_profile_samples >= 2 and transition_profile_guardrail_churn >= 0.52 and clean_score < 0.99:
                    reasons.append("transition_profile_high_churn")
            elif runtime_transition_signature and transition_profile_size >= 4 and clean_score < 0.97:
                reasons.append("transition_profile_unseen_signature")
            elif transition_profile_size >= 6 and transition_profile_weighted_success < 0.54 and clean_score < 0.98:
                reasons.append("transition_profile_overall_decay")
            if transition_profile_weighted_churn >= 0.56 and clean_score < 0.99:
                reasons.append("transition_profile_churn_cluster")

        if hard_guardrail_signals >= 1 and clean_score < 0.985:
            reasons.append("guardrail_hard_signal")
        if soft_guardrail_signals >= 2 and clean_score < 0.97:
            reasons.append("guardrail_soft_signal_cluster")
        if guardrail_repeat_count >= 3 and clean_score < 0.985:
            reasons.append("guardrail_repeat_failures")
        if guardrail_hard_severity_count >= 2 and clean_score < 0.99:
            reasons.append("guardrail_hard_severity_cluster")
        if guardrail_fallback_failed_hits >= 1 and guardrail_no_state_change_hits >= 1 and clean_score < 0.99:
            reasons.append("anchor_fallback_instability")
        if guardrail_precondition_failed_hits >= 2 and clean_score < 0.99:
            reasons.append("anchor_precondition_failure_cluster")
        transition_volatility = len(guardrail_transition_signatures)
        pre_anchor_collision_count = len(guardrail_post_hashes_for_pre)
        repeated_changed_paths = sum(1 for count in guardrail_changed_path_counter.values() if int(count) >= 2)
        context_churn_hits = sum(
            int(count)
            for path, count in guardrail_changed_path_counter.items()
            if str(path).startswith("window.") or str(path).startswith("app.")
        )
        if pre_anchor_collision_count >= 2 and clean_score < 0.99:
            reasons.append("state_anchor_collision")
        if transition_volatility >= 3 and clean_score < 0.99:
            reasons.append("guardrail_transition_volatility")
        if guardrail_visual_churn >= 2 and (repeated_changed_paths >= 2 or clean_score < 0.985):
            reasons.append("guardrail_visual_churn")
        if context_churn_hits >= 3 and clean_score < 0.99:
            reasons.append("guardrail_context_churn")
        if guardrail_recent_failure_hits >= 3 and transition_volatility >= 2 and clean_score < 0.995:
            reasons.append("guardrail_transition_burst")
        if historical_reason_cluster_count >= 3 and clean_score < 0.995:
            reasons.append("historical_invalidation_cluster")
        if historical_transition_signature_count >= 3 and clean_score < 0.995:
            reasons.append("historical_transition_signature_churn")
        if historical_hard_count >= 2 and clean_score < 0.996:
            reasons.append("historical_hard_invalidation_pressure")

        reasons = sorted({str(reason).strip().lower() for reason in reasons if str(reason).strip()})
        hard_reason_tokens = {
            "retry_state_anchor_mismatch",
            "pre_state_mismatch",
            "window_context_mismatch",
            "app_context_mismatch",
            "post_state_anchor_mismatch",
            "stale_anchor_hard",
            "guardrail_context_shift",
            "guardrail_hard_signal",
            "guardrail_pre_state_diverged",
            "state_profile_unseen_pre_hash",
            "guardrail_repeat_failures",
            "state_anchor_collision",
            "guardrail_transition_volatility",
            "guardrail_hard_severity_cluster",
            "anchor_fallback_instability",
            "anchor_precondition_failure_cluster",
            "guardrail_transition_burst",
            "guardrail_anchor_fallback_failed",
            "transition_profile_low_success",
            "transition_profile_high_churn",
            "transition_profile_churn_cluster",
            "historical_hard_invalidation_pressure",
            "historical_transition_signature_churn",
        }
        reason_weights = {
            "retry_state_anchor_mismatch": 0.34,
            "pre_state_mismatch": 0.24,
            "post_state_anchor_mismatch": 0.24,
            "window_context_mismatch": 0.18,
            "app_context_mismatch": 0.18,
            "target_mode_mismatch": 0.1,
            "element_anchor_mismatch": 0.2,
            "control_type_anchor_mismatch": 0.14,
            "query_anchor_mismatch": 0.1,
            "historical_context_shift_error": 0.14,
            "state_profile_unseen_pre_hash": 0.2,
            "state_profile_pre_hash_low_success": 0.16,
            "stale_anchor_hard": 0.22,
            "stale_anchor_soft": 0.08,
            "guardrail_context_shift": 0.26,
            "guardrail_pre_state_diverged": 0.24,
            "guardrail_hard_signal": 0.24,
            "guardrail_confirm_instability": 0.1,
            "guardrail_no_state_change": 0.08,
            "guardrail_soft_signal_cluster": 0.14,
            "guardrail_repeat_failures": 0.12,
            "state_anchor_collision": 0.24,
            "guardrail_transition_volatility": 0.2,
            "guardrail_hard_severity_cluster": 0.2,
            "anchor_fallback_instability": 0.24,
            "anchor_precondition_failure_cluster": 0.22,
            "guardrail_transition_burst": 0.18,
            "guardrail_anchor_fallback_failed": 0.2,
            "guardrail_visual_churn": 0.16,
            "guardrail_context_churn": 0.14,
            "transition_profile_unseen_signature": 0.12,
            "transition_profile_low_success": 0.2,
            "transition_profile_high_churn": 0.18,
            "transition_profile_overall_decay": 0.16,
            "transition_profile_churn_cluster": 0.18,
            "historical_invalidation_cluster": 0.14,
            "historical_transition_signature_churn": 0.18,
            "historical_hard_invalidation_pressure": 0.22,
            "low_success_rate": 0.08,
            "recent_consecutive_failures": 0.06,
            "last_status_unstable": 0.07,
        }
        drift_score = 0.0
        for reason in reasons:
            drift_score += reason_weights.get(reason, 0.05)
        drift_score += min(0.28, float(hard_guardrail_signals) * 0.11)
        drift_score += min(0.18, float(soft_guardrail_signals) * 0.05)
        if clean_score < 0.96:
            drift_score += min(0.32, (0.96 - clean_score) * 1.25)
        if profile_pre_samples >= 3 and profile_pre_success_rate < 0.5:
            drift_score += min(0.14, (0.5 - profile_pre_success_rate) * 0.4)
        if row_age_s >= 86_400.0:
            drift_score += 0.12
        elif row_age_s >= 18_000.0:
            drift_score += 0.05
        if guardrail_repeat_count >= 3:
            drift_score += min(0.16, float(guardrail_repeat_count - 2) * 0.04)
        if transition_volatility >= 3:
            drift_score += min(0.14, float(transition_volatility - 2) * 0.035)
        if guardrail_hard_severity_count >= 2:
            drift_score += min(0.16, float(guardrail_hard_severity_count - 1) * 0.05)
        if guardrail_fallback_failed_hits >= 1:
            drift_score += min(0.12, float(guardrail_fallback_failed_hits) * 0.06)
        if guardrail_recent_failure_hits >= 3:
            drift_score += min(0.1, float(guardrail_recent_failure_hits - 2) * 0.03)
        if pre_anchor_collision_count >= 2:
            drift_score += min(0.12, float(pre_anchor_collision_count - 1) * 0.04)
        if guardrail_visual_churn >= 2:
            drift_score += min(0.1, float(guardrail_visual_churn - 1) * 0.03)
        if context_churn_hits >= 3:
            drift_score += min(0.08, float(context_churn_hits - 2) * 0.02)
        if transition_profile_samples >= 3 and transition_profile_success_rate < 0.52:
            drift_score += min(0.2, (0.52 - transition_profile_success_rate) * 0.46)
        if transition_profile_guardrail_churn >= 0.42:
            drift_score += min(0.16, (transition_profile_guardrail_churn - 0.42) * 0.38)
        if transition_profile_size >= 5 and transition_profile_weighted_success < 0.62:
            drift_score += min(0.14, (0.62 - transition_profile_weighted_success) * 0.34)
        if transition_profile_size >= 4 and transition_profile_weighted_churn >= 0.48:
            drift_score += min(0.12, (transition_profile_weighted_churn - 0.48) * 0.28)
        if historical_reason_cluster_count >= 3:
            drift_score += min(0.14, float(historical_reason_cluster_count - 2) * 0.045)
        if historical_transition_signature_count >= 3:
            drift_score += min(0.12, float(historical_transition_signature_count - 2) * 0.035)
        if historical_hard_count >= 2:
            drift_score += min(0.16, float(historical_hard_count - 1) * 0.05)
        drift_score = max(0.0, min(1.0, drift_score))

        has_hard_reason = any(reason in hard_reason_tokens for reason in reasons)
        invalidated = bool(
            has_hard_reason
            or drift_score >= 0.42
            or (len(reasons) >= 3 and clean_score < 0.975)
        )
        if invalidated:
            if has_hard_reason or drift_score >= 0.72:
                severity = "hard"
            else:
                severity = "soft"
        return {
            "invalidated": invalidated,
            "severity": severity,
            "reasons": reasons,
            "score": round(clean_score, 6),
            "drift_score": round(drift_score, 6),
            "samples": int(samples),
            "success_rate": round(success_rate, 6),
            "consecutive_failures": int(consecutive_failures),
            "state_profile_size": int(state_profile_size),
            "pre_state_profile_samples": int(profile_pre_samples),
            "pre_state_profile_success_rate": round(profile_pre_success_rate, 6),
            "guardrail_feedback_count": len(feedback_list),
            "guardrail_hard_signals": int(hard_guardrail_signals),
            "guardrail_soft_signals": int(soft_guardrail_signals),
            "guardrail_hard_severity_count": int(guardrail_hard_severity_count),
            "guardrail_soft_severity_count": int(guardrail_soft_severity_count),
            "guardrail_fallback_failed_hits": int(guardrail_fallback_failed_hits),
            "guardrail_precondition_failed_hits": int(guardrail_precondition_failed_hits),
            "guardrail_no_state_change_hits": int(guardrail_no_state_change_hits),
            "guardrail_recent_failure_hits": int(guardrail_recent_failure_hits),
            "guardrail_repeat_count": int(guardrail_repeat_count),
            "guardrail_transition_volatility": int(transition_volatility),
            "guardrail_pre_anchor_collision_count": int(pre_anchor_collision_count),
            "guardrail_visual_churn": int(guardrail_visual_churn),
            "guardrail_context_churn_hits": int(context_churn_hits),
            "guardrail_repeated_changed_paths": int(repeated_changed_paths),
            "historical_invalidation_feedback_count": int(historical_row_count),
            "historical_reason_cluster_count": int(historical_reason_cluster_count),
            "historical_transition_signature_count": int(historical_transition_signature_count),
            "historical_hard_invalidation_count": int(historical_hard_count),
            "row_age_s": round(row_age_s, 3),
            "freshness_bucket": freshness_bucket,
            "runtime_transition_signature": runtime_transition_signature,
            "transition_profile_size": int(transition_profile_size),
            "transition_profile_match_key": matched_transition_key,
            "transition_profile_samples": int(transition_profile_samples),
            "transition_profile_success_rate": round(
                Executor._coerce_float(
                    transition_profile_success_rate,
                    minimum=0.0,
                    maximum=1.0,
                    default=1.0,
                ),
                6,
            ),
            "transition_profile_guardrail_churn_ema": round(
                Executor._coerce_float(
                    transition_profile_guardrail_churn,
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                6,
            ),
            "transition_profile_weighted_success_rate": round(
                Executor._coerce_float(
                    transition_profile_weighted_success,
                    minimum=0.0,
                    maximum=1.0,
                    default=1.0,
                ),
                6,
            ),
            "transition_profile_weighted_guardrail_churn": round(
                Executor._coerce_float(
                    transition_profile_weighted_churn,
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                6,
            ),
        }

    def _quarantine_desktop_anchor_memory(
        self,
        *,
        step: PlanStep,
        metadata: Dict[str, Any],
        invalidation: Dict[str, Any],
    ) -> Dict[str, Any]:
        if self.desktop_anchor_memory is None:
            return {}
        if not isinstance(invalidation, dict):
            return {}
        severity = str(invalidation.get("severity", "")).strip().lower()
        hard_signals = self._coerce_int(invalidation.get("guardrail_hard_signals", 0), minimum=0, maximum=1000, default=0)
        soft_signals = self._coerce_int(invalidation.get("guardrail_soft_signals", 0), minimum=0, maximum=1000, default=0)
        reasons = invalidation.get("reasons", [])
        reason_list = [str(item).strip().lower() for item in reasons if str(item).strip()] if isinstance(reasons, list) else []
        has_stale_hard = "stale_anchor_hard" in reason_list
        repeat_failures = "guardrail_repeat_failures" in reason_list
        post_state_mismatch = "post_state_anchor_mismatch" in reason_list
        transition_burst = "guardrail_transition_burst" in reason_list
        hard_cluster = "guardrail_hard_severity_cluster" in reason_list
        fallback_instability = "anchor_fallback_instability" in reason_list
        precondition_cluster = "anchor_precondition_failure_cluster" in reason_list
        historical_cluster = "historical_invalidation_cluster" in reason_list
        historical_transition_churn = "historical_transition_signature_churn" in reason_list
        historical_hard_pressure = "historical_hard_invalidation_pressure" in reason_list
        drift_score = self._coerce_float(
            invalidation.get("drift_score", 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        should_quarantine = bool(
            severity == "hard"
            or hard_signals >= 1
            or soft_signals >= 2
            or has_stale_hard
            or post_state_mismatch
            or repeat_failures
            or transition_burst
            or hard_cluster
            or fallback_instability
            or precondition_cluster
            or historical_cluster
            or historical_transition_churn
            or historical_hard_pressure
            or drift_score >= 0.58
        )
        if not should_quarantine:
            return {}

        reason_text = ", ".join(reason_list[:8]) or "anchor invalidated by runtime guardrails"
        base_ttl = 900
        if severity == "hard" or hard_signals >= 1:
            base_ttl = 2400
        if soft_signals >= 3:
            base_ttl = max(base_ttl, 1800)
        if drift_score > 0.0:
            base_ttl = int(round(base_ttl * (1.0 + (drift_score * 0.9))))
        if "state_profile_unseen_pre_hash" in reason_list:
            base_ttl = max(base_ttl, 2100)
        if "element_anchor_mismatch" in reason_list:
            base_ttl = max(base_ttl, 1500)
        if has_stale_hard:
            base_ttl = max(base_ttl, 4200)
        if post_state_mismatch:
            base_ttl = max(base_ttl, 3300)
        if repeat_failures:
            base_ttl = max(base_ttl, 3000)
        if "state_anchor_collision" in reason_list:
            base_ttl = max(base_ttl, 3600)
        if "guardrail_transition_volatility" in reason_list:
            base_ttl = max(base_ttl, 3900)
        if transition_burst:
            base_ttl = max(base_ttl, 3300)
        if hard_cluster:
            base_ttl = max(base_ttl, 3600)
        if fallback_instability:
            base_ttl = max(base_ttl, 3300)
        if precondition_cluster:
            base_ttl = max(base_ttl, 3600)
        if historical_cluster:
            base_ttl = max(base_ttl, 3000)
        if historical_transition_churn:
            base_ttl = max(base_ttl, 3600)
        if historical_hard_pressure:
            base_ttl = max(base_ttl, 4200)
        if "guardrail_visual_churn" in reason_list:
            base_ttl = max(base_ttl, 2400)
        if "guardrail_context_churn" in reason_list:
            base_ttl = max(base_ttl, 2700)
        base_ttl = self._coerce_int(base_ttl, minimum=300, maximum=10_800, default=900)

        args_payload = step.args if isinstance(step.args, dict) else {}
        payload = self.desktop_anchor_memory.quarantine(
            action=step.action,
            args=args_payload,
            metadata=metadata if isinstance(metadata, dict) else {},
            reason=reason_text,
            severity="hard"
            if (
                severity == "hard"
                or hard_signals >= 1
                or has_stale_hard
                or post_state_mismatch
                or hard_cluster
                or transition_burst
                or precondition_cluster
                or historical_hard_pressure
            )
            else "soft",
            signals=reason_list,
            ttl_s=base_ttl,
        )
        if isinstance(payload, dict) and str(payload.get("status", "")).strip().lower() == "success":
            item = payload.get("item", {})
            if isinstance(item, dict):
                self.telemetry.emit(
                    "desktop_anchor.quarantine",
                    {
                        "action": step.action,
                        "step_id": step.step_id,
                        "severity": str(item.get("severity", "")),
                        "key": str(item.get("key", "")),
                        "signals": item.get("signals", []),
                        "drift_score": round(drift_score, 6),
                        "ttl_s": int(base_ttl),
                    },
                )
            return payload
        return {}

    def _record_desktop_anchor_outcome(self, *, step: PlanStep, metadata: Dict[str, Any], result: ActionResult) -> None:
        if self.desktop_anchor_memory is None:
            return
        if step.action not in {"computer_click_target", "computer_click_text", "accessibility_invoke_element"}:
            return
        try:
            self.desktop_anchor_memory.record_outcome(
                action=step.action,
                args=step.args if isinstance(step.args, dict) else {},
                status=str(result.status or "").strip().lower(),
                output=result.output if isinstance(result.output, dict) else {},
                evidence=result.evidence if isinstance(result.evidence, dict) else {},
                metadata=metadata if isinstance(metadata, dict) else {},
                error=result.error or "",
            )
        except Exception as exc:  # noqa: BLE001
            self.telemetry.emit(
                "desktop_anchor.record_error",
                {
                    "action": step.action,
                    "step_id": step.step_id,
                    "message": str(exc),
                },
            )

    def _record_desktop_anchor_invalidation_feedback(
        self,
        *,
        step: PlanStep,
        metadata: Dict[str, Any],
        invalidation: Dict[str, Any],
    ) -> None:
        runtime_meta = metadata if isinstance(metadata, dict) else {}
        payload = invalidation if isinstance(invalidation, dict) else {}
        if not runtime_meta or not payload:
            return
        if not bool(payload.get("invalidated", False)):
            return
        reasons_raw = payload.get("reasons", [])
        reasons = [
            str(item).strip().lower()
            for item in reasons_raw
            if str(item).strip()
        ] if isinstance(reasons_raw, list) else []
        if not reasons:
            reasons = ["anchor_invalidation"]
        row = {
            "action": str(step.action or "").strip().lower(),
            "step_id": str(step.step_id or "").strip(),
            "severity": str(payload.get("severity", "")).strip().lower() or "soft",
            "drift_score": round(
                self._coerce_float(
                    payload.get("drift_score", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                ),
                6,
            ),
            "transition_signature": str(payload.get("runtime_transition_signature", "")).strip().lower(),
            "freshness_bucket": str(payload.get("freshness_bucket", "")).strip().lower(),
            "reasons": reasons[:16],
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }
        existing_rows = runtime_meta.get("__desktop_anchor_invalidation_feedback")
        rows = [dict(item) for item in existing_rows if isinstance(item, dict)] if isinstance(existing_rows, list) else []
        rows.append(row)
        runtime_meta["__desktop_anchor_invalidation_feedback"] = rows[-18:]
        self.telemetry.emit(
            "desktop_anchor.invalidation_feedback",
            {
                "action": row["action"],
                "step_id": row["step_id"],
                "severity": row["severity"],
                "drift_score": row["drift_score"],
                "reason_count": len(reasons),
                "freshness_bucket": row["freshness_bucket"],
            },
        )

    @staticmethod
    def _desktop_guardrail_tags_for_action(
        *,
        action: str,
        metadata: Dict[str, Any],
        limit: int = 10,
    ) -> List[str]:
        runtime_meta = metadata if isinstance(metadata, dict) else {}
        feedback_rows = runtime_meta.get("__desktop_guardrail_feedback")
        feedback_list = feedback_rows if isinstance(feedback_rows, list) else []
        clean_action = str(action or "").strip().lower()
        tags: List[str] = []
        for row in feedback_list[-max(1, int(limit)) :]:
            if not isinstance(row, dict):
                continue
            row_action = str(row.get("action", "")).strip().lower()
            if row_action and clean_action and row_action != clean_action:
                continue
            tags_raw = row.get("reason_tags", [])
            if not isinstance(tags_raw, list):
                continue
            for item in tags_raw:
                tag = str(item or "").strip().lower()
                if tag:
                    tags.append(tag)
        deduped: List[str] = []
        seen: set[str] = set()
        for tag in tags:
            if tag in seen:
                continue
            seen.add(tag)
            deduped.append(tag)
        return deduped[: max(1, int(limit))]

    @staticmethod
    def _desktop_anchor_invalidation_tags_for_action(
        *,
        action: str,
        metadata: Dict[str, Any],
        limit: int = 14,
    ) -> List[str]:
        runtime_meta = metadata if isinstance(metadata, dict) else {}
        rows_raw = runtime_meta.get("__desktop_anchor_invalidation_feedback")
        rows = rows_raw if isinstance(rows_raw, list) else []
        clean_action = str(action or "").strip().lower()
        tags: List[str] = []
        for row in rows[-max(1, int(limit)) :]:
            if not isinstance(row, dict):
                continue
            row_action = str(row.get("action", "")).strip().lower()
            if row_action and clean_action and row_action != clean_action:
                continue
            reasons_raw = row.get("reasons", [])
            if not isinstance(reasons_raw, list):
                continue
            for reason in reasons_raw:
                tag = str(reason or "").strip().lower()
                if tag:
                    tags.append(tag)
        deduped: List[str] = []
        seen: set[str] = set()
        for tag in tags:
            if tag in seen:
                continue
            seen.add(tag)
            deduped.append(tag)
        return deduped[: max(1, int(limit))]

    def _desktop_transition_contract_for_action(
        self,
        *,
        action: str,
        metadata: Dict[str, Any],
        limit: int = 16,
    ) -> Dict[str, Any]:
        runtime_meta = metadata if isinstance(metadata, dict) else {}
        clean_action = str(action or "").strip().lower()
        feedback_rows_raw = runtime_meta.get("__desktop_guardrail_feedback")
        feedback_rows = feedback_rows_raw if isinstance(feedback_rows_raw, list) else []
        action_rows: List[Dict[str, Any]] = []
        for row in feedback_rows[-max(1, int(limit)) :]:
            if not isinstance(row, dict):
                continue
            row_action = str(row.get("action", "")).strip().lower()
            if row_action and clean_action and row_action != clean_action:
                continue
            action_rows.append(row)

        invalidation_tags = self._desktop_anchor_invalidation_tags_for_action(
            action=clean_action,
            metadata=runtime_meta,
            limit=max(8, min(24, int(limit))),
        )
        invalidation_tag_set = set(invalidation_tags)

        context_shift_hits = 0
        fallback_failed_hits = 0
        precondition_failed_hits = 0
        no_state_change_hits = 0
        confirm_failed_hits = 0
        transition_signatures: set[str] = set()
        context_changed_path_hits = 0
        visual_changed_path_hits = 0
        for row in action_rows:
            tags_raw = row.get("reason_tags", [])
            tags = [
                str(item).strip().lower()
                for item in tags_raw
                if str(item).strip()
            ] if isinstance(tags_raw, list) else []
            if "window_transition" in tags or "app_transition" in tags:
                context_shift_hits += 1
            if "anchor_fallback_failed" in tags:
                fallback_failed_hits += 1
            if "anchor_precondition_failed" in tags:
                precondition_failed_hits += 1
            if "no_state_change" in tags:
                no_state_change_hits += 1
            if "confirm_policy_failed" in tags or "confirm_check_failed" in tags:
                confirm_failed_hits += 1
            signature = str(row.get("transition_signature", "")).strip().lower()
            if signature:
                transition_signatures.add(signature)
            changed_paths_raw = row.get("changed_paths", [])
            changed_paths = [
                str(item).strip().lower()
                for item in changed_paths_raw
                if str(item).strip()
            ] if isinstance(changed_paths_raw, list) else []
            if any(path.startswith("window.") or path.startswith("app.") for path in changed_paths):
                context_changed_path_hits += 1
            if any(path.startswith("visual.") or "screen_hash" in path for path in changed_paths):
                visual_changed_path_hits += 1

        sample_count = max(1, len(action_rows))
        context_shift_ratio = float(context_shift_hits) / float(sample_count)
        fallback_failed_ratio = float(fallback_failed_hits) / float(sample_count)
        no_state_change_ratio = float(no_state_change_hits) / float(sample_count)
        transition_volatility = min(1.0, float(len(transition_signatures)) / max(1.0, float(sample_count)))
        invalidation_context = bool(
            "guardrail_context_shift" in invalidation_tag_set
            or "guardrail_context_churn" in invalidation_tag_set
            or "window_context_mismatch" in invalidation_tag_set
            or "app_context_mismatch" in invalidation_tag_set
        )
        invalidation_transition = bool(
            "guardrail_transition_volatility" in invalidation_tag_set
            or "guardrail_transition_burst" in invalidation_tag_set
            or "transition_profile_low_success" in invalidation_tag_set
            or "transition_profile_high_churn" in invalidation_tag_set
            or "transition_profile_churn_cluster" in invalidation_tag_set
        )
        invalidation_state_anchor = bool(
            "pre_state_mismatch" in invalidation_tag_set
            or "retry_state_anchor_mismatch" in invalidation_tag_set
            or "post_state_anchor_mismatch" in invalidation_tag_set
            or "state_anchor_collision" in invalidation_tag_set
        )

        volatility_score = self._coerce_float(
            (context_shift_ratio * 0.24)
            + (fallback_failed_ratio * 0.22)
            + (transition_volatility * 0.22)
            + ((float(context_changed_path_hits) / float(sample_count)) * 0.16)
            + ((float(visual_changed_path_hits) / float(sample_count)) * 0.08)
            + ((float(confirm_failed_hits) / float(sample_count)) * 0.08)
            + (0.12 if invalidation_context else 0.0)
            + (0.12 if invalidation_transition else 0.0)
            + (0.14 if invalidation_state_anchor else 0.0),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )

        preferred_probe = "hybrid"
        if (
            context_shift_ratio >= 0.32
            or invalidation_context
            or invalidation_transition
            or (visual_changed_path_hits >= 2 and transition_volatility >= 0.3)
        ):
            preferred_probe = "ocr"
        elif no_state_change_ratio >= 0.34 and context_shift_ratio <= 0.24 and not invalidation_context:
            preferred_probe = "accessibility"

        require_dual_probe = bool(
            volatility_score >= 0.52
            or invalidation_state_anchor
            or invalidation_transition
            or (fallback_failed_hits + precondition_failed_hits) >= 2
        )
        force_probe = bool(
            require_dual_probe
            or fallback_failed_hits >= 1
            or precondition_failed_hits >= 1
        )
        prefer_context_free = bool(
            context_shift_ratio >= 0.36
            or invalidation_context
            or context_changed_path_hits >= 2
        )
        fallback_bias = "balanced"
        if preferred_probe == "ocr" or invalidation_transition or invalidation_context:
            fallback_bias = "ocr_first"
        elif preferred_probe == "accessibility" and not invalidation_transition and not invalidation_state_anchor:
            fallback_bias = "accessibility_first"
        if require_dual_probe and (transition_volatility >= 0.34 or invalidation_transition):
            fallback_bias = "hybrid_reconfirm"

        ocr_first = bool(
            fallback_bias in {"ocr_first", "hybrid_reconfirm"}
            and not (preferred_probe == "accessibility" and no_state_change_ratio >= 0.5 and not invalidation_transition)
        )
        observation_refresh_budget = 0
        if volatility_score >= 0.7 or invalidation_transition:
            observation_refresh_budget = 2
        elif volatility_score >= 0.45 or invalidation_context or context_shift_ratio >= 0.26:
            observation_refresh_budget = 1
        recovery_patch_threshold_boost = 0.0
        if require_dual_probe:
            recovery_patch_threshold_boost += 0.03
        if invalidation_state_anchor:
            recovery_patch_threshold_boost += 0.03
        if invalidation_transition:
            recovery_patch_threshold_boost += 0.03
        recovery_patch_threshold_boost = self._coerce_float(
            recovery_patch_threshold_boost,
            minimum=0.0,
            maximum=0.16,
            default=0.0,
        )
        fallback_chain_template = ["observe"]
        if ocr_first:
            fallback_chain_template.extend(["computer_find_text_targets", "accessibility_find_element"])
        else:
            fallback_chain_template.extend(["accessibility_find_element", "computer_find_text_targets"])
        if require_dual_probe:
            fallback_chain_template.append("observe_reconfirm")
        fallback_chain_template.extend(["rank_candidates", "derive_retry_patch"])
        return {
            "sample_count": int(sample_count),
            "context_shift_ratio": round(context_shift_ratio, 6),
            "fallback_failed_ratio": round(fallback_failed_ratio, 6),
            "no_state_change_ratio": round(no_state_change_ratio, 6),
            "transition_volatility": round(transition_volatility, 6),
            "volatility_score": round(volatility_score, 6),
            "preferred_probe": preferred_probe,
            "require_dual_probe": bool(require_dual_probe),
            "force_probe": bool(force_probe),
            "prefer_context_free": bool(prefer_context_free),
            "fallback_bias": fallback_bias,
            "ocr_first": bool(ocr_first),
            "observation_refresh_budget": int(observation_refresh_budget),
            "recovery_patch_threshold_boost": round(recovery_patch_threshold_boost, 6),
            "fallback_chain_template": fallback_chain_template[:10],
            "invalidation_context": bool(invalidation_context),
            "invalidation_transition": bool(invalidation_transition),
            "invalidation_state_anchor": bool(invalidation_state_anchor),
            "invalidation_tags": invalidation_tags[:16],
        }

    async def _run_desktop_diagnostic_fallback(
        self,
        *,
        step: PlanStep,
        result: ActionResult,
        source: str,
        metadata: Dict[str, Any],
        attempt: int,
    ) -> ActionResult:
        query = ""
        if isinstance(step.args, dict):
            for key in ("query", "text", "target"):
                value = step.args.get(key)
                if isinstance(value, str) and value.strip():
                    query = value.strip()
                    break
        if not query:
            return result

        chain_rows: List[Dict[str, Any]] = []
        guardrail_tags = self._desktop_guardrail_tags_for_action(
            action=step.action,
            metadata=metadata,
            limit=10,
        )
        guardrail_tag_set = set(guardrail_tags)
        invalidation_tags = self._desktop_anchor_invalidation_tags_for_action(
            action=step.action,
            metadata=metadata,
            limit=14,
        )
        invalidation_tag_set = set(invalidation_tags)
        invalidation_context_shift = bool(
            "window_context_mismatch" in invalidation_tag_set
            or "app_context_mismatch" in invalidation_tag_set
            or "historical_context_shift_error" in invalidation_tag_set
            or "guardrail_context_shift" in invalidation_tag_set
            or "guardrail_context_churn" in invalidation_tag_set
        )
        invalidation_state_anchor_mismatch = bool(
            "pre_state_mismatch" in invalidation_tag_set
            or "retry_state_anchor_mismatch" in invalidation_tag_set
            or "post_state_anchor_mismatch" in invalidation_tag_set
            or "state_anchor_collision" in invalidation_tag_set
            or "transition_profile_unseen_signature" in invalidation_tag_set
        )
        invalidation_transition_unstable = bool(
            "guardrail_transition_volatility" in invalidation_tag_set
            or "guardrail_transition_burst" in invalidation_tag_set
            or "transition_profile_low_success" in invalidation_tag_set
            or "transition_profile_high_churn" in invalidation_tag_set
            or "transition_profile_churn_cluster" in invalidation_tag_set
            or "transition_profile_overall_decay" in invalidation_tag_set
        )
        invalidation_anchor_identity_mismatch = bool(
            "element_anchor_mismatch" in invalidation_tag_set
            or "control_type_anchor_mismatch" in invalidation_tag_set
            or "query_anchor_mismatch" in invalidation_tag_set
            or "target_mode_mismatch" in invalidation_tag_set
        )
        transition_contract = self._desktop_transition_contract_for_action(
            action=step.action,
            metadata=metadata,
            limit=16,
        )
        transition_force_probe = bool(transition_contract.get("force_probe", False)) if isinstance(transition_contract, dict) else False
        transition_preferred_probe = (
            str(transition_contract.get("preferred_probe", "hybrid")).strip().lower()
            if isinstance(transition_contract, dict)
            else "hybrid"
        )
        transition_require_dual_probe = bool(
            transition_contract.get("require_dual_probe", False)
        ) if isinstance(transition_contract, dict) else False
        transition_volatility_score = self._coerce_float(
            transition_contract.get("volatility_score", 0.0) if isinstance(transition_contract, dict) else 0.0,
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        transition_fallback_bias = (
            str(transition_contract.get("fallback_bias", "balanced")).strip().lower()
            if isinstance(transition_contract, dict)
            else "balanced"
        )
        transition_ocr_first = bool(
            transition_contract.get("ocr_first", False)
        ) if isinstance(transition_contract, dict) else False
        transition_refresh_budget = self._coerce_int(
            transition_contract.get("observation_refresh_budget", 0) if isinstance(transition_contract, dict) else 0,
            minimum=0,
            maximum=3,
            default=0,
        )
        transition_patch_threshold_boost = self._coerce_float(
            transition_contract.get("recovery_patch_threshold_boost", 0.0) if isinstance(transition_contract, dict) else 0.0,
            minimum=0.0,
            maximum=0.2,
            default=0.0,
        )
        context_shift = bool(
            "window_transition" in guardrail_tag_set
            or "app_transition" in guardrail_tag_set
            or "guardrail_context_shift" in guardrail_tag_set
            or invalidation_context_shift
        )
        anchor_precondition = bool(
            "anchor_precondition_failed" in guardrail_tag_set
            or "anchor_fallback_failed" in guardrail_tag_set
            or "anchor_precondition_failure_cluster" in invalidation_tag_set
            or "anchor_fallback_instability" in invalidation_tag_set
        )
        confirm_instability = bool(
            "confirm_policy_failed" in guardrail_tag_set
            or "confirm_check_failed" in guardrail_tag_set
        )
        no_state_change = bool(
            "no_state_change" in guardrail_tag_set
            or "guardrail_no_state_change" in invalidation_tag_set
        )

        current_window_title = ""
        existing_evidence = result.evidence if isinstance(result.evidence, dict) else {}
        existing_desktop = existing_evidence.get("desktop_state")
        if isinstance(existing_desktop, dict):
            current_window_title = str(existing_desktop.get("window_title_after", "")).strip()

        observe_args: Dict[str, Any] = {
            "ocr": bool(
                context_shift
                or anchor_precondition
                or confirm_instability
                or invalidation_state_anchor_mismatch
                or invalidation_transition_unstable
                or transition_force_probe
                or transition_preferred_probe == "ocr"
            )
        }
        observe = await self._execute_aux_action(
            action="computer_observe",
            args=observe_args,
            source=f"{source}:fallback",
            metadata=metadata,
            timeout_s=8,
            attempt=attempt,
        )
        chain_rows.append(
            {
                "action": observe.action,
                "status": observe.status,
                "error": observe.error,
                "output": observe.output if isinstance(observe.output, dict) else {},
            }
        )
        observe_output = observe.output if isinstance(observe.output, dict) else {}
        if not current_window_title:
            current_window_title = str(observe_output.get("window_title", "")).strip()

        refresh_budget = int(transition_refresh_budget)
        if context_shift and not observe_args.get("ocr", False):
            refresh_budget = max(refresh_budget, 1)
        if invalidation_transition_unstable:
            refresh_budget = max(refresh_budget, 1)
        if transition_require_dual_probe and transition_volatility_score >= 0.62:
            refresh_budget = max(refresh_budget, 2)
        for _refresh_index in range(max(0, min(3, refresh_budget))):
            observe_refresh = await self._execute_aux_action(
                action="computer_observe",
                args={"ocr": True},
                source=f"{source}:fallback",
                metadata=metadata,
                timeout_s=9 if invalidation_transition_unstable else 8,
                attempt=attempt,
            )
            chain_rows.append(
                {
                    "action": observe_refresh.action,
                    "status": observe_refresh.status,
                    "error": observe_refresh.error,
                    "output": observe_refresh.output if isinstance(observe_refresh.output, dict) else {},
                }
            )
            refresh_output = observe_refresh.output if isinstance(observe_refresh.output, dict) else {}
            if not current_window_title:
                current_window_title = str(refresh_output.get("window_title", "")).strip()

        text_match_mode = "contains"
        text_max_results = (
            6
            + (2 if no_state_change else 0)
            + (2 if confirm_instability else 0)
            + (1 if anchor_precondition else 0)
            + (2 if invalidation_state_anchor_mismatch else 0)
            + (2 if invalidation_transition_unstable else 0)
            + (2 if transition_require_dual_probe else 0)
        )
        if transition_preferred_probe == "ocr":
            text_match_mode = "contains"
            text_max_results += 2
        if transition_volatility_score >= 0.62:
            text_max_results += 2

        preloaded_text_targets: List[Dict[str, Any]] = []
        preloaded_ocr_status = "skip"
        preloaded_ocr_error = ""
        strong_ocr_target = False
        if transition_ocr_first or transition_fallback_bias in {"ocr_first", "hybrid_reconfirm"}:
            preloaded_find_text = await self._execute_aux_action(
                action="computer_find_text_targets",
                args={"query": query, "match_mode": text_match_mode, "max_results": int(max(4, text_max_results - 1))},
                source=f"{source}:fallback",
                metadata=metadata,
                timeout_s=10,
                attempt=attempt,
            )
            preloaded_ocr_status = str(preloaded_find_text.status or "").strip().lower() or "unknown"
            preloaded_ocr_error = str(preloaded_find_text.error or "").strip()
            preloaded_out = preloaded_find_text.output if isinstance(preloaded_find_text.output, dict) else {}
            preloaded_targets = preloaded_out.get("targets", [])
            if isinstance(preloaded_targets, list):
                preloaded_text_targets = [row for row in preloaded_targets if isinstance(row, dict)][:16]
            top_preloaded_confidence = 0.0
            if preloaded_text_targets:
                top_preloaded_confidence = self._coerce_float(
                    preloaded_text_targets[0].get("confidence", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
            strong_threshold = 0.78 if invalidation_state_anchor_mismatch else 0.84
            if transition_require_dual_probe:
                strong_threshold = max(strong_threshold, 0.88)
            strong_ocr_target = bool(top_preloaded_confidence >= strong_threshold)
            chain_rows.append(
                {
                    "action": preloaded_find_text.action,
                    "status": preloaded_find_text.status,
                    "error": preloaded_find_text.error,
                    "output": preloaded_out,
                    "probe_lane": "ocr_preload",
                    "top_confidence": round(top_preloaded_confidence, 6),
                    "strong_target": bool(strong_ocr_target),
                }
            )

        find_element_args: Dict[str, Any] = {
            "query": query,
            "max_results": (
                4
                + (2 if context_shift else 0)
                + (2 if anchor_precondition else 0)
                + (2 if invalidation_anchor_identity_mismatch else 0)
                + (2 if invalidation_state_anchor_mismatch else 0)
            ),
        }
        if transition_volatility_score >= 0.62:
            find_element_args["max_results"] = int(find_element_args.get("max_results", 4) or 4) + 2
        if current_window_title and not invalidation_state_anchor_mismatch:
            find_element_args["window_title"] = current_window_title
        element_candidates: List[Dict[str, Any]] = []
        skip_primary_accessibility = bool(
            strong_ocr_target
            and not invalidation_anchor_identity_mismatch
            and not transition_require_dual_probe
        )
        if not skip_primary_accessibility:
            find_element = await self._execute_aux_action(
                action="accessibility_find_element",
                args=find_element_args,
                source=f"{source}:fallback",
                metadata=metadata,
                timeout_s=10,
                attempt=attempt,
            )
            chain_rows.append(
                {
                    "action": find_element.action,
                    "status": find_element.status,
                    "error": find_element.error,
                    "output": find_element.output if isinstance(find_element.output, dict) else {},
                }
            )
            find_element_out = find_element.output if isinstance(find_element.output, dict) else {}
            raw_items = find_element_out.get("items", [])
            if isinstance(raw_items, list):
                element_candidates.extend([row for row in raw_items if isinstance(row, dict)])

            if (context_shift or invalidation_state_anchor_mismatch or invalidation_transition_unstable) and (
                find_element.status != "success" or not element_candidates
            ):
                broad_find_args: Dict[str, Any] = {
                    "query": query,
                    "max_results": int(find_element_args.get("max_results", 4)) + (3 if invalidation_transition_unstable else 2),
                }
                broad_find = await self._execute_aux_action(
                    action="accessibility_find_element",
                    args=broad_find_args,
                    source=f"{source}:fallback",
                    metadata=metadata,
                    timeout_s=10,
                    attempt=attempt,
                )
                chain_rows.append(
                    {
                        "action": broad_find.action,
                        "status": broad_find.status,
                        "error": broad_find.error,
                        "output": broad_find.output if isinstance(broad_find.output, dict) else {},
                    }
                )
                broad_out = broad_find.output if isinstance(broad_find.output, dict) else {}
                broad_items = broad_out.get("items", [])
                if isinstance(broad_items, list):
                    element_candidates.extend([row for row in broad_items if isinstance(row, dict)])
        else:
            chain_rows.append(
                {
                    "action": "accessibility_find_element",
                    "status": "skip",
                    "error": "skipped_due_to_strong_ocr_target",
                    "output": {},
                    "probe_lane": "accessibility",
                }
            )

        text_targets: List[Dict[str, Any]] = [dict(row) for row in preloaded_text_targets if isinstance(row, dict)]
        run_full_text_probe = bool(not text_targets or not strong_ocr_target or transition_require_dual_probe)
        if run_full_text_probe:
            find_text = await self._execute_aux_action(
            action="computer_find_text_targets",
            args={"query": query, "match_mode": text_match_mode, "max_results": text_max_results},
            source=f"{source}:fallback",
            metadata=metadata,
            timeout_s=12,
            attempt=attempt,
            )
            chain_rows.append(
                {
                    "action": find_text.action,
                    "status": find_text.status,
                    "error": find_text.error,
                    "output": find_text.output if isinstance(find_text.output, dict) else {},
                    "probe_lane": "ocr_full",
                }
            )
            text_out = find_text.output if isinstance(find_text.output, dict) else {}
            full_targets = text_out.get("targets", [])
            if isinstance(full_targets, list):
                for row in full_targets:
                    if isinstance(row, dict):
                        text_targets.append(dict(row))
        elif preloaded_ocr_status and preloaded_ocr_status != "skip":
            chain_rows.append(
                {
                    "action": "computer_find_text_targets",
                    "status": "skip",
                    "error": "full_ocr_probe_not_required_after_strong_preload",
                    "output": {"preloaded_status": preloaded_ocr_status, "preloaded_error": preloaded_ocr_error},
                    "probe_lane": "ocr_full",
                }
            )

        element_items = element_candidates
        ranked_candidates = self._rank_desktop_recovery_candidates(
            step=step,
            query=query,
            metadata=metadata,
            element_items=element_items if isinstance(element_items, list) else [],
            text_targets=text_targets if isinstance(text_targets, list) else [],
        )
        selected_candidate = ranked_candidates[0] if ranked_candidates else {}
        suggestion: Dict[str, Any] = {}
        if isinstance(selected_candidate, dict):
            candidate_kind = str(selected_candidate.get("kind", "")).strip().lower()
            if candidate_kind == "accessibility":
                suggestion["element_id"] = str(selected_candidate.get("element_id", "")).strip()
                suggestion["target_mode"] = "accessibility"
                suggestion["confidence"] = self._coerce_float(
                    selected_candidate.get("confidence", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
            elif candidate_kind == "ocr":
                suggestion["target_mode"] = "ocr"
                suggestion["confidence"] = self._coerce_float(
                    selected_candidate.get("confidence", 0.0),
                    minimum=0.0,
                    maximum=1.0,
                    default=0.0,
                )
                suggestion["ocr_target"] = {
                    "x": selected_candidate.get("x"),
                    "y": selected_candidate.get("y"),
                    "confidence": selected_candidate.get("confidence"),
                }
        if ranked_candidates:
            self.telemetry.emit(
                "step.desktop_recovery_candidates_scored",
                {
                    "step_id": step.step_id,
                    "action": step.action,
                    "query": query[:120],
                    "candidate_count": len(ranked_candidates),
                    "selected_kind": str(selected_candidate.get("kind", "")) if isinstance(selected_candidate, dict) else "",
                    "selected_score": self._coerce_float(
                        selected_candidate.get("score", 0.0) if isinstance(selected_candidate, dict) else 0.0,
                        minimum=0.0,
                        maximum=2.0,
                        default=0.0,
                    ),
                },
            )

        recovery_patch, recovery_confidence = self._derive_desktop_recovery_patch(step=step, suggestion=suggestion)
        auto_patch_threshold = self._desktop_recovery_patch_threshold(step=step, metadata=metadata)
        if transition_patch_threshold_boost > 0.0:
            should_boost_threshold = bool(
                transition_require_dual_probe
                or invalidation_transition_unstable
                or invalidation_state_anchor_mismatch
                or transition_volatility_score >= 0.52
            )
            if should_boost_threshold:
                auto_patch_threshold = min(0.98, auto_patch_threshold + transition_patch_threshold_boost)
        applied_patch: Dict[str, Any] = {}
        if recovery_patch and recovery_confidence >= auto_patch_threshold and isinstance(step.args, dict):
            for key, value in recovery_patch.items():
                current = step.args.get(key)
                if current is None:
                    applied_patch[key] = value
                    continue
                if isinstance(current, str):
                    current_clean = current.strip().lower()
                    if current_clean in {"", "auto"}:
                        applied_patch[key] = value
                        continue
                if key in {"x", "y", "target_index"}:
                    applied_patch[key] = value
            if applied_patch:
                step.args.update(applied_patch)
                state_hash = ""
                desktop_state = result.evidence.get("desktop_state") if isinstance(result.evidence, dict) else {}
                if isinstance(desktop_state, dict):
                    state_hash = str(desktop_state.get("state_hash", "")).strip().lower()
                if state_hash:
                    metadata["__desktop_retry_anchor_state_hash"] = state_hash
                self.telemetry.emit(
                    "step.desktop_recovery_patch_applied",
                    {
                        "step_id": step.step_id,
                        "action": step.action,
                        "patch": dict(applied_patch),
                        "confidence": round(float(recovery_confidence), 6),
                        "threshold": round(float(auto_patch_threshold), 6),
                    },
                )

        if not isinstance(result.evidence, dict):
            result.evidence = {}
        result.evidence["desktop_recovery"] = {
            "query": query,
            "window_title": current_window_title,
            "guardrail_tags": guardrail_tags[:12],
            "invalidation_tags": invalidation_tags[:16],
            "fallback_profile": {
                "context_shift": bool(context_shift),
                "anchor_precondition": bool(anchor_precondition),
                "confirm_instability": bool(confirm_instability),
                "no_state_change": bool(no_state_change),
                "state_anchor_mismatch": bool(invalidation_state_anchor_mismatch),
                "transition_unstable": bool(invalidation_transition_unstable),
                "anchor_identity_mismatch": bool(invalidation_anchor_identity_mismatch),
                "transition_contract_force_probe": bool(transition_force_probe),
                "transition_contract_preferred_probe": transition_preferred_probe,
                "transition_contract_dual_probe": bool(transition_require_dual_probe),
                "transition_contract_volatility": round(transition_volatility_score, 6),
                "transition_contract_fallback_bias": transition_fallback_bias,
                "transition_contract_ocr_first": bool(transition_ocr_first),
                "transition_contract_refresh_budget": int(transition_refresh_budget),
                "transition_contract_patch_threshold_boost": round(transition_patch_threshold_boost, 6),
            },
            "transition_contract": transition_contract if isinstance(transition_contract, dict) else {},
            "chain": chain_rows,
            "suggestion": suggestion,
            "ranked_candidates": ranked_candidates[:6],
            "selected_candidate": selected_candidate if isinstance(selected_candidate, dict) else {},
            "retry_patch": dict(recovery_patch) if recovery_patch else {},
            "retry_patch_confidence": round(float(recovery_confidence), 6),
            "retry_patch_threshold": round(float(auto_patch_threshold), 6),
            "applied_retry_patch": dict(applied_patch),
        }
        if suggestion:
            result.error = (
                f"{result.error or 'Desktop action failed.'} "
                "Recovery diagnostics captured actionable UI anchors for replanning."
            ).strip()
        return result

    def _rank_desktop_recovery_candidates(
        self,
        *,
        step: PlanStep,
        query: str,
        metadata: Dict[str, Any],
        element_items: List[Any],
        text_targets: List[Any],
    ) -> List[Dict[str, Any]]:
        clean_action = str(step.action or "").strip().lower()
        guardrail_tags = self._desktop_guardrail_tags_for_action(
            action=clean_action,
            metadata=metadata,
            limit=8,
        )
        guardrail_tag_set = set(guardrail_tags)
        invalidation_tags = self._desktop_anchor_invalidation_tags_for_action(
            action=clean_action,
            metadata=metadata,
            limit=10,
        )
        invalidation_tag_set = set(invalidation_tags)
        invalidation_context_shift = bool(
            "window_context_mismatch" in invalidation_tag_set
            or "app_context_mismatch" in invalidation_tag_set
            or "guardrail_context_shift" in invalidation_tag_set
            or "guardrail_context_churn" in invalidation_tag_set
        )
        invalidation_state_mismatch = bool(
            "pre_state_mismatch" in invalidation_tag_set
            or "post_state_anchor_mismatch" in invalidation_tag_set
            or "retry_state_anchor_mismatch" in invalidation_tag_set
            or "state_anchor_collision" in invalidation_tag_set
        )
        invalidation_transition_unstable = bool(
            "guardrail_transition_volatility" in invalidation_tag_set
            or "guardrail_transition_burst" in invalidation_tag_set
            or "transition_profile_low_success" in invalidation_tag_set
            or "transition_profile_high_churn" in invalidation_tag_set
            or "transition_profile_churn_cluster" in invalidation_tag_set
        )
        invalidation_identity_mismatch = bool(
            "element_anchor_mismatch" in invalidation_tag_set
            or "control_type_anchor_mismatch" in invalidation_tag_set
            or "query_anchor_mismatch" in invalidation_tag_set
            or "target_mode_mismatch" in invalidation_tag_set
        )
        transition_contract = self._desktop_transition_contract_for_action(
            action=clean_action,
            metadata=metadata,
            limit=16,
        )
        preferred_probe = (
            str(transition_contract.get("preferred_probe", "hybrid")).strip().lower()
            if isinstance(transition_contract, dict)
            else "hybrid"
        )
        fallback_bias = (
            str(transition_contract.get("fallback_bias", "balanced")).strip().lower()
            if isinstance(transition_contract, dict)
            else "balanced"
        )
        transition_volatility_score = self._coerce_float(
            transition_contract.get("volatility_score", 0.0) if isinstance(transition_contract, dict) else 0.0,
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )
        require_dual_probe = bool(
            transition_contract.get("require_dual_probe", False)
        ) if isinstance(transition_contract, dict) else False

        anchor_hints: List[Dict[str, Any]] = []
        if self.desktop_anchor_memory is not None and query.strip():
            try:
                hint_rows = self.desktop_anchor_memory.hints(query=query, limit=8)
                anchor_hints = [row for row in hint_rows if isinstance(row, dict)]
            except Exception:
                anchor_hints = []

        candidates: List[Dict[str, Any]] = []
        for item in element_items[:10]:
            if not isinstance(item, dict):
                continue
            element_id = str(item.get("element_id", "")).strip()
            if not element_id:
                continue
            raw_confidence = self._coerce_float(
                item.get("match_score", item.get("confidence", 0.0)),
                minimum=0.0,
                maximum=5.0,
                default=0.0,
            )
            confidence = raw_confidence if raw_confidence <= 1.0 else min(1.0, raw_confidence / 1.2)
            score = confidence
            signals: List[str] = [f"match:{round(confidence, 6)}"]
            if clean_action in {"computer_click_target", "accessibility_invoke_element"}:
                score += 0.1
                signals.append("action_compatibility:accessibility")
            else:
                score -= 0.05
            if preferred_probe == "accessibility":
                score += 0.08
                signals.append("transition_contract:probe_bias_accessibility")
            elif preferred_probe == "ocr":
                score -= 0.06
                signals.append("transition_contract:probe_bias_ocr")
            if fallback_bias in {"ocr_first", "hybrid_reconfirm"}:
                score -= 0.04
                signals.append(f"transition_contract:fallback_bias_{fallback_bias}")
            if require_dual_probe:
                score -= 0.03
                signals.append("transition_contract:dual_probe_required")
            control_type = str(item.get("control_type", "")).strip().lower()
            if control_type in {"button", "menuitem", "hyperlink"}:
                score += 0.03
                signals.append(f"control:{control_type}")
            if "anchor_precondition_failed" in guardrail_tag_set:
                score -= 0.1
                signals.append("guardrail:anchor_precondition_failed")
            if "window_transition" in guardrail_tag_set or "app_transition" in guardrail_tag_set:
                score -= 0.08
                signals.append("guardrail:context_shift")
            if "confirm_policy_failed" in guardrail_tag_set or "confirm_check_failed" in guardrail_tag_set:
                score -= 0.04
                signals.append("guardrail:confirm_instability")
            if "no_state_change" in guardrail_tag_set:
                score += 0.03
                signals.append("guardrail:no_state_change_retarget")
            if invalidation_context_shift:
                score -= 0.08
                signals.append("invalidation:context_shift")
            if invalidation_state_mismatch:
                score -= 0.1
                signals.append("invalidation:state_mismatch")
            if invalidation_transition_unstable:
                score -= 0.06
                signals.append("invalidation:transition_unstable")
            if invalidation_identity_mismatch:
                score -= 0.07
                signals.append("invalidation:identity_mismatch")
            if transition_volatility_score >= 0.62:
                score -= min(0.16, transition_volatility_score * 0.18)
                signals.append("transition_contract:high_volatility")
            for hint in anchor_hints:
                hint_element = str(hint.get("element_id", "")).strip()
                if hint_element and hint_element.lower() == element_id.lower():
                    score += 0.18
                    signals.append("memory:element_match")
                    break
            candidates.append(
                {
                    "kind": "accessibility",
                    "element_id": element_id,
                    "control_type": control_type,
                    "confidence": round(confidence, 6),
                    "score": round(max(0.0, min(2.0, score)), 6),
                    "signals": signals[:8],
                }
            )

        for row in text_targets[:14]:
            if not isinstance(row, dict):
                continue
            x = row.get("center_x")
            y = row.get("center_y")
            if x is None or y is None:
                continue
            confidence = self._coerce_float(
                row.get("confidence", 0.0),
                minimum=0.0,
                maximum=1.0,
                default=0.0,
            )
            score = confidence
            signals = [f"ocr_match:{round(confidence, 6)}"]
            if clean_action in {"computer_click_target", "computer_click_text"}:
                score += 0.08
                signals.append("action_compatibility:ocr")
            else:
                score -= 0.05
            if preferred_probe == "ocr":
                score += 0.09
                signals.append("transition_contract:probe_bias_ocr")
            elif preferred_probe == "accessibility":
                score -= 0.05
                signals.append("transition_contract:probe_bias_accessibility")
            if fallback_bias in {"ocr_first", "hybrid_reconfirm"}:
                score += 0.05
                signals.append(f"transition_contract:fallback_bias_{fallback_bias}")
            if require_dual_probe:
                score += 0.02
                signals.append("transition_contract:dual_probe_required")
            if "no_state_change" in guardrail_tag_set:
                score += 0.04
                signals.append("guardrail:no_state_change")
            if "window_transition" in guardrail_tag_set or "app_transition" in guardrail_tag_set:
                score += 0.02
                signals.append("guardrail:context_shift_ocr_bias")
            if "anchor_precondition_failed" in guardrail_tag_set:
                score += 0.05
                signals.append("guardrail:anchor_probe_failed_ocr_bias")
            if "confirm_policy_failed" in guardrail_tag_set or "confirm_check_failed" in guardrail_tag_set:
                score += 0.03
                signals.append("guardrail:confirm_instability_ocr_bias")
            if invalidation_context_shift:
                score += 0.04
                signals.append("invalidation:context_shift_ocr_bias")
            if invalidation_state_mismatch:
                score += 0.06
                signals.append("invalidation:state_mismatch_ocr_bias")
            if invalidation_transition_unstable:
                score += 0.08
                signals.append("invalidation:transition_unstable_ocr_bias")
            if invalidation_identity_mismatch:
                score += 0.04
                signals.append("invalidation:identity_mismatch_ocr_bias")
            if transition_volatility_score >= 0.62:
                score += min(0.12, transition_volatility_score * 0.1)
                signals.append("transition_contract:high_volatility_ocr_bias")
            try:
                x_value = int(x)
                y_value = int(y)
            except Exception:
                continue
            for hint in anchor_hints:
                hint_x = hint.get("x")
                hint_y = hint.get("y")
                if hint_x is None or hint_y is None:
                    continue
                try:
                    dx = abs(int(hint_x) - x_value)
                    dy = abs(int(hint_y) - y_value)
                except Exception:
                    continue
                if dx <= 48 and dy <= 48:
                    score += 0.12
                    signals.append("memory:coordinate_near_match")
                    break
            candidates.append(
                {
                    "kind": "ocr",
                    "x": x_value,
                    "y": y_value,
                    "confidence": round(confidence, 6),
                    "score": round(max(0.0, min(2.0, score)), 6),
                    "signals": signals[:8],
                }
            )

        candidates.sort(
            key=lambda item: (
                -self._coerce_float(item.get("score", 0.0), minimum=0.0, maximum=2.0, default=0.0),
                -self._coerce_float(item.get("confidence", 0.0), minimum=0.0, maximum=1.0, default=0.0),
                str(item.get("kind", "")),
                str(item.get("element_id", "")),
            )
        )
        return candidates[:16]

    @staticmethod
    def _derive_desktop_recovery_patch(*, step: PlanStep, suggestion: Dict[str, Any]) -> tuple[Dict[str, Any], float]:
        clean_action = str(step.action or "").strip().lower()
        patch: Dict[str, Any] = {}
        confidence = 0.0
        if not isinstance(suggestion, dict) or not suggestion:
            return (patch, confidence)

        element_id = str(suggestion.get("element_id", "")).strip()
        target_mode = str(suggestion.get("target_mode", "")).strip().lower()
        try:
            direct_confidence = float(suggestion.get("confidence", 0.0) or 0.0)
        except Exception:
            direct_confidence = 0.0
        direct_confidence = max(0.0, min(1.0, direct_confidence))

        ocr_target = suggestion.get("ocr_target", {})
        ocr_x = None
        ocr_y = None
        ocr_confidence = 0.0
        if isinstance(ocr_target, dict):
            ocr_x = ocr_target.get("x")
            ocr_y = ocr_target.get("y")
            try:
                ocr_confidence = float(ocr_target.get("confidence", 0.0) or 0.0)
            except Exception:
                ocr_confidence = 0.0
            ocr_confidence = max(0.0, min(1.0, ocr_confidence))

        if clean_action in {"computer_click_target", "accessibility_invoke_element"} and element_id:
            patch["element_id"] = element_id
            if clean_action == "computer_click_target":
                patch["target_mode"] = "accessibility" if target_mode in {"", "accessibility"} else target_mode
            confidence = max(confidence, direct_confidence)
        if clean_action in {"computer_click_target", "computer_click_text"} and ocr_x is not None and ocr_y is not None:
            patch["x"] = ocr_x
            patch["y"] = ocr_y
            patch["target_index"] = 0
            if clean_action == "computer_click_target" and "target_mode" not in patch:
                patch["target_mode"] = "ocr"
            confidence = max(confidence, ocr_confidence)
        return (patch, confidence)

    @staticmethod
    def _desktop_recovery_patch_threshold(*, step: PlanStep, metadata: Dict[str, Any]) -> float:
        verify_cfg = step.verify if isinstance(step.verify, dict) else {}
        guardrail_cfg = verify_cfg.get("guardrail")
        verify_level = str(guardrail_cfg.get("level", "")).strip().lower() if isinstance(guardrail_cfg, dict) else ""
        runtime_level = str(metadata.get("guardrail_recommended_level", "")).strip().lower()
        strict_mode = verify_level in {"high", "critical", "strict"} or runtime_level in {"high", "critical"}
        return 0.62 if strict_mode else 0.38

    @staticmethod
    def _should_attempt_desktop_fallback(*, step: PlanStep, result: ActionResult, metadata: Dict[str, Any]) -> bool:
        if step.action not in {"computer_click_target", "computer_click_text", "accessibility_invoke_element"}:
            return False
        if result.status not in {"failed", "blocked"}:
            return False
        raw_error = str(result.error or "").strip().lower()
        if any(token in raw_error for token in ("target", "not visible", "confirm policy failed", "state change", "ui element")):
            return True
        evidence = result.evidence if isinstance(result.evidence, dict) else {}
        desktop_state = evidence.get("desktop_state")
        if isinstance(desktop_state, dict):
            if bool(desktop_state.get("window_transition", False)) or bool(desktop_state.get("app_transition", False)):
                return True
        level = str(metadata.get("guardrail_recommended_level", "")).strip().lower()
        if level in {"high", "critical"}:
            return True
        return False

    async def _execute_aux_action(
        self,
        *,
        action: str,
        args: Dict[str, Any],
        source: str,
        metadata: Dict[str, Any],
        timeout_s: int,
        attempt: int,
    ) -> ActionResult:
        request = ActionRequest(action=action, args=args, source=source, metadata=metadata)
        approval = self._enforce_approval(request)
        if approval is not None:
            approval.attempt = attempt
            return approval
        allowed, reason = self.policy_guard.authorize(request)
        if not allowed:
            return ActionResult(action=action, status="blocked", error=reason, attempt=attempt)
        row = await self.registry.execute(request, timeout_s=max(1, min(int(timeout_s), 60)))
        row.attempt = attempt
        return row

    def _apply_retry_contract_runtime_strategy(
        self,
        *,
        step: PlanStep,
        retry_contract: Dict[str, Any],
        metadata: Dict[str, Any],
        policy_profile: str,
        recovery_profile: str,
        verification_strictness: str,
    ) -> Dict[str, str]:
        contract = retry_contract if isinstance(retry_contract, dict) else {}
        if not contract:
            return {}
        mode = str(contract.get("mode", "")).strip().lower()
        if not mode:
            return {}
        risk = self._coerce_float(contract.get("risk_score", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        operation_class = str(contract.get("operation_class", "")).strip().lower()
        mission_profile = str(contract.get("mission_profile", "")).strip().lower()
        clean_action = str(step.action or "").strip().lower()
        is_external_mutation = self._is_external_mutation_action(clean_action) or operation_class in {"write", "mutate", "auth"}

        next_policy = str(policy_profile or "").strip().lower()
        next_recovery = str(recovery_profile or "").strip().lower()
        next_verification = str(verification_strictness or "").strip().lower()
        next_branch = str(metadata.get("external_branch_strategy", "warn")).strip().lower() or "warn"
        checkpoint_mode = str(metadata.get("external_remediation_checkpoint_mode", "auto")).strip().lower() or "auto"

        if mode in {"stabilize", "abort"} or risk >= 0.86:
            next_recovery = "safe"
            next_verification = "strict"
            next_branch = "enforce"
            if next_policy in {"automation_power", "interactive"}:
                next_policy = "automation_safe"
            metadata["execution_allow_parallel"] = False
            metadata["execution_max_parallel_steps"] = 1
            checkpoint_mode = "strict"
        elif mode == "adaptive_backoff":
            if not next_recovery:
                next_recovery = "balanced"
            if next_recovery == "aggressive":
                next_recovery = "balanced"
            if risk >= 0.62:
                next_recovery = "safe"
            next_verification = self._max_verification_level(next_verification, "standard")
            if risk >= 0.74 or mission_profile in {"defensive", "cautious"} or is_external_mutation:
                next_verification = self._max_verification_level(next_verification, "strict")
                next_branch = "enforce"
            else:
                next_branch = "warn"
            checkpoint_mode = "strict" if risk >= 0.66 else "standard"
            if risk >= 0.72:
                metadata["execution_allow_parallel"] = False
                metadata["execution_max_parallel_steps"] = 1
        elif mode == "probe_then_backoff":
            next_verification = self._max_verification_level(next_verification, "standard")
            if is_external_mutation and risk >= 0.55:
                next_branch = "enforce"
                checkpoint_mode = "standard"
            else:
                next_branch = "warn"
                checkpoint_mode = "standard"
        elif mode == "light_retry":
            if risk >= 0.58:
                next_verification = self._max_verification_level(next_verification, "standard")
                checkpoint_mode = "standard"

        metadata["policy_profile"] = next_policy
        metadata["recovery_profile"] = next_recovery
        metadata["verification_strictness"] = next_verification
        metadata["external_branch_strategy"] = next_branch
        metadata["external_remediation_checkpoint_mode"] = checkpoint_mode
        metadata["__external_runtime_strategy_mode"] = mode
        metadata["__external_runtime_strategy_risk"] = risk
        metadata["__external_runtime_strategy_operation"] = operation_class
        metadata["__external_runtime_strategy_profile"] = mission_profile

        self.telemetry.emit(
            "step.external_runtime_strategy_applied",
            {
                "step_id": step.step_id,
                "action": clean_action,
                "mode": mode,
                "risk": round(risk, 6),
                "operation_class": operation_class,
                "mission_profile": mission_profile,
                "policy_profile": next_policy,
                "recovery_profile": next_recovery,
                "verification_strictness": next_verification,
                "external_branch_strategy": next_branch,
                "checkpoint_mode": checkpoint_mode,
            },
        )
        return {
            "policy_profile": next_policy,
            "recovery_profile": next_recovery,
            "verification_strictness": next_verification,
        }

    @staticmethod
    def _max_verification_level(current: str, target: str) -> str:
        order = {"off": 0, "standard": 1, "strict": 2}
        clean_current = str(current or "").strip().lower() or "off"
        clean_target = str(target or "").strip().lower() or "off"
        current_rank = int(order.get(clean_current, 0))
        target_rank = int(order.get(clean_target, 0))
        if target_rank >= current_rank:
            return clean_target
        return clean_current

    def _apply_retry_hint(
        self,
        *,
        step: PlanStep,
        retry_hint: Dict[str, Any],
        retry_contract: Dict[str, Any] | None = None,
    ) -> None:
        verify_payload = step.verify if isinstance(step.verify, dict) else {}
        retry_payload = verify_payload.get("retry")
        retry_cfg = retry_payload if isinstance(retry_payload, dict) else {}
        updated = False
        for key, minimum, maximum in (
            ("base_delay_s", 0.0, 30.0),
            ("max_delay_s", 0.1, 60.0),
            ("multiplier", 1.0, 5.0),
            ("jitter_s", 0.0, 5.0),
        ):
            if key not in retry_hint:
                continue
            try:
                value = float(retry_hint.get(key, retry_cfg.get(key, 0.0)))
            except Exception:
                continue
            value = max(minimum, min(maximum, value))
            previous = retry_cfg.get(key)
            if previous is None or float(previous) != float(value):
                retry_cfg[key] = round(value, 3)
                updated = True
        contract = retry_contract if isinstance(retry_contract, dict) else {}
        if contract:
            timing = contract.get("timing", {})
            timing_row = timing if isinstance(timing, dict) else {}
            for key, minimum, maximum in (
                ("base_delay_s", 0.0, 45.0),
                ("max_delay_s", 0.1, 120.0),
                ("multiplier", 1.0, 6.0),
                ("jitter_s", 0.0, 8.0),
            ):
                if key not in timing_row:
                    continue
                try:
                    value = float(timing_row.get(key, retry_cfg.get(key, 0.0)))
                except Exception:
                    continue
                value = max(minimum, min(maximum, value))
                previous = retry_cfg.get(key)
                if previous is None or float(previous) != float(value):
                    retry_cfg[key] = round(value, 3)
                    updated = True
            budget = contract.get("budget", {})
            budget_row = budget if isinstance(budget, dict) else {}
            try:
                max_attempts = int(budget_row.get("max_attempts", 0) or 0)
            except Exception:
                max_attempts = 0
            if max_attempts > 0:
                clamped_attempts = max(1, min(8, max_attempts))
                if int(step.max_retries) != int(clamped_attempts):
                    step.max_retries = clamped_attempts
                    updated = True
            try:
                suggested_timeout = int(budget_row.get("suggested_timeout_s", 0) or 0)
            except Exception:
                suggested_timeout = 0
            if suggested_timeout > 0:
                bounded_timeout = max(3, min(180, suggested_timeout))
                if int(step.timeout_s) != int(bounded_timeout):
                    step.timeout_s = bounded_timeout
                    updated = True
            mode = str(contract.get("mode", "")).strip().lower()
            if mode in {"stabilize", "abort"} and int(step.max_retries) > 2:
                step.max_retries = 2
                updated = True
            elif mode in {"adaptive_backoff", "probe_then_backoff"} and int(step.max_retries) < 2:
                step.max_retries = 2
                updated = True
            if mode in {"stabilize"}:
                step.timeout_s = max(int(step.timeout_s), 35)
                updated = True
        if updated:
            verify_payload["retry"] = retry_cfg
            step.verify = verify_payload
            self.telemetry.emit(
                "step.external_retry_policy_applied",
                {
                    "step_id": step.step_id,
                    "action": step.action,
                    "max_retries": int(step.max_retries),
                    "timeout_s": int(step.timeout_s),
                    "has_retry_hint": bool(retry_hint),
                    "has_retry_contract": bool(contract),
                    "retry_contract_mode": str(contract.get("mode", "")).strip().lower() if contract else "",
                    "retry_contract_risk": float(contract.get("risk_score", 0.0) or 0.0) if contract else 0.0,
                },
            )

    @staticmethod
    def _retry_contract_delay_floor(*, retry_contract: Dict[str, Any]) -> float:
        payload = retry_contract if isinstance(retry_contract, dict) else {}
        timing = payload.get("timing", {})
        budget = payload.get("budget", {})
        timing_row = timing if isinstance(timing, dict) else {}
        budget_row = budget if isinstance(budget, dict) else {}
        values: list[float] = []
        for key in ("min_delay_s", "base_delay_s"):
            try:
                value = float(timing_row.get(key, 0.0) or 0.0)
            except Exception:
                value = 0.0
            if value > 0.0:
                values.append(value)
        try:
            cooldown_s = float(budget_row.get("cooldown_recommendation_s", 0.0) or 0.0)
        except Exception:
            cooldown_s = 0.0
        if cooldown_s > 0.0:
            values.append(min(75.0, cooldown_s * 0.2))
        if not values:
            return 0.0
        return max(0.0, min(75.0, max(values)))

    @staticmethod
    def _is_retryable_external_preflight(*, failure_category: str, status: str) -> bool:
        clean_category = str(failure_category or "").strip().lower() or "unknown"
        clean_status = str(status or "").strip().lower()
        if clean_status not in {"blocked", "error"}:
            return False
        if clean_category in {"non_retryable"}:
            return False
        if clean_category in {"auth", "rate_limited", "timeout", "transient", "unknown"}:
            return True
        return clean_status == "blocked"

    @staticmethod
    def _attach_recovery_evidence(
        result: ActionResult,
        *,
        step: PlanStep,
        attempt: int,
        retry_history: List[Dict[str, Any]],
        last_category: str = "",
        last_reason: str = "",
    ) -> None:
        if not isinstance(result.evidence, dict):
            result.evidence = {}

        normalized_history: List[Dict[str, Any]] = []
        for item in retry_history:
            if not isinstance(item, dict):
                continue
            try:
                history_attempt = max(1, int(item.get("attempt", 1)))
            except Exception:  # noqa: BLE001
                history_attempt = 1
            try:
                delay_s = float(item.get("delay_s", 0.0))
                delay_s = max(0.0, min(delay_s, 60.0))
            except Exception:  # noqa: BLE001
                delay_s = 0.0
            normalized_history.append(
                {
                    "attempt": history_attempt,
                    "delay_s": delay_s,
                    "reason": str(item.get("reason", "")).strip(),
                    "category": str(item.get("category", "")).strip().lower(),
                }
            )

        resolved_category = str(last_category or "").strip().lower()
        if not resolved_category and normalized_history:
            resolved_category = str(normalized_history[-1].get("category", "")).strip().lower()

        recovery_payload: Dict[str, Any] = {
            "attempt": max(1, int(attempt)),
            "max_retries": max(0, int(step.max_retries)),
            "retry_count": len(normalized_history),
            "last_category": resolved_category,
            "retry_history": normalized_history,
        }
        if str(last_reason or "").strip():
            recovery_payload["decision_reason"] = str(last_reason).strip()
        result.evidence["recovery"] = recovery_payload

    @staticmethod
    def _interrupted_result(action: str, reason: str, attempt: int = 1) -> ActionResult:
        message = str(reason or "").strip() or "Goal cancelled by user request."
        return ActionResult(
            action=action,
            status="blocked",
            error=message,
            output={"status": "error", "interrupted": True, "message": message},
            attempt=attempt,
        )

    @staticmethod
    def _resolve_interrupt_reason(default_reason: str, provider: Callable[[], str] | None) -> str:
        if provider is not None:
            try:
                resolved = str(provider() or "").strip()
                if resolved:
                    return resolved
            except Exception:  # noqa: BLE001
                pass
        return str(default_reason or "").strip() or "Goal cancelled by user request."

    def _resolve_circuit_scope(self, *, step: PlanStep, metadata: Dict[str, Any]) -> str:
        action = str(step.action or "").strip().lower()
        if not action.startswith("external_") and not action.startswith("oauth_token_"):
            return ""
        args = step.args if isinstance(step.args, dict) else {}
        candidates = [
            args.get("provider"),
            metadata.get("provider"),
        ]
        for value in candidates:
            provider = str(value or "").strip().lower()
            if not provider or provider == "auto":
                continue
            if provider in {"gmail", "google_docs", "google_tasks", "google_calendar"}:
                return "google"
            if provider in {
                "microsoft",
                "microsoft_graph",
                "microsoft_graph_mail",
                "microsoft_graph_todo",
                "microsoft_graph_calendar",
                "microsoft_graph_drive",
            }:
                return "graph"
            return provider
        return ""

    @staticmethod
    def _resolve_circuit_scope_from_output(output: Dict[str, Any]) -> str:
        provider = str(output.get("provider", "")).strip().lower()
        if not provider or provider == "auto":
            return ""
        if provider in {"gmail", "google_docs", "google_tasks", "google_calendar"}:
            return "google"
        if provider in {
            "microsoft",
            "microsoft_graph",
            "microsoft_graph_mail",
            "microsoft_graph_todo",
            "microsoft_graph_calendar",
            "microsoft_graph_drive",
        }:
            return "graph"
        return provider

    def _check_circuit_breaker(self, *, step: PlanStep, attempt: int, scope: str = "") -> ActionResult | None:
        if self.circuit_breaker is None:
            return None
        blocked, reason, retry_after_s = self.circuit_breaker.should_block(step.action, scope=scope)
        if not blocked:
            return None
        message = f"{reason} Retry after {max(0.0, retry_after_s):.1f}s."
        result = ActionResult(
            action=step.action,
            status="blocked",
            error=message,
            output={
                "status": "error",
                "circuit_breaker": {
                    "open": True,
                    "retry_after_s": round(max(0.0, retry_after_s), 3),
                    "reason": reason,
                    "scope": scope,
                },
            },
            attempt=attempt,
        )
        if not isinstance(result.evidence, dict):
            result.evidence = {}
        result.evidence["circuit_breaker"] = {
            "open": True,
            "retry_after_s": round(max(0.0, retry_after_s), 3),
            "reason": reason,
            "scope": scope,
        }
        return result

    def _record_circuit_breaker(
        self,
        *,
        action: str,
        status: str,
        failure_category: str = "",
        error: str = "",
        scope: str = "",
    ) -> None:
        if self.circuit_breaker is None:
            return
        outcome = self.circuit_breaker.record_outcome(
            action=action,
            status=status,
            failure_category=failure_category,
            error=error,
            scope=scope,
        )
        if bool(outcome.get("opened", False)):
            state = outcome.get("state", {})
            if isinstance(state, dict):
                self.telemetry.emit(
                    "step.circuit_opened",
                    {
                        "action": str(state.get("action", action)),
                        "scope": str(state.get("scope", scope)),
                        "opened_count": int(state.get("opened_count", 0) or 0),
                        "open_until": str(state.get("open_until", "")),
                        "last_failure_category": str(state.get("last_failure_category", "")),
                    },
                )

    @staticmethod
    def _classify_error_category(message: str) -> str:
        lowered = str(message or "").strip().lower()
        if not lowered:
            return "unknown"
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

    def _enforce_approval(self, request: ActionRequest) -> ActionResult | None:
        metadata = request.metadata if isinstance(request.metadata, dict) else {}
        if self._coerce_bool(metadata.get("__skip_approval", False), default=False):
            return None
        definition = self.registry.get(request.action)
        if definition is None or not definition.requires_confirmation:
            return None

        approval_id = self.approval_gate.extract_approval_id(metadata, request.action)
        if approval_id:
            ok, reason, record = self.approval_gate.consume(
                approval_id,
                action=request.action,
                args=request.args,
                source=request.source,
            )
            if ok:
                return None

            payload = {
                "status": "error",
                "approval_required": True,
                "message": reason,
                "approval_id": approval_id,
            }
            if record:
                payload["approval"] = record.to_dict()
            return ActionResult(action=request.action, status="blocked", error=reason, output=payload)

        record = self.approval_gate.request(
            action=request.action,
            args=request.args,
            source=request.source,
            reason=f"Action '{request.action}' requires explicit user approval.",
        )
        reason = (
            f"Approval required for '{request.action}'. "
            f"Approve ticket {record.approval_id} and retry with metadata.approval_id."
        )
        return ActionResult(
            action=request.action,
            status="blocked",
            error=reason,
            output={
                "status": "error",
                "approval_required": True,
                "message": reason,
                "approval": record.to_dict(),
            },
        )

    async def _run_confirm_check(
        self,
        *,
        step: PlanStep,
        result: ActionResult,
        source: str,
        attempt: int,
    ) -> ActionResult | None:
        if result.status != "success":
            return None

        configs = self._extract_confirm_configs(step.verify if isinstance(step.verify, dict) else {})
        if not configs:
            return None

        return await self._run_confirm_check_with_config(
            confirm_cfg=configs[0],
            step=step,
            result=result,
            source=source,
            attempt=attempt,
        )

    async def _run_confirm_checks(
        self,
        *,
        step: PlanStep,
        result: ActionResult,
        source: str,
        attempt: int,
    ) -> List[ActionResult]:
        if result.status != "success":
            return []

        configs = self._extract_confirm_configs(step.verify if isinstance(step.verify, dict) else {})
        if not configs:
            return []

        rows: List[ActionResult] = []
        for config in configs:
            confirm_result = await self._run_confirm_check_with_config(
                confirm_cfg=config,
                step=step,
                result=result,
                source=source,
                attempt=attempt,
            )
            if confirm_result is not None:
                rows.append(confirm_result)
        return rows

    async def _run_confirm_check_with_config(
        self,
        *,
        confirm_cfg: Dict[str, Any],
        step: PlanStep,
        result: ActionResult,
        source: str,
        attempt: int,
    ) -> ActionResult | None:
        action = str(confirm_cfg.get("action", "")).strip()
        if not action:
            return None

        raw_args = confirm_cfg.get("args", {})
        args = self._render_payload(raw_args, step=step, result=result)
        if not isinstance(args, dict):
            args = {}
        timeout_s = int(confirm_cfg.get("timeout_s", min(step.timeout_s, 20)))
        timeout_s = max(1, min(timeout_s, 60))
        attempts = int(confirm_cfg.get("attempts", 1))
        attempts = max(1, min(attempts, 5))
        delay_s = float(confirm_cfg.get("delay_s", 0.2))
        delay_s = max(0.0, min(delay_s, 3.0))

        last_result: ActionResult | None = None
        for index in range(attempts):
            request = ActionRequest(action=action, args=args, source=f"{source}:verify")
            allowed, reason = self.policy_guard.authorize(request)
            if not allowed:
                return ActionResult(action=action, status="blocked", error=reason, attempt=attempt)

            confirm_result = await self.registry.execute(request, timeout_s=timeout_s)
            if confirm_result.status == "success":
                return confirm_result
            last_result = confirm_result
            if index < attempts - 1 and delay_s > 0:
                await asyncio.sleep(delay_s)

        return last_result

    @staticmethod
    def _extract_confirm_configs(verify_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        configs: List[Dict[str, Any]] = []
        raw_confirm = verify_cfg.get("confirm")
        if isinstance(raw_confirm, dict):
            configs.append(dict(raw_confirm))
        elif isinstance(raw_confirm, list):
            for item in raw_confirm:
                if isinstance(item, dict):
                    configs.append(dict(item))
        return configs

    @staticmethod
    def _resolve_confirm_policy(verify_cfg: Dict[str, Any], confirm_results: List[ActionResult]) -> Dict[str, Any]:
        raw_policy = verify_cfg.get("confirm_policy")
        policy = raw_policy if isinstance(raw_policy, dict) else {}

        mode = str(policy.get("mode", "all")).strip().lower() or "all"
        if mode not in {"all", "any", "quorum", "majority"}:
            mode = "all"

        required: bool
        if "required" in policy:
            required = bool(policy.get("required"))
        else:
            configs = Executor._extract_confirm_configs(verify_cfg)
            if not configs:
                required = False
            else:
                required = any(bool(item.get("required", True)) for item in configs)

        configs = Executor._extract_confirm_configs(verify_cfg)
        gated_results: List[ActionResult] = []
        if configs and len(confirm_results) == len(configs):
            for idx, row in enumerate(confirm_results):
                cfg = configs[idx]
                if bool(cfg.get("required", True)):
                    gated_results.append(row)
        else:
            gated_results = list(confirm_results)

        total_count = len(gated_results)
        min_success = 0
        if mode == "all":
            min_success = total_count
        elif mode == "any":
            min_success = 1 if total_count > 0 else 0
        elif mode == "majority":
            min_success = max(1, (total_count // 2) + 1) if total_count > 0 else 0
        elif mode == "quorum":
            raw_min = policy.get("min_success", 1)
            try:
                min_success = int(raw_min)
            except Exception:  # noqa: BLE001
                min_success = 1
            min_success = max(1, min(min_success, total_count)) if total_count > 0 else 0

        return {
            "mode": mode,
            "required": required,
            "min_success": min_success,
            "gated_results": gated_results,
        }

    @staticmethod
    def _confirm_policy_satisfied(
        *,
        mode: str,
        success_count: int,
        total_count: int,
        min_success: int,
        required: bool,
    ) -> bool:
        if not required:
            return True
        if total_count <= 0:
            return True
        normalized_mode = str(mode or "").strip().lower() or "all"
        if normalized_mode in {"all", "any", "quorum", "majority"}:
            return int(success_count) >= int(min_success)
        return int(success_count) >= total_count

    def _render_payload(self, payload: Any, *, step: PlanStep, result: ActionResult) -> Any:
        if isinstance(payload, dict):
            return {str(key): self._render_payload(value, step=step, result=result) for key, value in payload.items()}
        if isinstance(payload, list):
            return [self._render_payload(item, step=step, result=result) for item in payload]
        if not isinstance(payload, str):
            return payload

        token_match = re.fullmatch(r"\{\{\s*(args|result)\.([a-zA-Z0-9_.-]+)\s*\}\}", payload)
        if not token_match:
            return payload

        source_kind = token_match.group(1)
        source_path = token_match.group(2)
        source_payload: Dict[str, Any] = step.args if source_kind == "args" else result.output
        resolved = self._resolve_path(source_payload, source_path)
        return resolved if resolved is not None else payload

    @staticmethod
    def _resolve_path(payload: Dict[str, Any], path: str) -> Any:
        value: Any = payload
        for part in path.split("."):
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return None
        return value
