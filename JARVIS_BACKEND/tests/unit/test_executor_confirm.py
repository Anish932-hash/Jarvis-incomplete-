from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from backend.python.core.approval_gate import ApprovalGate
from backend.python.core.circuit_breaker import ActionCircuitBreaker
from backend.python.core.contracts import ActionResult, ExecutionPlan, PlanStep
from backend.python.core.desktop_anchor_memory import DesktopAnchorMemory
from backend.python.core.executor import Executor
from backend.python.core.recovery import RecoveryManager
from backend.python.core.telemetry import Telemetry
from backend.python.core.tool_registry import ToolRegistry
from backend.python.core.verifier import Verifier


class _AllowAllPolicyGuard:
    def authorize(self, _request: Any) -> tuple[bool, str]:
        return (True, "Allowed")


def _build_executor(
    registry: ToolRegistry,
    telemetry: Telemetry | None = None,
    circuit_breaker: ActionCircuitBreaker | None = None,
    desktop_anchor_memory: DesktopAnchorMemory | None = None,
) -> Executor:
    return Executor(
        registry=registry,
        policy_guard=_AllowAllPolicyGuard(),  # type: ignore[arg-type]
        verifier=Verifier(),
        recovery=RecoveryManager(),
        telemetry=telemetry or Telemetry(),
        approval_gate=ApprovalGate(),
        circuit_breaker=circuit_breaker,
        desktop_anchor_memory=desktop_anchor_memory,
    )


def test_render_payload_resolves_args_and_result_tokens() -> None:
    registry = ToolRegistry()
    executor = _build_executor(registry)

    step = PlanStep(
        step_id="step-1",
        action="write_file",
        args={"path": "C:/notes/todo.txt"},
    )
    result = ActionResult(action="write_file", status="success", output={"bytes": 12, "meta": {"path": "C:/notes/todo.txt"}})

    rendered = executor._render_payload(  # noqa: SLF001
        {
            "from_args": "{{ args.path }}",
            "from_result": "{{ result.meta.path }}",
            "literal": "unchanged",
        },
        step=step,
        result=result,
    )

    assert rendered["from_args"] == "C:/notes/todo.txt"
    assert rendered["from_result"] == "C:/notes/todo.txt"
    assert rendered["literal"] == "unchanged"


def test_confirm_check_retries_until_success() -> None:
    registry = ToolRegistry()
    attempts: Dict[str, int] = {"count": 0}

    def flaky_confirm(_args: Dict[str, Any]) -> Dict[str, Any]:
        attempts["count"] += 1
        if attempts["count"] < 2:
            return {"status": "error", "message": "not ready"}
        return {"status": "success", "ok": True}

    registry.register("confirm_probe", flaky_confirm)
    executor = _build_executor(registry)

    step = PlanStep(
        step_id="step-1",
        action="open_app",
        verify={"confirm": {"action": "confirm_probe", "attempts": 2, "delay_s": 0}},
    )
    result = ActionResult(action="open_app", status="success", output={"status": "success"})

    confirm_result = asyncio.run(
        executor._run_confirm_check(  # noqa: SLF001
            step=step,
            result=result,
            source="test",
            attempt=1,
        )
    )

    assert confirm_result is not None
    assert confirm_result.status == "success"
    assert attempts["count"] == 2


def test_confirm_check_returns_last_failure_after_max_attempts() -> None:
    registry = ToolRegistry()
    attempts: Dict[str, int] = {"count": 0}

    def failing_confirm(_args: Dict[str, Any]) -> Dict[str, Any]:
        attempts["count"] += 1
        return {"status": "error", "message": "still failing"}

    registry.register("always_fail_probe", failing_confirm)
    executor = _build_executor(registry)

    step = PlanStep(
        step_id="step-1",
        action="open_app",
        verify={"confirm": {"action": "always_fail_probe", "attempts": 3, "delay_s": 0}},
    )
    result = ActionResult(action="open_app", status="success", output={"status": "success"})

    confirm_result = asyncio.run(
        executor._run_confirm_check(  # noqa: SLF001
            step=step,
            result=result,
            source="test",
            attempt=1,
        )
    )

    assert confirm_result is not None
    assert confirm_result.status == "failed"
    assert attempts["count"] == 3


def test_execute_plan_redirects_tts_to_notification_for_voice_runtime_policy() -> None:
    registry = ToolRegistry()
    executed: Dict[str, Any] = {}

    def speak(_args: Dict[str, Any]) -> Dict[str, Any]:
        executed["tts"] = True
        return {"status": "success"}

    def notify(args: Dict[str, Any]) -> Dict[str, Any]:
        executed["notification"] = dict(args)
        return {"status": "success", "title": args.get("title", ""), "message": args.get("message", "")}

    registry.register("tts_speak", speak)
    registry.register("send_notification", notify)
    executor = _build_executor(registry)

    plan = ExecutionPlan(
        plan_id="plan-voice-runtime-1",
        goal_id="goal-voice-runtime-1",
        intent="voice_followup",
        steps=[
            PlanStep(
                step_id="step-1",
                action="tts_speak",
                args={"text": "Wakeword recovery is active, so I am moving this response into a notification."},
                verify={"optional": True},
            )
        ],
        context={},
    )

    results = asyncio.run(
        executor.execute_plan(
            plan,
            source="voice-session",
            metadata={
                "voice_execution_policy": {
                    "prefer_notification_followup": True,
                    "prefer_non_voice_completion": True,
                },
                "voice_delivery_policy": {
                    "notification_fallback_available": True,
                    "suppress_tts": True,
                    "reason_code": "wakeword_recovery_guard",
                    "reason": "Wakeword route remains in recovery.",
                },
            },
        )
    )

    assert "tts" not in executed
    assert isinstance(executed.get("notification"), dict)
    assert executed["notification"]["title"] == "JARVIS Voice Follow-up"
    assert results and results[0].status == "success"
    redirect = results[0].evidence.get("voice_execution_redirect", {})
    assert isinstance(redirect, dict)
    assert redirect.get("original_action") == "tts_speak"
    assert redirect.get("executed_action") == "send_notification"
    assert redirect.get("reason_code") == "wakeword_recovery_guard"


def test_execute_plan_normalizes_notification_followup_for_voice_runtime_policy() -> None:
    registry = ToolRegistry()
    executed: Dict[str, Any] = {}

    def notify(args: Dict[str, Any]) -> Dict[str, Any]:
        executed["notification"] = dict(args)
        return {"status": "success", "title": args.get("title", ""), "message": args.get("message", "")}

    registry.register("send_notification", notify)
    executor = _build_executor(registry)

    plan = ExecutionPlan(
        plan_id="plan-voice-runtime-2",
        goal_id="goal-voice-runtime-2",
        intent="voice_followup_notification",
        steps=[
            PlanStep(
                step_id="step-1",
                action="send_notification",
                args={
                    "message": (
                        "Wakeword recovery remains active and the spoken follow-up is intentionally being moved into a "
                        "compact notification so the user still gets a stable outcome without entering another fragile "
                        "voice loop."
                    ),
                },
                verify={"expect_status": "success", "expect_key": "title"},
            )
        ],
        context={},
    )

    results = asyncio.run(
        executor.execute_plan(
            plan,
            source="voice-session",
            metadata={
                "voice_execution_policy": {
                    "prefer_notification_followup": True,
                    "prefer_non_voice_completion": True,
                    "confirmation_mode": "explicit",
                    "followup_mode": "notification",
                },
                "voice_delivery_policy": {
                    "notification_fallback_available": True,
                    "mode": "notification_preferred",
                    "reason_code": "wakeword_recovery_notification",
                    "reason": "Wakeword route remains in recovery.",
                },
            },
        )
    )

    assert isinstance(executed.get("notification"), dict)
    assert executed["notification"]["title"] == "JARVIS Voice Confirmation"
    assert len(str(executed["notification"]["message"])) <= 220
    assert results and results[0].status == "success"
    redirect = results[0].evidence.get("voice_execution_redirect", {})
    assert isinstance(redirect, dict)
    assert redirect.get("original_action") == "send_notification"
    assert redirect.get("executed_action") == "send_notification"
    assert redirect.get("reason_code") == "wakeword_recovery_notification"


def test_execute_plan_normalizes_open_url_followup_for_voice_runtime_policy() -> None:
    registry = ToolRegistry()
    executed: Dict[str, Any] = {}

    def open_url(args: Dict[str, Any]) -> Dict[str, Any]:
        executed["open_url"] = dict(args)
        return {"status": "success", "url": args.get("url", "")}

    registry.register("open_url", open_url)
    executor = _build_executor(registry)

    plan = ExecutionPlan(
        plan_id="plan-voice-runtime-3",
        goal_id="goal-voice-runtime-3",
        intent="voice_followup_open_url",
        steps=[
            PlanStep(
                step_id="step-1",
                action="open_url",
                args={
                    "url": "https://example.com/recovery",
                    "title": (
                        "Wakeword recovery guide and alternate follow-up channel for the current voice mission, "
                        "including the operator notes that explain why spoken delivery was intentionally suppressed."
                    ),
                },
                verify={"expect_status": "success", "expect_key": "url"},
            )
        ],
        context={},
    )

    results = asyncio.run(
        executor.execute_plan(
            plan,
            source="voice-session",
            metadata={
                "voice_execution_policy": {
                    "prefer_non_voice_completion": True,
                    "followup_mode": "hybrid",
                    "notification_message_max_chars": 140,
                    "runtime_redirect_action": "open_url",
                    "runtime_redirect_args": {
                        "url": "https://status.example.com/voice/recovery",
                    },
                },
                "voice_delivery_policy": {
                    "suppress_tts": True,
                    "reason_code": "voice_route_recovery_open_url",
                    "reason": "Voice follow-up moved into a browser handoff while wakeword recovery settles.",
                },
            },
        )
    )

    assert isinstance(executed.get("open_url"), dict)
    assert executed["open_url"]["url"] == "https://status.example.com/voice/recovery"
    assert len(str(executed["open_url"]["title"])) <= 140
    assert results and results[0].status == "success"
    redirect = results[0].evidence.get("voice_execution_redirect", {})
    assert isinstance(redirect, dict)
    assert redirect.get("original_action") == "open_url"
    assert redirect.get("executed_action") == "open_url"
    assert redirect.get("reason_code") == "voice_route_recovery_open_url"


def test_execute_plan_redirects_tts_followup_to_clipboard_when_notification_tool_is_unavailable() -> None:
    registry = ToolRegistry()
    executed: Dict[str, Any] = {}

    def clipboard_write(args: Dict[str, Any]) -> Dict[str, Any]:
        executed["clipboard"] = dict(args)
        return {"status": "success", "text": args.get("text", "")}

    registry.register("clipboard_write", clipboard_write)
    executor = _build_executor(registry)

    plan = ExecutionPlan(
        plan_id="plan-voice-runtime-clipboard-1",
        goal_id="goal-voice-runtime-clipboard-1",
        intent="voice_followup_clipboard_fallback",
        steps=[
            PlanStep(
                step_id="step-1",
                action="tts_speak",
                args={
                    "text": (
                        "Wakeword recovery remains active, so this spoken response is being redirected into a more "
                        "stable non-speech follow-up channel."
                    )
                },
                verify={"optional": True},
            )
        ],
        context={},
    )

    results = asyncio.run(
        executor.execute_plan(
            plan,
            source="voice-session",
            metadata={
                "voice_execution_policy": {
                    "prefer_notification_followup": True,
                    "prefer_non_voice_completion": True,
                    "clipboard_text_max_chars": 120,
                    "followup_channel_priority": ["send_notification", "clipboard_write"],
                },
                "voice_delivery_policy": {
                    "notification_fallback_available": True,
                    "suppress_tts": True,
                    "reason_code": "wakeword_recovery_clipboard",
                    "reason": "Wakeword route remains in recovery and clipboard is the first executable fallback.",
                },
            },
        )
    )

    assert "notification" not in executed
    assert isinstance(executed.get("clipboard"), dict)
    assert len(str(executed["clipboard"]["text"])) <= 120
    assert results and results[0].status == "success"
    redirect = results[0].evidence.get("voice_execution_redirect", {})
    assert isinstance(redirect, dict)
    assert redirect.get("original_action") == "tts_speak"
    assert redirect.get("executed_action") == "clipboard_write"
    assert redirect.get("reason_code") == "wakeword_recovery_clipboard"


def test_execute_plan_redirects_generic_recovery_step_using_planner_followup_contract() -> None:
    registry = ToolRegistry()
    executed: Dict[str, Any] = {}

    def open_app(args: Dict[str, Any]) -> Dict[str, Any]:
        executed["open_app"] = dict(args)
        return {"status": "success", "app": args.get("app", "")}

    registry.register("open_app", open_app)
    executor = _build_executor(registry)

    plan = ExecutionPlan(
        plan_id="plan-voice-runtime-contract-1",
        goal_id="goal-voice-runtime-contract-1",
        intent="recovery_handoff_non_voice",
        steps=[
            PlanStep(
                step_id="step-1",
                action="browser_read_dom",
                args={"selector": "#status"},
                verify={"optional": True},
            )
        ],
        context={},
    )

    results = asyncio.run(
        executor.execute_plan(
            plan,
            source="desktop-recovery",
            metadata={
                "voice_recovery_handoff": True,
                "voice_execution_policy": {
                    "prefer_non_voice_completion": True,
                    "planner_followup_contract": {
                        "policy_scope": "voice_recovery_handoff",
                        "recovery_handoff_active": True,
                        "handoff_reason": "Voice recovery handoff prefers reopening the app console.",
                        "selected_followup_action": "open_app",
                        "preferred_followup_action": "open_app",
                        "candidates": [
                            {
                                "action": "open_app",
                                "selection_score": 93,
                                "channel_reason": "open_app_allowed_for_low_risk_reentry",
                                "args": {
                                    "app": "Recovery Console",
                                    "name": "Recovery Console",
                                },
                            }
                        ],
                    },
                },
                "voice_delivery_policy": {
                    "notification_fallback_available": True,
                    "reason_code": "voice_recovery_handoff_contract",
                    "reason": "Planner requested a stable app handoff for recovery.",
                },
            },
        )
    )

    assert "open_app" in executed
    assert executed["open_app"]["app"] == "Recovery Console"
    assert results and results[0].status == "success"
    redirect = results[0].evidence.get("voice_execution_redirect", {})
    assert isinstance(redirect, dict)
    assert redirect.get("original_action") == "browser_read_dom"
    assert redirect.get("executed_action") == "open_app"
    contract = redirect.get("planner_followup_contract", {})
    assert isinstance(contract, dict)
    assert contract.get("selected_followup_action") == "open_app"
    assert contract.get("recovery_handoff_active") is True
    assert contract.get("channel_reason") == "open_app_allowed_for_low_risk_reentry"


def test_execute_plan_prefers_notification_over_open_app_for_high_risk_voice_followup() -> None:
    registry = ToolRegistry()
    executed: Dict[str, Any] = {}

    def notify(args: Dict[str, Any]) -> Dict[str, Any]:
        executed["notification"] = dict(args)
        return {"status": "success", "title": args.get("title", ""), "message": args.get("message", "")}

    def open_app(args: Dict[str, Any]) -> Dict[str, Any]:
        executed["open_app"] = dict(args)
        return {"status": "success", "app": args.get("app", "")}

    registry.register("send_notification", notify)
    registry.register("open_app", open_app)
    executor = _build_executor(registry)

    plan = ExecutionPlan(
        plan_id="plan-voice-runtime-risk-1",
        goal_id="goal-voice-runtime-risk-1",
        intent="voice_followup_high_risk",
        steps=[
            PlanStep(
                step_id="step-1",
                action="tts_speak",
                args={"text": "Open the recovery console."},
                verify={"optional": True},
            )
        ],
        context={},
    )

    results = asyncio.run(
        executor.execute_plan(
            plan,
            source="voice-session",
            metadata={
                "voice_execution_policy": {
                    "prefer_notification_followup": True,
                    "prefer_non_voice_completion": True,
                    "confirmation_mode": "explicit",
                    "mission_risk_level": "high",
                    "followup_channel_priority": ["open_app", "send_notification"],
                    "selected_present_followup_action": "open_app",
                    "runtime_redirect_action": "open_app",
                    "runtime_redirect_args": {
                        "app": "Recovery Console",
                    },
                    "planner_followup_candidates": [
                        {"action": "open_app", "rank": 1, "args": {"app": "Recovery Console"}},
                        {"action": "send_notification", "rank": 2, "args": {"title": "Recovery Console"}},
                    ],
                },
                "voice_delivery_policy": {
                    "notification_fallback_available": True,
                    "suppress_tts": True,
                    "reason_code": "high_risk_voice_followup_redirect",
                    "reason": "High-risk voice missions should prefer notification handoff over app launch.",
                },
            },
        )
    )

    assert "open_app" not in executed
    assert isinstance(executed.get("notification"), dict)
    assert results and results[0].status == "success"
    redirect = results[0].evidence.get("voice_execution_redirect", {})
    assert isinstance(redirect, dict)
    assert redirect.get("executed_action") == "send_notification"
    assert redirect.get("reason_code") == "high_risk_voice_followup_redirect"


def test_execute_plan_uses_voice_recovery_handoff_contract_for_non_voice_source() -> None:
    registry = ToolRegistry()
    executed: Dict[str, Any] = {}

    def notify(args: Dict[str, Any]) -> Dict[str, Any]:
        executed["notification"] = dict(args)
        return {"status": "success", "title": args.get("title", ""), "message": args.get("message", "")}

    registry.register("send_notification", notify)
    executor = _build_executor(registry)

    plan = ExecutionPlan(
        plan_id="plan-voice-recovery-handoff-1",
        goal_id="goal-voice-recovery-handoff-1",
        intent="mission_resume_recovery_handoff",
        steps=[
            PlanStep(
                step_id="step-1",
                action="tts_speak",
                args={"text": "Open the recovery console and continue with the safe operator handoff."},
                verify={"optional": True},
            )
        ],
        context={},
    )

    results = asyncio.run(
        executor.execute_plan(
            plan,
            source="mission_resume",
            metadata={
                "voice_recovery_handoff": True,
                "voice_session_id": "voice-session-44",
                "voice_execution_policy": {
                    "policy_scope": "voice_recovery_handoff",
                    "recovery_handoff_active": True,
                    "prefer_notification_followup": True,
                    "prefer_non_voice_completion": True,
                    "confirmation_mode": "explicit",
                    "mission_risk_level": "high",
                    "planner_followup_contract": {
                        "policy_scope": "voice_recovery_handoff",
                        "recovery_handoff_active": True,
                        "handoff_reason": "Wakeword recovery is unstable, so the executor must keep the follow-up out of speech.",
                        "preferred_followup_action": "send_notification",
                        "selected_followup_action": "send_notification",
                        "candidates": [
                            {
                                "action": "send_notification",
                                "rank": 1,
                                "selection_score": 118,
                                "args": {"title": "Recovery Handoff"},
                            }
                        ],
                    },
                },
                "voice_delivery_policy": {
                    "notification_fallback_available": True,
                    "suppress_tts": True,
                    "reason_code": "voice_recovery_handoff_runtime",
                    "reason": "Recovery handoff should stay out of speech while wakeword policy is guarded.",
                },
            },
        )
    )

    assert isinstance(executed.get("notification"), dict)
    assert "open the recovery console" in str(executed["notification"].get("message", "")).lower()
    assert results and results[0].status == "success"
    redirect = results[0].evidence.get("voice_execution_redirect", {})
    assert isinstance(redirect, dict)
    assert redirect.get("executed_action") == "send_notification"
    assert redirect.get("reason_code") == "voice_recovery_handoff_runtime"


def test_execute_plan_falls_back_from_open_url_to_notification_when_browser_tool_is_unavailable() -> None:
    registry = ToolRegistry()
    executed: Dict[str, Any] = {}

    def notify(args: Dict[str, Any]) -> Dict[str, Any]:
        executed["notification"] = dict(args)
        return {"status": "success", "title": args.get("title", ""), "message": args.get("message", "")}

    registry.register("send_notification", notify)
    executor = _build_executor(registry)

    plan = ExecutionPlan(
        plan_id="plan-voice-runtime-open-url-fallback-1",
        goal_id="goal-voice-runtime-open-url-fallback-1",
        intent="voice_followup_open_url_notification_fallback",
        steps=[
            PlanStep(
                step_id="step-1",
                action="open_url",
                args={
                    "url": "https://example.com/recovery",
                    "title": "Wakeword recovery guide for the current mission",
                    "description": "Open the recovery guide in the browser if that channel is available.",
                },
                verify={"expect_status": "success", "expect_key": "title"},
            )
        ],
        context={},
    )

    results = asyncio.run(
        executor.execute_plan(
            plan,
            source="voice-session",
            metadata={
                "voice_execution_policy": {
                    "prefer_notification_followup": True,
                    "prefer_non_voice_completion": True,
                    "followup_mode": "hybrid",
                    "notification_message_max_chars": 130,
                    "followup_channel_priority": ["open_url", "send_notification"],
                },
                "voice_delivery_policy": {
                    "notification_fallback_available": True,
                    "suppress_tts": True,
                    "reason_code": "voice_browser_handoff_unavailable",
                    "reason": "Browser handoff is unavailable, so the follow-up is redirected to notification.",
                },
            },
        )
    )

    assert "open_url" not in executed
    assert isinstance(executed.get("notification"), dict)
    assert len(str(executed["notification"]["message"])) <= 130
    assert "https://example.com/recovery" in str(executed["notification"]["message"])
    assert results and results[0].status == "success"
    redirect = results[0].evidence.get("voice_execution_redirect", {})
    assert isinstance(redirect, dict)
    assert redirect.get("original_action") == "open_url"
    assert redirect.get("executed_action") == "send_notification"
    assert redirect.get("reason_code") == "voice_browser_handoff_unavailable"


def test_run_step_confirm_policy_any_passes_when_one_probe_succeeds() -> None:
    registry = ToolRegistry()
    registry.register("open_app", lambda _args: {"status": "success"})
    registry.register("confirm_fail", lambda _args: {"status": "error", "message": "not ready"})
    registry.register("confirm_ok", lambda _args: {"status": "success", "ok": True})
    executor = _build_executor(registry)

    step = PlanStep(
        step_id="step-confirm-any",
        action="open_app",
        args={},
        max_retries=0,
        verify={
            "confirm": [
                {"action": "confirm_fail", "attempts": 1, "delay_s": 0},
                {"action": "confirm_ok", "attempts": 1, "delay_s": 0},
            ],
            "confirm_policy": {"mode": "any", "required": True},
        },
    )

    result = asyncio.run(
        executor._run_step(  # noqa: SLF001
            step=step,
            source="test",
            metadata={"external_mutation_simulation_enabled": False},
        )
    )

    assert result.status == "success"
    confirm_actions = result.evidence.get("confirm_actions", [])
    assert isinstance(confirm_actions, list)
    assert len(confirm_actions) == 2


def test_run_step_confirm_policy_all_fails_when_any_probe_fails() -> None:
    registry = ToolRegistry()
    registry.register("open_app", lambda _args: {"status": "success"})
    registry.register("confirm_fail", lambda _args: {"status": "error", "message": "still failing"})
    registry.register("confirm_ok", lambda _args: {"status": "success", "ok": True})
    executor = _build_executor(registry)

    step = PlanStep(
        step_id="step-confirm-all",
        action="open_app",
        args={},
        max_retries=0,
        verify={
            "confirm": [
                {"action": "confirm_fail", "attempts": 1, "delay_s": 0},
                {"action": "confirm_ok", "attempts": 1, "delay_s": 0},
            ],
            "confirm_policy": {"mode": "all", "required": True},
        },
    )

    result = asyncio.run(
        executor._run_step(  # noqa: SLF001
            step=step,
            source="test",
            metadata={"external_mutation_simulation_enabled": False},
        )
    )

    assert result.status == "failed"
    assert "confirm policy failed" in str(result.error or "").lower()


def test_run_step_confirm_policy_ignores_optional_probe() -> None:
    registry = ToolRegistry()
    registry.register("open_app", lambda _args: {"status": "success"})
    registry.register("confirm_ok", lambda _args: {"status": "success", "ok": True})
    registry.register("confirm_optional_fail", lambda _args: {"status": "error", "message": "optional probe failed"})
    executor = _build_executor(registry)

    step = PlanStep(
        step_id="step-confirm-optional",
        action="open_app",
        args={},
        max_retries=0,
        verify={
            "confirm": [
                {"action": "confirm_ok", "attempts": 1, "delay_s": 0, "required": True},
                {"action": "confirm_optional_fail", "attempts": 1, "delay_s": 0, "required": False},
            ],
            "confirm_policy": {"mode": "all", "required": True},
        },
    )

    result = asyncio.run(
        executor._run_step(
            step=step,
            source="test",
            metadata={"external_mutation_simulation_enabled": False},
        )
    )  # noqa: SLF001

    assert result.status == "success"


def test_run_step_records_recovery_history_in_evidence() -> None:
    registry = ToolRegistry()
    attempts: Dict[str, int] = {"count": 0}

    def flaky_action(_args: Dict[str, Any]) -> Dict[str, Any]:
        attempts["count"] += 1
        if attempts["count"] == 1:
            return {"status": "error", "message": "service unavailable"}
        return {"status": "success", "ok": True}

    registry.register("flaky_action", flaky_action)
    executor = _build_executor(registry)

    step = PlanStep(
        step_id="step-retry",
        action="flaky_action",
        args={},
        max_retries=3,
        verify={"expect_status": "success"},
    )

    result = asyncio.run(
        executor._run_step(
            step=step,
            source="test",
            metadata={"external_mutation_simulation_enabled": False},
        )
    )  # noqa: SLF001

    recovery = result.evidence.get("recovery", {})
    history = recovery.get("retry_history", []) if isinstance(recovery, dict) else []

    assert result.status == "success"
    assert result.attempt == 2
    assert isinstance(recovery, dict)
    assert recovery.get("retry_count") == 1
    assert recovery.get("last_category") == "transient"
    assert isinstance(history, list) and len(history) == 1
    assert attempts["count"] == 2


def test_execute_plan_interrupt_check_stops_before_running_steps() -> None:
    registry = ToolRegistry()
    call_counter = {"count": 0}

    def probe(_args: Dict[str, Any]) -> Dict[str, Any]:
        call_counter["count"] += 1
        return {"status": "success"}

    registry.register("probe", probe)
    executor = _build_executor(registry)

    plan = ExecutionPlan(
        plan_id="plan-1",
        goal_id="goal-1",
        intent="test",
        steps=[PlanStep(step_id="s1", action="probe", args={})],
    )

    results = asyncio.run(
        executor.execute_plan(
            plan,
            source="test",
            interrupt_check=lambda: True,
            interrupt_reason="Cancelled from test.",
        )
    )

    assert call_counter["count"] == 0
    assert len(results) == 1
    assert results[0].status == "blocked"
    assert results[0].output.get("interrupted") is True


def test_execute_plan_resolves_step_args_from_dependency_outputs() -> None:
    registry = ToolRegistry()

    registry.register("backup_file", lambda _args: {"status": "success", "backup_path": "C:/tmp/report.bak"})
    registry.register("hash_file", lambda args: {"status": "success", "received_path": str(args.get("path", ""))})
    executor = _build_executor(registry)

    backup_step = PlanStep(step_id="backup-1", action="backup_file", args={"source": "C:/tmp/report.txt"})
    hash_step = PlanStep(
        step_id="hash-2",
        action="hash_file",
        args={"path": "{{steps.backup-1.output.backup_path}}", "algo": "sha256"},
        depends_on=["backup-1"],
    )
    plan = ExecutionPlan(
        plan_id="plan-step-context",
        goal_id="goal-step-context",
        intent="backup_and_hash",
        steps=[backup_step, hash_step],
    )

    results = asyncio.run(executor.execute_plan(plan, source="test"))

    hash_result = next(item for item in results if item.action == "hash_file")
    assert hash_result.status == "success"
    assert hash_result.output.get("received_path") == "C:/tmp/report.bak"


def test_execute_plan_respects_step_dependencies() -> None:
    registry = ToolRegistry()
    order: list[str] = []

    def first(_args: Dict[str, Any]) -> Dict[str, Any]:
        order.append("first")
        return {"status": "success"}

    def second(_args: Dict[str, Any]) -> Dict[str, Any]:
        order.append("second")
        return {"status": "success"}

    registry.register("first", first)
    registry.register("second", second)
    executor = _build_executor(registry)

    step1 = PlanStep(step_id="s1", action="first", args={})
    step2 = PlanStep(step_id="s2", action="second", args={}, depends_on=["s1"])
    plan = ExecutionPlan(
        plan_id="plan-deps",
        goal_id="goal-deps",
        intent="test_deps",
        steps=[step2, step1],  # intentionally out-of-order to verify dependency scheduling
        context={"allow_parallel": True, "max_parallel_steps": 2},
    )

    results = asyncio.run(executor.execute_plan(plan, source="test"))
    assert [item.status for item in results] == ["success", "success"]
    assert order == ["first", "second"]


def test_execute_plan_runs_independent_steps_in_parallel_when_enabled() -> None:
    registry = ToolRegistry()

    async def sleep_a(_args: Dict[str, Any]) -> Dict[str, Any]:
        await asyncio.sleep(0.2)
        return {"status": "success", "name": "a"}

    async def sleep_b(_args: Dict[str, Any]) -> Dict[str, Any]:
        await asyncio.sleep(0.2)
        return {"status": "success", "name": "b"}

    registry.register("sleep_a", sleep_a)
    registry.register("sleep_b", sleep_b)
    executor = _build_executor(registry)

    plan = ExecutionPlan(
        plan_id="plan-parallel",
        goal_id="goal-parallel",
        intent="parallel",
        steps=[
            PlanStep(step_id="s1", action="sleep_a", args={}),
            PlanStep(step_id="s2", action="sleep_b", args={}),
        ],
        context={"allow_parallel": True, "max_parallel_steps": 2},
    )

    started = time.perf_counter()
    results = asyncio.run(executor.execute_plan(plan, source="test"))
    elapsed = time.perf_counter() - started

    assert len(results) == 2
    assert all(item.status == "success" for item in results)
    assert elapsed < 0.35


def test_execute_plan_uses_interrupt_reason_provider() -> None:
    registry = ToolRegistry()
    executor = _build_executor(registry)

    registry.register("probe", lambda _args: {"status": "success"})
    plan = ExecutionPlan(
        plan_id="plan-interrupt-provider",
        goal_id="goal-interrupt-provider",
        intent="interrupt_provider",
        steps=[PlanStep(step_id="s1", action="probe", args={})],
    )

    results = asyncio.run(
        executor.execute_plan(
            plan,
            source="test",
            interrupt_check=lambda: True,
            interrupt_reason="Cancelled by user request.",
            interrupt_reason_provider=lambda: "Goal step budget exceeded (1 steps).",
        )
    )

    assert len(results) == 1
    assert results[0].status == "blocked"
    assert results[0].error == "Goal step budget exceeded (1 steps)."


class _StubDesktopState:
    def latest(self) -> Dict[str, Any]:
        return {"status": "success", "state_hash": "before-hash"}

    def observe(
        self,
        *,
        action: str,
        output: Dict[str, Any],
        goal_id: str = "",
        plan_id: str = "",
        step_id: str = "",
        source: str = "",
    ) -> Dict[str, Any]:
        return {
            "status": "success",
            "action": action,
            "goal_id": goal_id,
            "plan_id": plan_id,
            "step_id": step_id,
            "source": source,
            "state_hash": "after-hash",
            "previous_hash": "before-hash",
            "changed_paths": ["input.mouse.x", "visual.screen_hash"],
        }

    def diff(self, *, from_hash: str = "", to_hash: str = "") -> Dict[str, Any]:
        if from_hash == "before-hash" and to_hash == "after-hash":
            return {
                "status": "success",
                "from_hash": from_hash,
                "to_hash": to_hash,
                "changed_paths": ["input.mouse.x", "visual.screen_hash"],
                "change_count": 2,
            }
        return {"status": "error", "message": "invalid hash pair"}


def test_run_step_uses_desktop_state_in_verification_context() -> None:
    registry = ToolRegistry()
    telemetry = Telemetry()
    desktop_state = _StubDesktopState()
    executor = Executor(
        registry=registry,
        policy_guard=_AllowAllPolicyGuard(),  # type: ignore[arg-type]
        verifier=Verifier(),
        recovery=RecoveryManager(),
        telemetry=telemetry,
        approval_gate=ApprovalGate(),
        desktop_state=desktop_state,
    )

    registry.register("mouse_click", lambda _args: {"status": "success", "x": 12, "y": 24})
    step = PlanStep(
        step_id="desktop-state-step",
        action="mouse_click",
        args={"x": 12, "y": 24},
        verify={
            "checks": [
                {"source": "desktop_state", "type": "desktop_state_changed"},
                {"source": "desktop_state", "type": "changed_path_contains", "value": "input.mouse"},
            ]
        },
    )

    result = asyncio.run(
        executor._run_step(  # noqa: SLF001
            step=step,
            source="test",
            metadata={"__goal_id": "goal-1", "__plan_id": "plan-1"},
        )
    )
    assert result.status == "success"
    desktop_evidence = result.evidence.get("desktop_state", {})
    assert isinstance(desktop_evidence, dict)
    assert desktop_evidence.get("state_hash") == "after-hash"
    assert desktop_evidence.get("state_changed") is True


def test_execute_plan_calls_on_step_result_for_each_step() -> None:
    registry = ToolRegistry()
    executor = _build_executor(registry)
    observed: list[str] = []

    registry.register("first", lambda _args: {"status": "success"})
    registry.register("second", lambda _args: {"status": "success"})
    plan = ExecutionPlan(
        plan_id="plan-callback",
        goal_id="goal-callback",
        intent="callback",
        steps=[
            PlanStep(step_id="s1", action="first", args={}),
            PlanStep(step_id="s2", action="second", args={}),
        ],
    )

    results = asyncio.run(
        executor.execute_plan(
            plan,
            source="test",
            on_step_result=lambda item: observed.append(f"{item.action}:{item.status}"),
        )
    )

    assert len(results) == 2
    assert observed == ["first:success", "second:success"]


def test_execute_plan_calls_on_step_started_for_each_step() -> None:
    registry = ToolRegistry()
    executor = _build_executor(registry)
    started: list[str] = []

    registry.register("first", lambda _args: {"status": "success"})
    registry.register("second", lambda _args: {"status": "success"})
    plan = ExecutionPlan(
        plan_id="plan-started-callback",
        goal_id="goal-started-callback",
        intent="callback",
        steps=[
            PlanStep(step_id="s1", action="first", args={}),
            PlanStep(step_id="s2", action="second", args={}, depends_on=["s1"]),
        ],
    )

    results = asyncio.run(
        executor.execute_plan(
            plan,
            source="test",
            on_step_started=lambda step: started.append(f"{step.step_id}:{step.action}"),
        )
    )

    assert len(results) == 2
    assert started == ["s1:first", "s2:second"]


def test_execute_plan_emits_goal_scoped_step_telemetry() -> None:
    registry = ToolRegistry()
    telemetry = Telemetry(max_events=20)
    executor = _build_executor(registry, telemetry=telemetry)

    registry.register("probe", lambda _args: {"status": "success"})
    plan = ExecutionPlan(
        plan_id="plan-telemetry",
        goal_id="goal-telemetry",
        intent="telemetry",
        steps=[PlanStep(step_id="s1", action="probe", args={})],
    )

    results = asyncio.run(executor.execute_plan(plan, source="test"))

    assert len(results) == 1
    events = telemetry.list_events(limit=10)["items"]
    started = next((item for item in events if item.get("event") == "step.started"), None)
    finished = next((item for item in events if item.get("event") == "step.finished"), None)
    assert started is not None
    assert finished is not None
    assert started["payload"]["goal_id"] == "goal-telemetry"
    assert finished["payload"]["goal_id"] == "goal-telemetry"


def test_circuit_breaker_blocks_after_repeated_retryable_failures() -> None:
    registry = ToolRegistry()
    telemetry = Telemetry(max_events=40)
    breaker = ActionCircuitBreaker(failure_threshold=2, cooldown_s=30, max_cooldown_s=60, max_states=100)

    registry.register("flaky_remote", lambda _args: {"status": "error", "message": "service unavailable"})
    executor = _build_executor(registry, telemetry=telemetry, circuit_breaker=breaker)
    step = PlanStep(
        step_id="step-cb",
        action="flaky_remote",
        args={},
        max_retries=0,
    )

    first = asyncio.run(executor._run_step(step=step, source="test"))  # noqa: SLF001
    second = asyncio.run(executor._run_step(step=step, source="test"))  # noqa: SLF001
    third = asyncio.run(executor._run_step(step=step, source="test"))  # noqa: SLF001

    assert first.status == "failed"
    assert second.status == "failed"
    assert third.status == "blocked"
    assert isinstance(third.output, dict)
    assert isinstance(third.output.get("circuit_breaker"), dict)
    assert bool(third.output["circuit_breaker"].get("open")) is True

    open_events = [
        row
        for row in telemetry.list_events(limit=50).get("items", [])
        if isinstance(row, dict) and str(row.get("event", "")) == "step.circuit_opened"
    ]
    assert len(open_events) >= 1


class _FakeExternalReliability:
    def __init__(self, *, blocked: bool = False, selected_provider: str = "") -> None:
        self.blocked = blocked
        self.selected_provider = str(selected_provider or "").strip().lower()
        self.outcomes: list[dict[str, Any]] = []

    def preflight(self, *, action: str, args: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        if self.blocked:
            return {
                "status": "blocked",
                "message": "provider cooldown active",
                "failure_category": "transient",
                "retry_after_s": 12.0,
                "action": action,
            }
        payload: Dict[str, Any] = {
            "status": "ok",
            "action": action,
            "retry_hint": {"base_delay_s": 1.2, "max_delay_s": 4.0, "multiplier": 1.8},
        }
        if self.selected_provider:
            payload["provider_routing"] = {
                "status": "success",
                "strategy": "healthiest_available",
                "selected_provider": self.selected_provider,
            }
            payload["args_patch"] = {"provider": self.selected_provider}
        return payload

    def record_outcome(
        self,
        *,
        action: str,
        args: Dict[str, Any],
        status: str,
        error: str = "",
        output: Dict[str, Any] | None = None,
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        self.outcomes.append(
            {
                "action": action,
                "status": status,
                "error": error,
                "output": output if isinstance(output, dict) else {},
                "metadata": metadata if isinstance(metadata, dict) else {},
            }
        )
        return {"status": "success", "cooldowns": []}


class _RetryablePreflightReliability:
    def __init__(self) -> None:
        self.preflight_calls = 0
        self.outcomes: list[dict[str, Any]] = []

    def preflight(self, *, action: str, args: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        self.preflight_calls += 1
        if self.preflight_calls == 1:
            return {
                "status": "blocked",
                "action": action,
                "message": "provider cooldown active",
                "failure_category": "transient",
                "retry_after_s": 0.0,
                "retry_hint": {"base_delay_s": 0.0, "max_delay_s": 0.1, "multiplier": 1.0, "jitter_s": 0.0},
            }
        return {
            "status": "ok",
            "action": action,
            "retry_hint": {"base_delay_s": 0.0, "max_delay_s": 0.1, "multiplier": 1.0, "jitter_s": 0.0},
        }

    def record_outcome(
        self,
        *,
        action: str,
        args: Dict[str, Any],
        status: str,
        error: str = "",
        output: Dict[str, Any] | None = None,
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        self.outcomes.append(
            {
                "action": action,
                "status": status,
                "error": error,
                "output": output if isinstance(output, dict) else {},
                "metadata": metadata if isinstance(metadata, dict) else {},
            }
        )
        return {"status": "success", "cooldowns": []}


class _RetryContractPreflightReliability:
    def __init__(self) -> None:
        self.preflight_calls = 0
        self.outcomes: list[dict[str, Any]] = []

    def preflight(self, *, action: str, args: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        del args, metadata
        self.preflight_calls += 1
        return {
            "status": "ok",
            "action": action,
            "retry_hint": {"base_delay_s": 0.4, "max_delay_s": 1.4, "multiplier": 1.3, "jitter_s": 0.05},
            "retry_contract": {
                "version": "1.0",
                "mode": "adaptive_backoff",
                "risk_score": 0.71,
                "timing": {
                    "min_delay_s": 0.35,
                    "base_delay_s": 0.8,
                    "max_delay_s": 3.8,
                    "multiplier": 1.9,
                    "jitter_s": 0.2,
                },
                "budget": {
                    "max_attempts": 2,
                    "suggested_timeout_s": 42,
                    "cooldown_recommendation_s": 30,
                },
            },
        }

    def record_outcome(
        self,
        *,
        action: str,
        args: Dict[str, Any],
        status: str,
        error: str = "",
        output: Dict[str, Any] | None = None,
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        self.outcomes.append(
            {
                "action": action,
                "status": status,
                "error": error,
                "output": output if isinstance(output, dict) else {},
                "metadata": metadata if isinstance(metadata, dict) else {},
            }
        )
        return {"status": "success", "cooldowns": []}


def test_run_step_external_preflight_blocks_when_provider_on_cooldown() -> None:
    registry = ToolRegistry()
    telemetry = Telemetry(max_events=20)
    orchestrator = _FakeExternalReliability(blocked=True)
    executor = Executor(
        registry=registry,
        policy_guard=_AllowAllPolicyGuard(),  # type: ignore[arg-type]
        verifier=Verifier(),
        recovery=RecoveryManager(),
        telemetry=telemetry,
        approval_gate=ApprovalGate(),
        external_reliability=orchestrator,  # type: ignore[arg-type]
    )
    registry.register("external_doc_update", lambda _args: {"status": "success", "document_id": "doc-1"})

    step = PlanStep(
        step_id="external-cooldown-step",
        action="external_doc_update",
        args={"provider": "google", "document_id": "doc-1", "title": "Updated"},
        max_retries=0,
    )

    result = asyncio.run(
        executor._run_step(
            step=step,
            source="test",
            metadata={"external_mutation_simulation_enabled": False},
        )
    )  # noqa: SLF001

    assert result.status == "failed"
    assert "cooldown" in str(result.error or "").lower()
    assert isinstance(result.output, dict)
    assert isinstance(result.output.get("external_reliability"), dict)


def test_run_step_applies_desktop_anchor_before_click_action() -> None:
    registry = ToolRegistry()
    orchestrator = _FakeExternalReliability(blocked=False)
    executor = Executor(
        registry=registry,
        policy_guard=_AllowAllPolicyGuard(),  # type: ignore[arg-type]
        verifier=Verifier(),
        recovery=RecoveryManager(),
        telemetry=Telemetry(max_events=20),
        approval_gate=ApprovalGate(),
        external_reliability=orchestrator,  # type: ignore[arg-type]
    )

    registry.register(
        "accessibility_find_element",
        lambda _args: {
            "status": "success",
            "items": [{"element_id": "uia_btn_send", "match_score": 0.93}],
        },
    )
    registry.register(
        "computer_click_target",
        lambda args: {
            "status": "success",
            "query": str(args.get("query", "")),
            "element_id": str(args.get("element_id", "")),
            "method": "accessibility",
        },
    )

    step = PlanStep(
        step_id="desktop-anchor-step",
        action="computer_click_target",
        args={"query": "Send button"},
        verify={
            "desktop_anchor": {
                "enabled": True,
                "required": True,
                "action": "accessibility_find_element",
                "query": "{{args.query}}",
                "timeout_s": 8,
            },
            "expect_status": "success",
            "expect_key": "element_id",
        },
        max_retries=0,
    )

    result = asyncio.run(executor._run_step(step=step, source="test"))  # noqa: SLF001

    assert result.status == "success"
    assert result.output.get("element_id") == "uia_btn_send"
    anchor = result.evidence.get("desktop_anchor", {})
    assert isinstance(anchor, dict)
    assert anchor.get("status") == "success"
    assert isinstance(anchor.get("args_patch", {}), dict)


def test_run_step_applies_external_preflight_args_patch_before_execution() -> None:
    registry = ToolRegistry()
    orchestrator = _FakeExternalReliability(blocked=False, selected_provider="google")
    executor = Executor(
        registry=registry,
        policy_guard=_AllowAllPolicyGuard(),  # type: ignore[arg-type]
        verifier=Verifier(),
        recovery=RecoveryManager(),
        telemetry=Telemetry(max_events=20),
        approval_gate=ApprovalGate(),
        external_reliability=orchestrator,  # type: ignore[arg-type]
    )
    registry.register("external_task_list", lambda args: {"status": "success", "provider": str(args.get("provider", ""))})

    step = PlanStep(
        step_id="external-route-step",
        action="external_task_list",
        args={"provider": "auto"},
        verify={"expect_status": "success", "expect_key": "provider"},
        max_retries=0,
    )

    result = asyncio.run(executor._run_step(step=step, source="test"))  # noqa: SLF001

    assert result.status == "success"
    assert str(result.output.get("provider", "")) == "google"
    preflight = result.evidence.get("external_reliability_preflight", {})
    assert isinstance(preflight, dict)
    args_patch = preflight.get("applied_args_patch", {})
    assert isinstance(args_patch, dict)
    assert str(args_patch.get("provider", "")) == "google"


def test_run_step_applies_retry_contract_to_retry_policy_and_timeout() -> None:
    registry = ToolRegistry()
    orchestrator = _RetryContractPreflightReliability()
    executor = Executor(
        registry=registry,
        policy_guard=_AllowAllPolicyGuard(),  # type: ignore[arg-type]
        verifier=Verifier(),
        recovery=RecoveryManager(),
        telemetry=Telemetry(max_events=30),
        approval_gate=ApprovalGate(),
        external_reliability=orchestrator,  # type: ignore[arg-type]
    )
    registry.register("external_task_list", lambda _args: {"status": "success", "provider": "google", "items": []})

    step = PlanStep(
        step_id="external-retry-contract-step",
        action="external_task_list",
        args={"provider": "google"},
        max_retries=5,
        timeout_s=15,
        verify={"retry": {"base_delay_s": 0.0, "max_delay_s": 0.1, "multiplier": 1.0, "jitter_s": 0.0}},
    )

    result = asyncio.run(executor._run_step(step=step, source="test"))  # noqa: SLF001

    assert result.status == "success"
    assert step.max_retries == 2
    assert step.timeout_s == 42
    retry_cfg = step.verify.get("retry", {}) if isinstance(step.verify, dict) else {}
    assert isinstance(retry_cfg, dict)
    assert float(retry_cfg.get("base_delay_s", 0.0) or 0.0) >= 0.8
    assert float(retry_cfg.get("max_delay_s", 0.0) or 0.0) >= 3.8
    assert float(retry_cfg.get("multiplier", 0.0) or 0.0) >= 1.9


def test_record_external_outcome_forwards_retry_contract_metadata() -> None:
    registry = ToolRegistry()
    orchestrator = _RetryContractPreflightReliability()
    executor = Executor(
        registry=registry,
        policy_guard=_AllowAllPolicyGuard(),  # type: ignore[arg-type]
        verifier=Verifier(),
        recovery=RecoveryManager(),
        telemetry=Telemetry(max_events=30),
        approval_gate=ApprovalGate(),
        external_reliability=orchestrator,  # type: ignore[arg-type]
    )
    registry.register("external_task_list", lambda _args: {"status": "success", "provider": "google", "items": []})

    step = PlanStep(
        step_id="external-retry-contract-metadata-step",
        action="external_task_list",
        args={"provider": "google"},
        max_retries=0,
    )

    result = asyncio.run(executor._run_step(step=step, source="test"))  # noqa: SLF001

    assert result.status == "success"
    assert orchestrator.outcomes
    metadata = orchestrator.outcomes[0].get("metadata", {})
    assert isinstance(metadata, dict)
    assert str(metadata.get("__external_retry_contract_mode", "")) == "adaptive_backoff"
    assert float(metadata.get("__external_retry_contract_risk", 0.0) or 0.0) >= 0.7
    assert int(metadata.get("__external_retry_contract_max_attempts", 0) or 0) == 2


def test_run_step_external_mutation_simulation_blocks_commit_when_dry_run_fails() -> None:
    registry = ToolRegistry()
    call_count = {"dry_run": 0, "live": 0}

    def _mutation_handler(args: Dict[str, Any]) -> Dict[str, Any]:
        if bool(args.get("dry_run", False)):
            call_count["dry_run"] += 1
            return {"status": "error", "message": "preflight contract mismatch"}
        call_count["live"] += 1
        return {"status": "success", "document_id": "doc-1"}

    registry.register("external_doc_update", _mutation_handler)
    executor = _build_executor(registry)
    step = PlanStep(
        step_id="external-sim-block",
        action="external_doc_update",
        args={"provider": "auto", "document_id": "doc-1", "content": "patch"},
        max_retries=0,
    )

    result = asyncio.run(executor._run_step(step=step, source="test"))  # noqa: SLF001

    assert result.status in {"failed", "blocked"}
    assert "simulation" in str(result.error or "").lower()
    assert int(call_count["dry_run"]) == 1
    assert int(call_count["live"]) == 0
    assert bool(result.evidence.get("external_mutation_simulation_only", False)) is True


def test_run_step_external_mutation_simulation_applies_patch_before_live_execute() -> None:
    registry = ToolRegistry()
    call_count = {"dry_run": 0, "live": 0}

    def _mutation_handler(args: Dict[str, Any]) -> Dict[str, Any]:
        if bool(args.get("dry_run", False)):
            call_count["dry_run"] += 1
            return {
                "status": "success",
                "simulation": {
                    "selected_provider": "google",
                    "recommended_args_patch": {"provider": "google"},
                },
            }
        call_count["live"] += 1
        return {"status": "success", "provider": str(args.get("provider", ""))}

    registry.register("external_doc_update", _mutation_handler)
    executor = _build_executor(registry)
    step = PlanStep(
        step_id="external-sim-patch",
        action="external_doc_update",
        args={"provider": "auto", "document_id": "doc-1", "content": "patch"},
        max_retries=0,
        verify={"expect_status": "success", "expect_key": "provider"},
    )

    result = asyncio.run(executor._run_step(step=step, source="test"))  # noqa: SLF001

    assert result.status == "success"
    assert str(result.output.get("provider", "")) == "google"
    assert int(call_count["dry_run"]) == 1
    assert int(call_count["live"]) == 1
    simulation = result.evidence.get("external_mutation_simulation", {})
    assert isinstance(simulation, dict)
    patch = simulation.get("applied_args_patch", {})
    assert isinstance(patch, dict)
    assert str(patch.get("provider", "")) == "google"


def test_run_step_external_retry_applies_contract_provider_patch_and_succeeds() -> None:
    registry = ToolRegistry()
    call_counter = {"count": 0}

    def _doc_update(args: Dict[str, Any]) -> Dict[str, Any]:
        call_counter["count"] += 1
        provider = str(args.get("provider", "")).strip().lower()
        if provider in {"", "auto"}:
            return {
                "status": "error",
                "message": "provider must be one of: google, graph",
                "contract_diagnostic": {
                    "code": "provider_not_supported_for_action",
                    "allowed_providers": ["google", "graph"],
                    "requested_provider": provider or "auto",
                },
            }
        return {"status": "success", "provider": provider, "document_id": "doc-1"}

    registry.register("external_doc_update", _doc_update)
    executor = _build_executor(registry)
    step = PlanStep(
        step_id="external-repair-contract",
        action="external_doc_update",
        args={"provider": "auto", "document_id": "doc-1", "content": "patch"},
        max_retries=2,
        verify={"expect_status": "success", "expect_key": "provider"},
    )

    result = asyncio.run(
        executor._run_step(
            step=step,
            source="test",
            metadata={"external_mutation_simulation_enabled": False},
        )
    )  # noqa: SLF001

    assert result.status == "success"
    assert int(result.attempt) == 2
    assert int(call_counter["count"]) == 2
    assert str(result.output.get("provider", "")) == "google"
    repair = result.evidence.get("external_repair", {})
    assert isinstance(repair, dict)
    patch = repair.get("applied_patch", {})
    assert isinstance(patch, dict)
    assert str(patch.get("provider", "")) == "google"


def test_run_step_external_retry_applies_remediation_contract_patch() -> None:
    registry = ToolRegistry()
    call_counter = {"count": 0}

    def _doc_update(args: Dict[str, Any]) -> Dict[str, Any]:
        call_counter["count"] += 1
        provider = str(args.get("provider", "")).strip().lower()
        if provider != "graph":
            return {
                "status": "error",
                "message": "connector contract rejected current provider",
                "contract_diagnostic": {
                    "code": "provider_contract_failed",
                    "remediation_hints": [
                        {
                            "id": "switch_provider",
                            "priority": 1,
                            "confidence": 0.93,
                            "args_patch": {"provider": "graph"},
                        }
                    ],
                    "remediation_contract": {
                        "version": "1.0",
                        "strategy_count": 1,
                        "strategies": [
                            {
                                "id": "switch_provider",
                                "type": "args_patch",
                                "priority": 1,
                                "confidence": 0.93,
                                "args_patch": {"provider": "graph"},
                            }
                        ],
                    },
                },
            }
        return {"status": "success", "provider": provider, "document_id": "doc-77"}

    registry.register("external_doc_update", _doc_update)
    executor = _build_executor(registry)
    step = PlanStep(
        step_id="external-repair-remediation",
        action="external_doc_update",
        args={"provider": "auto", "document_id": "doc-77", "content": "hello"},
        max_retries=2,
        verify={"expect_status": "success", "expect_key": "provider"},
    )

    result = asyncio.run(
        executor._run_step(
            step=step,
            source="test",
            metadata={"external_mutation_simulation_enabled": False},
        )
    )  # noqa: SLF001

    assert result.status == "success"
    assert int(result.attempt) == 2
    assert int(call_counter["count"]) == 2
    assert str(result.output.get("provider", "")) == "graph"
    repair = result.evidence.get("external_repair", {})
    assert isinstance(repair, dict)
    patch = repair.get("applied_patch", {})
    assert isinstance(patch, dict)
    assert str(patch.get("provider", "")) == "graph"


def test_run_step_external_retry_uses_runtime_memory_repair_hint_patch() -> None:
    registry = ToolRegistry()
    call_counter = {"count": 0}

    def _task_list(args: Dict[str, Any]) -> Dict[str, Any]:
        call_counter["count"] += 1
        provider = str(args.get("provider", "")).strip().lower()
        if provider == "graph":
            return {"status": "success", "provider": provider, "count": 1, "items": [{"id": "task-1"}]}
        return {"status": "error", "message": "request timed out", "provider": provider}

    registry.register("external_task_list", _task_list)
    executor = _build_executor(registry)
    step = PlanStep(
        step_id="external-repair-memory",
        action="external_task_list",
        args={"provider": "auto", "max_results": 60},
        max_retries=2,
        verify={"expect_status": "success", "expect_key": "provider"},
    )
    metadata = {
        "repair_memory_hints": [
            {
                "memory_id": "mem-1",
                "signals": [
                    {
                        "action": "external_task_list",
                        "status": "success",
                        "score": 1.18,
                        "args": {"provider": "graph", "max_results": 12},
                    }
                ],
            }
        ]
    }

    result = asyncio.run(executor._run_step(step=step, source="test", metadata=metadata))  # noqa: SLF001

    assert result.status == "success"
    assert int(result.attempt) == 2
    assert int(call_counter["count"]) == 2
    assert str(result.output.get("provider", "")) == "graph"
    repair = result.evidence.get("external_repair", {})
    assert isinstance(repair, dict)
    patch = repair.get("applied_patch", {})
    assert isinstance(patch, dict)
    assert str(patch.get("provider", "")) == "graph"


def test_run_step_external_branch_guard_blocks_without_ack_when_enforced(monkeypatch) -> None:
    monkeypatch.setenv("JARVIS_EXTERNAL_BRANCH_GUARD_ENFORCE", "1")
    registry = ToolRegistry()
    registry.register("external_email_send", lambda _args: {"status": "success"})
    executor = _build_executor(registry)
    step = PlanStep(
        step_id="external-branch-guard-blocked",
        action="external_email_send",
        args={"to": ["team@example.com"], "subject": "hello", "body": "body"},
        max_retries=0,
    )

    result = asyncio.run(executor._run_step(step=step, source="test", metadata={}))  # noqa: SLF001

    assert result.status == "blocked"
    branch = result.evidence.get("rollback_branch", {})
    assert isinstance(branch, dict)
    assert str(branch.get("status", "")) == "blocked"
    assert bool(branch.get("enforced", False)) is True


def test_external_reliability_outcome_skips_simulation_only_failures() -> None:
    registry = ToolRegistry()
    orchestrator = _FakeExternalReliability(blocked=False)

    def _mutation_handler(args: Dict[str, Any]) -> Dict[str, Any]:
        if bool(args.get("dry_run", False)):
            return {"status": "error", "message": "dry-run contract failed"}
        return {"status": "success"}

    registry.register("external_doc_update", _mutation_handler)
    executor = Executor(
        registry=registry,
        policy_guard=_AllowAllPolicyGuard(),  # type: ignore[arg-type]
        verifier=Verifier(),
        recovery=RecoveryManager(),
        telemetry=Telemetry(max_events=20),
        approval_gate=ApprovalGate(),
        external_reliability=orchestrator,  # type: ignore[arg-type]
    )
    step = PlanStep(
        step_id="external-sim-outcome-skip",
        action="external_doc_update",
        args={"provider": "auto", "document_id": "doc-1", "content": "patch"},
        max_retries=0,
    )

    result = asyncio.run(executor._run_step(step=step, source="test"))  # noqa: SLF001

    assert result.status in {"failed", "blocked"}
    assert orchestrator.outcomes == []


def test_run_step_records_external_outcome_with_duration_metadata() -> None:
    registry = ToolRegistry()
    orchestrator = _FakeExternalReliability(blocked=False, selected_provider="google")
    executor = Executor(
        registry=registry,
        policy_guard=_AllowAllPolicyGuard(),  # type: ignore[arg-type]
        verifier=Verifier(),
        recovery=RecoveryManager(),
        telemetry=Telemetry(max_events=20),
        approval_gate=ApprovalGate(),
        external_reliability=orchestrator,  # type: ignore[arg-type]
    )
    registry.register("external_task_list", lambda args: {"status": "success", "provider": str(args.get("provider", ""))})

    step = PlanStep(
        step_id="external-outcome-meta-step",
        action="external_task_list",
        args={"provider": "auto"},
        verify={"expect_status": "success"},
        max_retries=0,
    )

    result = asyncio.run(executor._run_step(step=step, source="test"))  # noqa: SLF001

    assert result.status == "success"
    assert orchestrator.outcomes
    recorded = orchestrator.outcomes[-1]
    metadata = recorded.get("metadata", {})
    output = recorded.get("output", {})
    assert isinstance(metadata, dict)
    assert isinstance(output, dict)
    assert int(metadata.get("__result_duration_ms", 0) or 0) >= 0
    assert int(metadata.get("__result_attempt", 0) or 0) >= 1
    assert int(output.get("duration_ms", 0) or 0) >= 0


def test_circuit_breaker_scopes_external_provider_failures() -> None:
    registry = ToolRegistry()
    breaker = ActionCircuitBreaker(failure_threshold=1, cooldown_s=30, max_cooldown_s=60, max_states=100)
    executor = _build_executor(registry, circuit_breaker=breaker, telemetry=Telemetry(max_events=30))

    registry.register("external_doc_update", lambda _args: {"status": "error", "message": "request timed out"})
    graph_step = PlanStep(
        step_id="scope-graph",
        action="external_doc_update",
        args={"provider": "graph", "document_id": "doc-1", "title": "A"},
        max_retries=0,
    )
    google_step = PlanStep(
        step_id="scope-google",
        action="external_doc_update",
        args={"provider": "google", "document_id": "doc-1", "title": "A"},
        max_retries=0,
    )

    first_graph = asyncio.run(executor._run_step(step=graph_step, source="test"))  # noqa: SLF001
    second_graph = asyncio.run(executor._run_step(step=graph_step, source="test"))  # noqa: SLF001
    first_google = asyncio.run(executor._run_step(step=google_step, source="test"))  # noqa: SLF001

    assert first_graph.status == "failed"
    assert second_graph.status == "blocked"
    assert isinstance(second_graph.output.get("circuit_breaker"), dict)
    assert str(second_graph.output["circuit_breaker"].get("scope", "")) == "graph"
    assert first_google.status == "failed"
    assert "circuit breaker is open" not in str(first_google.error or "").lower()


def test_run_step_retries_retryable_external_preflight_blocks() -> None:
    registry = ToolRegistry()
    orchestrator = _RetryablePreflightReliability()
    executor = Executor(
        registry=registry,
        policy_guard=_AllowAllPolicyGuard(),  # type: ignore[arg-type]
        verifier=Verifier(),
        recovery=RecoveryManager(),
        telemetry=Telemetry(max_events=30),
        approval_gate=ApprovalGate(),
        external_reliability=orchestrator,  # type: ignore[arg-type]
    )
    registry.register(
        "external_task_list",
        lambda _args: {"status": "success", "provider": "google", "items": []},
    )
    step = PlanStep(
        step_id="external-preflight-retry",
        action="external_task_list",
        args={"provider": "google"},
        max_retries=2,
        verify={"retry": {"base_delay_s": 0.0, "max_delay_s": 0.1, "multiplier": 1.0, "jitter_s": 0.0}},
    )

    result = asyncio.run(executor._run_step(step=step, source="test"))  # noqa: SLF001

    assert result.status == "success"
    assert result.attempt == 2
    assert orchestrator.preflight_calls >= 2
    assert orchestrator.outcomes
    first_outcome = orchestrator.outcomes[0]
    assert str(first_outcome.get("status", "")).lower() == "failed"


def test_run_step_executes_external_remediation_tool_action_before_retry() -> None:
    registry = ToolRegistry()
    state = {"maintained": False}

    class _AuthRemediationReliability:
        def __init__(self) -> None:
            self.preflight_calls = 0
            self.outcomes: list[dict[str, Any]] = []

        def preflight(self, *, action: str, args: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
            del args, metadata
            self.preflight_calls += 1
            if self.preflight_calls == 1:
                return {
                    "status": "blocked",
                    "action": action,
                    "message": "oauth credentials need refresh",
                    "failure_category": "auth",
                    "retry_after_s": 0.0,
                    "remediation_contract": {
                        "version": "1.0",
                        "strategy_count": 1,
                        "strategies": [
                            {
                                "id": "maintain_oauth",
                                "type": "tool_action",
                                "priority": 1,
                                "confidence": 0.95,
                                "tool_action": {
                                    "action": "oauth_token_maintain",
                                    "args": {"provider": "google", "refresh_window_s": 600},
                                },
                            }
                        ],
                    },
                }
            return {"status": "ok", "action": action}

        def record_outcome(
            self,
            *,
            action: str,
            args: Dict[str, Any],
            status: str,
            error: str = "",
            output: Dict[str, Any] | None = None,
            metadata: Dict[str, Any] | None = None,
        ) -> Dict[str, Any]:
            self.outcomes.append(
                {
                    "action": action,
                    "args": dict(args),
                    "status": status,
                    "error": error,
                    "output": output if isinstance(output, dict) else {},
                    "metadata": metadata if isinstance(metadata, dict) else {},
                }
            )
            return {"status": "success", "cooldowns": []}

    orchestrator = _AuthRemediationReliability()
    executor = Executor(
        registry=registry,
        policy_guard=_AllowAllPolicyGuard(),  # type: ignore[arg-type]
        verifier=Verifier(),
        recovery=RecoveryManager(),
        telemetry=Telemetry(max_events=40),
        approval_gate=ApprovalGate(),
        external_reliability=orchestrator,  # type: ignore[arg-type]
    )

    def _oauth_maintain(_args: Dict[str, Any]) -> Dict[str, Any]:
        state["maintained"] = True
        return {"status": "success", "refreshed_count": 1}

    registry.register("oauth_token_maintain", _oauth_maintain)
    registry.register(
        "external_doc_update",
        lambda _args: {"status": "success", "provider": "google", "document_id": "doc-1"}
        if state["maintained"]
        else {"status": "error", "message": "auth credentials missing"},
    )
    step = PlanStep(
        step_id="external-remediation-tool-action",
        action="external_doc_update",
        args={"provider": "google", "document_id": "doc-1", "content": "x"},
        max_retries=2,
        verify={"retry": {"base_delay_s": 0.0, "max_delay_s": 0.1, "multiplier": 1.0, "jitter_s": 0.0}},
    )

    result = asyncio.run(executor._run_step(step=step, source="test"))  # noqa: SLF001

    assert result.status == "success"
    assert result.attempt == 1
    assert orchestrator.preflight_calls >= 2
    remediation = result.evidence.get("external_remediation", {})
    assert isinstance(remediation, dict)
    actions = remediation.get("actions", [])
    assert isinstance(actions, list)
    assert any(
        isinstance(row, dict)
        and str(row.get("action", "")).strip().lower() == "oauth_token_maintain"
        and str(row.get("status", "")).strip().lower() == "success"
        for row in actions
    )


def test_external_repair_patch_derives_from_structured_remediation_plan() -> None:
    registry = ToolRegistry()
    executor = _build_executor(registry)
    step = PlanStep(
        step_id="external-remediation-plan-patch",
        action="external_doc_update",
        args={"provider": "auto", "document_id": "doc-1", "content": "hello"},
        max_retries=0,
    )
    patch, details = executor._derive_external_repair_patch_from_payload(  # noqa: SLF001
        step=step,
        payload={
            "contract_diagnostic": {
                "diagnostic_id": "diag_patch_001",
                "contract_stage": "provider_contract",
                "code": "provider_not_supported_for_action",
                "remediation_plan": [
                    {
                        "phase": "normalize_args",
                        "confidence": 0.92,
                        "args_patch": {"provider": "graph"},
                    }
                ],
            }
        },
        error="provider must be one of: google, graph",
        metadata={},
    )

    assert patch["provider"] == "graph"
    assert isinstance(details, dict)
    signals = details.get("signals", [])
    assert isinstance(signals, list)
    assert "remediation_plan.normalize_args" in [str(item) for item in signals]
    assert str(details.get("contract_stage", "")) == "provider_contract"
    assert str(details.get("diagnostic_id", "")) == "diag_patch_001"


def test_extract_external_remediation_actions_uses_structured_remediation_plan_tool_action() -> None:
    registry = ToolRegistry()
    executor = _build_executor(registry)
    step = PlanStep(
        step_id="external-remediation-plan-action",
        action="external_doc_update",
        args={"provider": "auto", "document_id": "doc-1", "content": "hello"},
        max_retries=0,
    )
    actions = executor._extract_external_remediation_actions_from_payload(  # noqa: SLF001
        step=step,
        payload={
            "contract_diagnostic": {
                "diagnostic_id": "diag_action_001",
                "contract_stage": "auth_preflight",
                "remediation_plan": [
                    {
                        "phase": "repair_dependency",
                        "confidence": 0.9,
                        "tool_action": {
                            "action": "oauth_token_maintain",
                            "args": {"provider": "google", "refresh_window_s": 900},
                        },
                    }
                ],
            }
        },
    )

    assert isinstance(actions, list)
    assert actions
    assert any(
        isinstance(row, dict)
        and str(row.get("action", "")).strip().lower() == "oauth_token_maintain"
        and str(row.get("source", "")).strip().lower() == "remediation_plan.tool_action"
        and str(row.get("contract_stage", "")).strip().lower() == "auth_preflight"
        and str(row.get("diagnostic_id", "")).strip().lower() == "diag_action_001"
        and str(row.get("plan_phase", "")).strip().lower() == "repair_dependency"
        for row in actions
    )


def test_external_remediation_budget_profile_raises_budget_for_high_contract_risk() -> None:
    registry = ToolRegistry()
    executor = _build_executor(registry)
    profile = executor._external_remediation_budget_profile(  # noqa: SLF001
        metadata={
            "external_remediation_max_actions": 2,
            "external_remediation_max_total_actions": 6,
            "external_remediation_contract_risk_floor": 0.4,
        },
        payloads=[
            {
                "contract_diagnostic": {
                    "code": "auth_preflight_failed",
                    "contract_stage": "auth_preflight",
                    "severity": "error",
                    "checks": [
                        {"check": "auth_credentials", "status": "failed", "severity": "error"},
                        {"check": "token_ttl", "status": "failed", "severity": "error"},
                    ],
                    "remediation_plan": [{"phase": "repair_dependency"}],
                }
            }
        ],
        planned_count=6,
    )

    assert isinstance(profile, dict)
    assert int(profile.get("budget", 0) or 0) >= 3
    assert float(profile.get("contract_risk", 0.0) or 0.0) >= 0.5
    assert str(profile.get("contract_risk_level", "")) in {"high", "critical"}
    contract_codes = profile.get("contract_codes", [])
    assert isinstance(contract_codes, list)
    assert "auth_preflight_failed" in [str(item) for item in contract_codes]


def test_run_external_remediation_actions_honors_manual_execution_contract_mode() -> None:
    registry = ToolRegistry()
    calls = {"maintain": 0}

    def _maintain(_args: Dict[str, Any]) -> Dict[str, Any]:
        calls["maintain"] += 1
        return {"status": "success"}

    registry.register("oauth_token_maintain", _maintain)
    executor = _build_executor(registry)
    step = PlanStep(
        step_id="external-remediation-manual-mode",
        action="external_doc_update",
        args={"provider": "google", "document_id": "doc-1"},
        max_retries=0,
    )

    payload = {
        "contract_diagnostic": {
            "contract_stage": "auth_preflight",
            "remediation_plan": [
                {
                    "phase": "repair_dependency",
                    "confidence": 0.95,
                    "tool_action": {
                        "action": "oauth_token_maintain",
                        "args": {"provider": "google", "refresh_window_s": 600},
                    },
                }
            ],
            "remediation_contract": {
                "automation_tier": "manual",
                "execution_contract": {
                    "mode": "manual",
                    "max_retry_attempts": 1,
                    "stop_conditions": ["manual_escalation"],
                },
            },
        }
    }

    result = asyncio.run(
        executor._run_external_remediation_actions(  # noqa: SLF001
            step=step,
            payloads=[payload],
            metadata={},
            source="test",
            attempt=1,
        )
    )

    assert isinstance(result, dict)
    assert str(result.get("status", "")).strip().lower() == "manual_required"
    assert int(result.get("attempted", 0) or 0) == 0
    assert calls["maintain"] == 0
    profile = result.get("execution_profile", {})
    assert isinstance(profile, dict)
    assert str(profile.get("mode", "")).strip().lower() == "manual"


def test_external_contract_risk_score_includes_execution_contract_signals() -> None:
    registry = ToolRegistry()
    executor = _build_executor(registry)

    payload = {
        "contract_diagnostic": {
            "code": "provider_outage_blocked",
            "severity": "error",
            "severity_score": 0.88,
            "blocking_class": "provider",
            "estimated_recovery_s": 1200,
            "automation_tier": "manual",
            "checks": [
                {"check": "provider_availability", "status": "failed", "severity": "error"},
            ],
            "remediation_contract": {
                "automation_tier": "manual",
                "execution_contract": {
                    "mode": "manual",
                    "max_retry_attempts": 1,
                    "verification": {"allow_provider_reroute": False},
                    "stop_conditions": ["manual_escalation", "checkpoint_failure"],
                },
            },
        }
    }

    risk = executor._external_contract_risk_score(  # noqa: SLF001
        metadata={},
        payloads=[payload],
    )

    assert isinstance(risk, dict)
    assert float(risk.get("risk", 0.0) or 0.0) >= 0.55
    assert str(risk.get("risk_level", "")) in {"high", "critical"}
    execution_modes = risk.get("execution_modes", [])
    assert isinstance(execution_modes, list)
    assert "manual" in [str(item) for item in execution_modes]
    assert bool(risk.get("provider_reroute_locked", False)) is True
    assert int(risk.get("retry_attempt_floor", 0) or 0) == 1
    stop_conditions = risk.get("stop_conditions", [])
    assert isinstance(stop_conditions, list)
    assert "manual_escalation" in [str(item) for item in stop_conditions]


def test_prepare_desktop_anchor_forces_probe_when_memory_viability_requires_it() -> None:
    class _ProbeRequiredMemory:
        def lookup(self, *, action: str, args: Dict[str, Any], metadata: Dict[str, Any], limit: int = 1) -> Dict[str, Any]:
            del args, metadata, limit
            return {
                "status": "success",
                "action": action,
                "items": [
                    {
                        "element_id": "btn_memory",
                        "target_mode": "accessibility",
                        "match_score": 0.95,
                        "viability_policy": "use_with_probe",
                        "risk_score": 0.74,
                        "invalidation_flags": ["transition_profile_high_churn"],
                    }
                ],
            }

    registry = ToolRegistry()
    registry.register(
        "accessibility_find_element",
        lambda _args: {
            "status": "success",
            "items": [{"element_id": "btn_probe", "match_score": 0.9}],
        },
    )
    executor = _build_executor(registry, desktop_anchor_memory=_ProbeRequiredMemory())  # type: ignore[arg-type]
    step = PlanStep(
        step_id="desktop-anchor-memory-probe",
        action="computer_click_target",
        args={"query": "Submit", "target_mode": "auto"},
        max_retries=0,
    )

    payload = asyncio.run(
        executor._prepare_desktop_anchor(  # noqa: SLF001
            step=step,
            source="test",
            metadata={},
            attempt=1,
        )
    )

    assert isinstance(payload, dict)
    evidence = payload.get("evidence", {})
    assert isinstance(evidence, dict)
    assert bool(evidence.get("probe_forced", False)) is True
    chain = evidence.get("chain", [])
    assert isinstance(chain, list)
    assert len(chain) >= 1
    assert str(step.args.get("element_id", "")) == "btn_probe"
    memory_hint = evidence.get("memory_hint", {})
    assert isinstance(memory_hint, dict)
    assert str(memory_hint.get("viability_policy", "")).strip().lower() == "use_with_probe"


def test_desktop_anchor_memory_patches_click_target_args_without_live_anchor_probe(tmp_path) -> None:
    registry = ToolRegistry()
    anchor_memory = DesktopAnchorMemory(store_path=str(tmp_path / "desktop_anchor_memory.json"))
    anchor_memory.record_outcome(
        action="computer_click_target",
        args={"query": "Submit", "target_mode": "accessibility"},
        status="success",
        output={"status": "success", "element_id": "btn_submit", "method": "accessibility"},
        evidence={},
        metadata={},
    )

    registry.register(
        "computer_click_target",
        lambda args: {
            "status": "success",
            "query": str(args.get("query", "")),
            "element_id": str(args.get("element_id", "")),
            "method": str(args.get("target_mode", "auto") or "auto"),
        },
    )
    executor = _build_executor(registry, desktop_anchor_memory=anchor_memory)
    step = PlanStep(
        step_id="memory-anchor-step",
        action="computer_click_target",
        args={"query": "Submit", "target_mode": "auto"},
        verify={"expect_status": "success", "expect_key": "element_id"},
        max_retries=0,
    )

    result = asyncio.run(executor._run_step(step=step, source="test"))  # noqa: SLF001

    assert result.status == "success"
    assert result.output.get("element_id") == "btn_submit"
    anchor = result.evidence.get("desktop_anchor", {})
    assert isinstance(anchor, dict)
    memory_hint = anchor.get("memory_hint", {})
    assert isinstance(memory_hint, dict)
    row = memory_hint.get("row", {})
    assert isinstance(row, dict)
    assert row.get("element_id") == "btn_submit"


def test_desktop_anchor_memory_invalidates_patch_on_state_mismatch(tmp_path) -> None:
    registry = ToolRegistry()
    anchor_memory = DesktopAnchorMemory(store_path=str(tmp_path / "desktop_anchor_memory_invalidated.json"))
    anchor_memory.record_outcome(
        action="computer_click_target",
        args={"query": "Submit", "target_mode": "accessibility"},
        status="success",
        output={"status": "success", "element_id": "btn_submit", "method": "accessibility"},
        evidence={},
        metadata={
            "__desktop_pre_state_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "__desktop_post_state_hash": "dddddddddddddddddddddddddddddddd",
        },
    )

    registry.register(
        "computer_click_target",
        lambda args: {
            "status": "success",
            "query": str(args.get("query", "")),
            "element_id": str(args.get("element_id", "")),
            "method": str(args.get("target_mode", "auto") or "auto"),
        },
    )
    executor = _build_executor(registry, desktop_anchor_memory=anchor_memory)
    step = PlanStep(
        step_id="memory-anchor-invalidated-step",
        action="computer_click_target",
        args={"query": "Submit", "target_mode": "auto"},
        verify={"expect_status": "success"},
        max_retries=0,
    )

    result = asyncio.run(
        executor._run_step(  # noqa: SLF001
            step=step,
            source="test",
            metadata={
                "__desktop_pre_state_hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "__desktop_retry_anchor_state_hash": "cccccccccccccccccccccccccccccccc",
            },
        )
    )

    assert result.status == "success"
    assert str(result.output.get("element_id", "")) == ""
    anchor = result.evidence.get("desktop_anchor", {})
    assert isinstance(anchor, dict)
    memory_hint = anchor.get("memory_hint", {})
    assert isinstance(memory_hint, dict)
    invalidation = memory_hint.get("invalidation", {})
    assert isinstance(invalidation, dict)
    assert bool(invalidation.get("invalidated", False)) is True


def test_desktop_anchor_memory_invalidates_patch_on_guardrail_feedback(tmp_path) -> None:
    registry = ToolRegistry()
    anchor_memory = DesktopAnchorMemory(store_path=str(tmp_path / "desktop_anchor_memory_guardrail.json"))
    anchor_memory.record_outcome(
        action="computer_click_target",
        args={"query": "Submit", "target_mode": "accessibility"},
        status="success",
        output={"status": "success", "element_id": "btn_submit", "method": "accessibility"},
        evidence={},
        metadata={
            "__desktop_pre_state_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "__desktop_post_state_hash": "dddddddddddddddddddddddddddddddd",
        },
    )

    registry.register(
        "computer_click_target",
        lambda args: {
            "status": "success",
            "query": str(args.get("query", "")),
            "element_id": str(args.get("element_id", "")),
            "method": str(args.get("target_mode", "auto") or "auto"),
        },
    )
    executor = _build_executor(registry, desktop_anchor_memory=anchor_memory)
    step = PlanStep(
        step_id="memory-anchor-guardrail-invalidated-step",
        action="computer_click_target",
        args={"query": "Submit", "target_mode": "auto"},
        verify={"expect_status": "success"},
        max_retries=0,
    )
    metadata = {
        "__desktop_pre_state_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "__desktop_guardrail_feedback": [
            {
                "action": "computer_click_target",
                "severity": "hard",
                "reason_tags": ["window_transition", "confirm_policy_failed"],
                "pre_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            }
        ],
    }

    result = asyncio.run(executor._run_step(step=step, source="test", metadata=metadata))  # noqa: SLF001

    assert result.status == "success"
    assert str(result.output.get("element_id", "")) == ""
    anchor = result.evidence.get("desktop_anchor", {})
    assert isinstance(anchor, dict)
    memory_hint = anchor.get("memory_hint", {})
    assert isinstance(memory_hint, dict)
    invalidation = memory_hint.get("invalidation", {})
    assert isinstance(invalidation, dict)
    assert bool(invalidation.get("invalidated", False)) is True
    reasons = invalidation.get("reasons", [])
    assert isinstance(reasons, list)
    assert any(str(reason).startswith("guardrail_") for reason in reasons)
    quarantine = invalidation.get("quarantine", {})
    assert isinstance(quarantine, dict)
    assert str(quarantine.get("status", "")).strip().lower() == "success"


def test_desktop_anchor_memory_invalidates_patch_on_unseen_state_profile(tmp_path) -> None:
    registry = ToolRegistry()
    anchor_memory = DesktopAnchorMemory(store_path=str(tmp_path / "desktop_anchor_memory_state_profile.json"))
    profile_rows = [
        ("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        ("cccccccccccccccccccccccccccccccc", "dddddddddddddddddddddddddddddddd"),
        ("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "11111111111111111111111111111111"),
    ]
    for pre_hash, post_hash in profile_rows:
        anchor_memory.record_outcome(
            action="computer_click_target",
            args={"query": "Submit", "target_mode": "accessibility"},
            status="success",
            output={"status": "success", "element_id": "btn_submit", "method": "accessibility"},
            evidence={},
            metadata={
                "__desktop_pre_state_hash": pre_hash,
                "__desktop_post_state_hash": post_hash,
            },
        )

    registry.register(
        "computer_click_target",
        lambda args: {
            "status": "success",
            "query": str(args.get("query", "")),
            "element_id": str(args.get("element_id", "")),
            "method": str(args.get("target_mode", "auto") or "auto"),
        },
    )
    executor = _build_executor(registry, desktop_anchor_memory=anchor_memory)
    step = PlanStep(
        step_id="memory-anchor-state-profile-invalidated-step",
        action="computer_click_target",
        args={"query": "Submit", "target_mode": "auto"},
        verify={"expect_status": "success"},
        max_retries=0,
    )

    result = asyncio.run(
        executor._run_step(  # noqa: SLF001
            step=step,
            source="test",
            metadata={"__desktop_pre_state_hash": "ffffffffffffffffffffffffffffffff"},
        )
    )

    assert result.status == "success"
    assert str(result.output.get("element_id", "")) == ""
    anchor = result.evidence.get("desktop_anchor", {})
    assert isinstance(anchor, dict)
    memory_hint = anchor.get("memory_hint", {})
    assert isinstance(memory_hint, dict)
    invalidation = memory_hint.get("invalidation", {})
    assert isinstance(invalidation, dict)
    assert bool(invalidation.get("invalidated", False)) is True
    reasons = invalidation.get("reasons", [])
    assert isinstance(reasons, list)
    assert "state_profile_unseen_pre_hash" in [str(item) for item in reasons]
    assert float(invalidation.get("drift_score", 0.0) or 0.0) > 0.0


def test_desktop_anchor_memory_invalidation_flags_stale_anchor_profiles() -> None:
    step = PlanStep(
        step_id="stale-anchor-step",
        action="computer_click_target",
        args={"query": "Submit", "target_mode": "accessibility"},
    )
    stale_updated_at = (datetime.now(timezone.utc) - timedelta(days=2, hours=3)).isoformat()
    invalidation = Executor._evaluate_desktop_anchor_memory_invalidation(  # noqa: SLF001
        row={
            "samples": 2,
            "success_rate": 1.0,
            "consecutive_failures": 0,
            "last_status": "success",
            "state_profile_size": 0,
            "state_profile": {},
            "last_pre_state_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "last_post_state_hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "query": "submit",
            "target_mode": "accessibility",
            "updated_at": stale_updated_at,
        },
        step=step,
        metadata={"__desktop_pre_state_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
        normalized_score=0.9,
    )

    assert isinstance(invalidation, dict)
    assert bool(invalidation.get("invalidated", False)) is True
    reasons = invalidation.get("reasons", [])
    assert isinstance(reasons, list)
    assert "stale_anchor_hard" in [str(item) for item in reasons]
    assert str(invalidation.get("freshness_bucket", "")) == "stale"
    assert float(invalidation.get("row_age_s", 0.0) or 0.0) >= 86_400.0


def test_desktop_anchor_memory_invalidation_flags_transition_volatility() -> None:
    step = PlanStep(
        step_id="volatile-anchor-step",
        action="computer_click_target",
        args={"query": "Submit", "target_mode": "accessibility"},
    )
    feedback_rows = [
        {
            "action": "computer_click_target",
            "reason_tags": ["window_transition", "confirm_policy_failed"],
            "pre_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "state_hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "changed_paths": ["visual.screen_hash", "window.title"],
            "transition_signature": "sig-1",
        },
        {
            "action": "computer_click_target",
            "reason_tags": ["window_transition"],
            "pre_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "state_hash": "cccccccccccccccccccccccccccccccc",
            "changed_paths": ["visual.screen_hash", "window.title", "app"],
            "transition_signature": "sig-2",
        },
        {
            "action": "computer_click_target",
            "reason_tags": ["window_transition", "no_state_change"],
            "pre_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "state_hash": "dddddddddddddddddddddddddddddddd",
            "changed_paths": ["visual.screen_hash", "window.title"],
            "transition_signature": "sig-3",
        },
    ]
    invalidation = Executor._evaluate_desktop_anchor_memory_invalidation(  # noqa: SLF001
        row={
            "samples": 8,
            "success_rate": 0.82,
            "consecutive_failures": 1,
            "last_status": "success",
            "state_profile_size": 2,
            "state_profile": {},
            "last_pre_state_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "last_post_state_hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "query": "submit",
            "target_mode": "accessibility",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
        step=step,
        metadata={
            "__desktop_pre_state_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "__desktop_guardrail_feedback": feedback_rows,
        },
        normalized_score=0.93,
    )

    assert isinstance(invalidation, dict)
    assert bool(invalidation.get("invalidated", False)) is True
    reasons = invalidation.get("reasons", [])
    assert isinstance(reasons, list)
    assert "guardrail_transition_volatility" in [str(item) for item in reasons]
    assert "state_anchor_collision" in [str(item) for item in reasons]
    assert int(invalidation.get("guardrail_transition_volatility", 0) or 0) >= 3
    assert int(invalidation.get("guardrail_pre_anchor_collision_count", 0) or 0) >= 2


def test_desktop_anchor_memory_invalidation_flags_anchor_fallback_instability() -> None:
    step = PlanStep(
        step_id="fallback-instability-step",
        action="computer_click_target",
        args={"query": "Submit", "target_mode": "accessibility"},
    )
    now_iso = datetime.now(timezone.utc).isoformat()
    feedback_rows = [
        {
            "action": "computer_click_target",
            "severity": "hard",
            "reason_tags": ["anchor_fallback_failed", "no_state_change"],
            "pre_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "state_hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "changed_paths": ["visual.screen_hash", "window.title"],
            "transition_signature": "sig-fallback-1",
            "recorded_at": now_iso,
        },
        {
            "action": "computer_click_target",
            "severity": "soft",
            "reason_tags": ["anchor_fallback_failed", "no_state_change"],
            "pre_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "state_hash": "cccccccccccccccccccccccccccccccc",
            "changed_paths": ["visual.screen_hash", "window.title"],
            "transition_signature": "sig-fallback-2",
            "recorded_at": now_iso,
        },
        {
            "action": "computer_click_target",
            "severity": "soft",
            "reason_tags": ["no_state_change", "confirm_check_failed"],
            "pre_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "state_hash": "dddddddddddddddddddddddddddddddd",
            "changed_paths": ["window.title"],
            "transition_signature": "sig-fallback-3",
            "recorded_at": now_iso,
        },
    ]
    invalidation = Executor._evaluate_desktop_anchor_memory_invalidation(  # noqa: SLF001
        row={
            "samples": 9,
            "success_rate": 0.79,
            "consecutive_failures": 1,
            "last_status": "success",
            "state_profile_size": 2,
            "state_profile": {},
            "last_pre_state_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "last_post_state_hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "query": "submit",
            "target_mode": "accessibility",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
        step=step,
        metadata={
            "__desktop_pre_state_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "__desktop_guardrail_feedback": feedback_rows,
        },
        normalized_score=0.94,
    )

    assert isinstance(invalidation, dict)
    assert bool(invalidation.get("invalidated", False)) is True
    reasons = invalidation.get("reasons", [])
    assert isinstance(reasons, list)
    assert "anchor_fallback_instability" in [str(item) for item in reasons]
    assert "guardrail_transition_burst" in [str(item) for item in reasons]
    assert int(invalidation.get("guardrail_fallback_failed_hits", 0) or 0) >= 1
    assert int(invalidation.get("guardrail_recent_failure_hits", 0) or 0) >= 3


def test_desktop_anchor_memory_invalidation_flags_hard_severity_cluster() -> None:
    step = PlanStep(
        step_id="hard-cluster-step",
        action="computer_click_target",
        args={"query": "Submit", "target_mode": "accessibility"},
    )
    now_iso = datetime.now(timezone.utc).isoformat()
    feedback_rows = [
        {
            "action": "computer_click_target",
            "severity": "hard",
            "reason_tags": ["window_transition"],
            "pre_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "state_hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "changed_paths": ["window.title", "app"],
            "transition_signature": "sig-hard-1",
            "recorded_at": now_iso,
        },
        {
            "action": "computer_click_target",
            "severity": "hard",
            "reason_tags": ["app_transition"],
            "pre_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "state_hash": "cccccccccccccccccccccccccccccccc",
            "changed_paths": ["window.title", "app"],
            "transition_signature": "sig-hard-2",
            "recorded_at": now_iso,
        },
    ]
    invalidation = Executor._evaluate_desktop_anchor_memory_invalidation(  # noqa: SLF001
        row={
            "samples": 10,
            "success_rate": 0.87,
            "consecutive_failures": 0,
            "last_status": "success",
            "state_profile_size": 2,
            "state_profile": {},
            "last_pre_state_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "last_post_state_hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "query": "submit",
            "target_mode": "accessibility",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
        step=step,
        metadata={
            "__desktop_pre_state_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "__desktop_guardrail_feedback": feedback_rows,
        },
        normalized_score=0.96,
    )

    assert isinstance(invalidation, dict)
    assert bool(invalidation.get("invalidated", False)) is True
    reasons = invalidation.get("reasons", [])
    assert isinstance(reasons, list)
    assert "guardrail_hard_severity_cluster" in [str(item) for item in reasons]
    assert int(invalidation.get("guardrail_hard_severity_count", 0) or 0) >= 2


def test_desktop_anchor_memory_invalidation_flags_transition_profile_low_success() -> None:
    step = PlanStep(
        step_id="transition-profile-instability-step",
        action="computer_click_target",
        args={"query": "Submit", "target_mode": "accessibility"},
    )
    pre_hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    post_hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    transition_key = f"{pre_hash[:24]}->{post_hash[:24]}"
    invalidation = Executor._evaluate_desktop_anchor_memory_invalidation(  # noqa: SLF001
        row={
            "samples": 12,
            "success_rate": 0.82,
            "consecutive_failures": 1,
            "last_status": "success",
            "state_profile_size": 2,
            "state_profile": {},
            "last_pre_state_hash": pre_hash,
            "last_post_state_hash": post_hash,
            "transition_profile": {
                transition_key: {
                    "samples": 5,
                    "success_rate": 0.2,
                    "guardrail_churn_ema": 0.68,
                    "signature": transition_key,
                }
            },
            "query": "submit",
            "target_mode": "accessibility",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
        step=step,
        metadata={
            "__desktop_pre_state_hash": pre_hash,
            "__desktop_post_state_hash": post_hash,
        },
        normalized_score=0.95,
    )

    assert isinstance(invalidation, dict)
    assert bool(invalidation.get("invalidated", False)) is True
    reasons = invalidation.get("reasons", [])
    assert isinstance(reasons, list)
    reason_set = {str(item) for item in reasons}
    assert "transition_profile_low_success" in reason_set
    assert "transition_profile_high_churn" in reason_set
    assert int(invalidation.get("transition_profile_samples", 0) or 0) >= 5
    assert float(invalidation.get("transition_profile_guardrail_churn_ema", 0.0) or 0.0) >= 0.6


def test_desktop_guardrail_tag_extractor_filters_rows_by_action() -> None:
    tags = Executor._desktop_guardrail_tags_for_action(  # noqa: SLF001
        action="computer_click_target",
        metadata={
            "__desktop_guardrail_feedback": [
                {"action": "computer_click_target", "reason_tags": ["window_transition", "confirm_policy_failed"]},
                {"action": "computer_click_text", "reason_tags": ["no_state_change"]},
            ]
        },
        limit=8,
    )

    assert tags == ["window_transition", "confirm_policy_failed"]


def test_desktop_anchor_invalidation_tag_extractor_filters_rows_by_action() -> None:
    tags = Executor._desktop_anchor_invalidation_tags_for_action(  # noqa: SLF001
        action="computer_click_target",
        metadata={
            "__desktop_anchor_invalidation_feedback": [
                {
                    "action": "computer_click_target",
                    "reasons": ["post_state_anchor_mismatch", "transition_profile_low_success"],
                },
                {
                    "action": "computer_click_text",
                    "reasons": ["query_anchor_mismatch"],
                },
            ]
        },
        limit=8,
    )

    assert tags == ["post_state_anchor_mismatch", "transition_profile_low_success"]


def test_extract_external_remediation_actions_materializes_staggered_retry_schedule() -> None:
    executor = _build_executor(ToolRegistry())
    step = PlanStep(
        step_id="remediation-schedule",
        action="external_doc_update",
        args={"provider": "auto", "document_id": "doc-1"},
    )
    payload = {
        "contract_diagnostic": {
            "contract_stage": "runtime_reliability",
            "diagnostic_id": "diag_schedule_1",
            "remediation_hints": [
                {
                    "id": "staggered_provider_retry",
                    "priority": 2,
                    "confidence": 0.82,
                    "remediation": {
                        "type": "staggered_provider_retry",
                        "schedule": [
                            {"provider": "graph", "delay_s": 5.0},
                            {"provider": "google", "delay_s": 1.2},
                        ],
                    },
                }
            ],
        }
    }

    rows = executor._extract_external_remediation_actions_from_payload(  # noqa: SLF001
        step=step,
        payload=payload,
    )

    assert isinstance(rows, list)
    assert rows
    schedule_rows = [
        row
        for row in rows
        if isinstance(row, dict) and str(row.get("source", "")).strip().lower() == "remediation_hints.retry_schedule"
    ]
    assert schedule_rows
    first = schedule_rows[0]
    assert str(first.get("action", "")).strip().lower() == "external_connector_preflight"
    assert float(first.get("delay_s", 0.0) or 0.0) > 0.0
    assert str(first.get("phase", "")).strip().lower() == "diagnose"


def test_run_step_desktop_anchor_uses_fallback_probe_chain_when_primary_fails() -> None:
    registry = ToolRegistry()
    orchestrator = _FakeExternalReliability(blocked=False)
    executor = Executor(
        registry=registry,
        policy_guard=_AllowAllPolicyGuard(),  # type: ignore[arg-type]
        verifier=Verifier(),
        recovery=RecoveryManager(),
        telemetry=Telemetry(max_events=20),
        approval_gate=ApprovalGate(),
        external_reliability=orchestrator,  # type: ignore[arg-type]
    )

    registry.register("accessibility_find_element", lambda _args: {"status": "error", "message": "no matches"})
    registry.register(
        "computer_find_text_targets",
        lambda _args: {
            "status": "success",
            "targets": [{"center_x": 220, "center_y": 180, "confidence": 0.93}],
        },
    )
    registry.register(
        "computer_click_text",
        lambda args: {"status": "success", "x": int(args.get("x", 0)), "y": int(args.get("y", 0))},
    )

    step = PlanStep(
        step_id="desktop-anchor-fallback-step",
        action="computer_click_text",
        args={"query": "Send"},
        verify={
            "desktop_anchor": {
                "enabled": True,
                "required": True,
                "action": "accessibility_find_element",
                "query": "{{args.query}}",
                "timeout_s": 8,
            },
            "expect_status": "success",
            "expect_key": "x",
        },
        max_retries=0,
    )

    result = asyncio.run(executor._run_step(step=step, source="test"))  # noqa: SLF001

    assert result.status == "success"
    assert int(result.output.get("x", 0)) == 220
    assert int(result.output.get("y", 0)) == 180
    anchor = result.evidence.get("desktop_anchor", {})
    assert isinstance(anchor, dict)
    assert anchor.get("fallback_used") is True
    chain = anchor.get("chain", [])
    assert isinstance(chain, list)
    assert len(chain) >= 2


def test_desktop_transition_contract_marks_probe_bias_and_dual_requirement() -> None:
    executor = _build_executor(ToolRegistry())
    metadata = {
        "__desktop_guardrail_feedback": [
            {
                "action": "computer_click_target",
                "reason_tags": ["window_transition", "anchor_fallback_failed", "confirm_policy_failed"],
                "transition_signature": "sig-a",
                "changed_paths": ["window.title", "visual.screen_hash"],
            },
            {
                "action": "computer_click_target",
                "reason_tags": ["window_transition", "anchor_precondition_failed"],
                "transition_signature": "sig-b",
                "changed_paths": ["window.title", "app", "visual.screen_hash"],
            },
        ],
        "__desktop_anchor_invalidation_feedback": [
            {"action": "computer_click_target", "reasons": ["guardrail_transition_volatility", "window_context_mismatch"]},
        ],
    }

    contract = executor._desktop_transition_contract_for_action(  # noqa: SLF001
        action="computer_click_target",
        metadata=metadata,
        limit=10,
    )

    assert isinstance(contract, dict)
    assert str(contract.get("preferred_probe", "")) == "ocr"
    assert bool(contract.get("require_dual_probe", False)) is True
    assert bool(contract.get("force_probe", False)) is True
    assert float(contract.get("volatility_score", 0.0) or 0.0) > 0.4


def test_prepare_desktop_anchor_enforces_dual_probe_when_transition_contract_is_volatile() -> None:
    registry = ToolRegistry()
    registry.register(
        "accessibility_find_element",
        lambda _args: {"status": "success", "items": [{"element_id": "btn_submit", "match_score": 0.9}]},
    )
    registry.register(
        "computer_find_text_targets",
        lambda _args: {"status": "success", "targets": [{"center_x": 140, "center_y": 96, "confidence": 0.86}]},
    )
    executor = _build_executor(registry)
    step = PlanStep(
        step_id="desktop-anchor-transition-contract",
        action="computer_click_target",
        args={"query": "Submit", "target_mode": "auto"},
        verify={"desktop_anchor": {"enabled": True, "required": True}},
        max_retries=0,
    )
    metadata = {
        "__desktop_guardrail_feedback": [
            {
                "action": "computer_click_target",
                "reason_tags": ["window_transition", "anchor_fallback_failed", "confirm_policy_failed"],
                "transition_signature": "sig-a",
                "changed_paths": ["window.title", "visual.screen_hash"],
            },
            {
                "action": "computer_click_target",
                "reason_tags": ["window_transition", "anchor_precondition_failed"],
                "transition_signature": "sig-b",
                "changed_paths": ["window.title", "app", "visual.screen_hash"],
            },
        ],
        "__desktop_anchor_invalidation_feedback": [
            {"action": "computer_click_target", "reasons": ["guardrail_transition_volatility", "window_context_mismatch"]},
        ],
    }

    payload = asyncio.run(
        executor._prepare_desktop_anchor(  # noqa: SLF001
            step=step,
            source="test",
            metadata=metadata,
            attempt=1,
        )
    )

    evidence = payload.get("evidence", {})
    assert isinstance(evidence, dict)
    assert evidence.get("status") == "success"
    assert bool(evidence.get("dual_probe_required", False)) is True
    assert int(evidence.get("dual_probe_hits", 0) or 0) >= 2
    transition_contract = evidence.get("transition_contract", {})
    assert isinstance(transition_contract, dict)
    assert str(transition_contract.get("preferred_probe", "")) == "ocr"


def test_should_attempt_desktop_fallback_when_window_transitions_on_failure() -> None:
    step = PlanStep(
        step_id="window-shift",
        action="computer_click_target",
        args={"query": "Submit"},
        max_retries=0,
    )
    failed = ActionResult(
        action="computer_click_target",
        status="failed",
        error="click did not activate target",
        evidence={"desktop_state": {"window_transition": True}},
    )

    should = Executor._should_attempt_desktop_fallback(  # noqa: SLF001
        step=step,
        result=failed,
        metadata={},
    )
    assert should is True


def test_run_step_applies_desktop_recovery_patch_for_retry_attempt() -> None:
    registry = ToolRegistry()
    telemetry = Telemetry(max_events=40)
    orchestrator = _FakeExternalReliability(blocked=False)
    executor = Executor(
        registry=registry,
        policy_guard=_AllowAllPolicyGuard(),  # type: ignore[arg-type]
        verifier=Verifier(),
        recovery=RecoveryManager(),
        telemetry=telemetry,
        approval_gate=ApprovalGate(),
        external_reliability=orchestrator,  # type: ignore[arg-type]
    )

    registry.register("computer_observe", lambda _args: {"status": "success", "window_title": "Inbox"})
    registry.register(
        "accessibility_find_element",
        lambda _args: {"status": "success", "items": [{"element_id": "btn_send", "match_score": 0.91}]},
    )
    registry.register(
        "computer_find_text_targets",
        lambda _args: {"status": "success", "targets": [{"center_x": 140, "center_y": 120, "confidence": 0.84}]},
    )

    call_counter = {"count": 0}

    def _click_target(args: Dict[str, Any]) -> Dict[str, Any]:
        call_counter["count"] += 1
        if str(args.get("element_id", "")).strip() == "btn_send":
            return {
                "status": "success",
                "element_id": "btn_send",
                "attempt_count": call_counter["count"],
            }
        return {"status": "error", "message": "target not found"}

    registry.register("computer_click_target", _click_target)

    step = PlanStep(
        step_id="desktop-recovery-retry",
        action="computer_click_target",
        args={"query": "Send"},
        max_retries=2,
        verify={"retry": {"base_delay_s": 0.0, "max_delay_s": 0.1, "multiplier": 1.0, "jitter_s": 0.0}},
    )

    result = asyncio.run(executor._run_step(step=step, source="test"))  # noqa: SLF001

    assert result.status == "success"
    assert result.attempt == 2
    assert result.output.get("element_id") == "btn_send"
    events = telemetry.list_events(limit=50).get("items", [])
    patch_events = [row for row in events if isinstance(row, dict) and str(row.get("event", "")) == "step.desktop_recovery_patch_applied"]
    assert patch_events
