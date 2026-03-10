from __future__ import annotations

from backend.python.core.contracts import ActionResult, PlanStep
from backend.python.core.recovery import RecoveryManager


def test_non_retryable_failure_is_not_retried() -> None:
    manager = RecoveryManager()
    step = PlanStep(step_id="s1", action="write_file", can_retry=True, max_retries=3)
    result = ActionResult(
        action="write_file",
        status="failed",
        error="Missing required args for write_file: path",
    )

    decision = manager.decide(step, result, attempt=1)
    assert decision.retry is False
    assert decision.category == "non_retryable"


def test_rate_limited_failure_uses_adaptive_delay() -> None:
    manager = RecoveryManager()
    step = PlanStep(step_id="s2", action="open_url", can_retry=True, max_retries=4)
    result = ActionResult(
        action="open_url",
        status="failed",
        error="HTTP 429 rate limit exceeded, please try again.",
    )

    decision = manager.decide(step, result, attempt=1)
    assert decision.retry is True
    assert decision.category == "rate_limited"
    assert decision.delay_s >= 1.5


def test_retry_profile_from_verify_caps_delay() -> None:
    manager = RecoveryManager()
    step = PlanStep(
        step_id="s3",
        action="browser_read_dom",
        can_retry=True,
        max_retries=5,
        verify={
            "retry": {
                "base_delay_s": 2.0,
                "max_delay_s": 3.0,
                "multiplier": 3.0,
                "jitter_s": 0.0,
            }
        },
    )
    result = ActionResult(
        action="browser_read_dom",
        status="failed",
        error="Temporary connection reset by peer",
    )

    decision = manager.decide(step, result, attempt=2)
    assert decision.retry is True
    assert decision.delay_s == 3.0
    assert decision.category == "transient"


def test_blocked_status_is_non_retryable() -> None:
    manager = RecoveryManager()
    step = PlanStep(step_id="s4", action="copy_file", can_retry=True, max_retries=3)
    result = ActionResult(action="copy_file", status="blocked", error="Approval required")

    decision = manager.decide(step, result, attempt=1)
    assert decision.retry is False


def test_safe_profile_disables_unknown_failure_retries() -> None:
    manager = RecoveryManager()
    step = PlanStep(step_id="s5", action="custom_action", can_retry=True, max_retries=4)
    result = ActionResult(
        action="custom_action",
        status="failed",
        error="unexpected downstream response signature",
    )

    decision = manager.decide(step, result, attempt=1, recovery_profile="safe")
    assert decision.retry is False
    assert decision.category == "unknown"
    assert decision.profile == "safe"


def test_aggressive_profile_expands_retry_budget() -> None:
    manager = RecoveryManager()
    step = PlanStep(step_id="s6", action="open_url", can_retry=True, max_retries=1)
    result = ActionResult(
        action="open_url",
        status="failed",
        error="temporary service unavailable",
    )

    decision = manager.decide(step, result, attempt=2, recovery_profile="aggressive")
    assert decision.retry is True
    assert decision.category == "transient"
    assert decision.profile == "aggressive"


def test_recovery_profile_listing_and_default_update() -> None:
    manager = RecoveryManager()
    listed = manager.list_profiles()
    assert listed["status"] == "success"
    assert listed["count"] >= 3
    assert listed["default_profile"] in {"safe", "balanced", "aggressive"}

    ok, message, selected = manager.set_default_profile("safe")
    assert ok is True
    assert "updated" in message.lower()
    assert selected == "safe"
    assert manager.list_profiles()["default_profile"] == "safe"

    ok, _message, selected = manager.set_default_profile("does-not-exist")
    assert ok is False
    assert selected == "safe"
