from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from threading import RLock
from typing import Dict, Tuple


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _parse_iso(value: str) -> datetime | None:
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


@dataclass(slots=True)
class CircuitState:
    action: str
    scope: str = ""
    consecutive_failures: int = 0
    opened_count: int = 0
    open_until: str = ""
    last_failure_category: str = ""
    last_error: str = ""
    last_updated_at: str = ""

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


class ActionCircuitBreaker:
    """
    Action-level circuit breaker to avoid repeated failures causing retry storms.
    """

    RETRYABLE_CATEGORIES = {"transient", "timeout", "rate_limited", "unknown"}

    def __init__(
        self,
        *,
        failure_threshold: int = 3,
        cooldown_s: int = 45,
        max_cooldown_s: int = 900,
        max_states: int = 2000,
    ) -> None:
        self.failure_threshold = max(1, min(int(failure_threshold), 20))
        self.cooldown_s = max(5, min(int(cooldown_s), 3600))
        self.max_cooldown_s = max(self.cooldown_s, min(int(max_cooldown_s), 7200))
        self.max_states = max(10, min(int(max_states), 100_000))
        self._states: Dict[str, CircuitState] = {}
        self._lock = RLock()

    def should_block(self, action: str, *, scope: str = "") -> Tuple[bool, str, float]:
        clean_action = str(action or "").strip().lower()
        clean_scope = self._normalize_scope(scope)
        if not clean_action:
            return (False, "", 0.0)
        state_key = self._state_key(clean_action, clean_scope)
        with self._lock:
            state = self._states.get(state_key)
            if state is None or not state.open_until:
                return (False, "", 0.0)
            open_until_dt = _parse_iso(state.open_until)
            if open_until_dt is None:
                return (False, "", 0.0)
            now = _utc_now()
            if now >= open_until_dt:
                state.open_until = ""
                state.last_updated_at = _iso(now)
                self._states[state_key] = state
                return (False, "", 0.0)
            retry_after_s = max(0.0, (open_until_dt - now).total_seconds())
            if clean_scope:
                reason = (
                    f"Circuit breaker is open for action '{clean_action}' scope '{clean_scope}' "
                    f"due to repeated {state.last_failure_category or 'runtime'} failures."
                )
            else:
                reason = (
                    f"Circuit breaker is open for action '{clean_action}' "
                    f"due to repeated {state.last_failure_category or 'runtime'} failures."
                )
            return (True, reason, retry_after_s)

    def record_outcome(
        self,
        *,
        action: str,
        status: str,
        failure_category: str = "",
        error: str = "",
        scope: str = "",
    ) -> Dict[str, object]:
        clean_action = str(action or "").strip().lower()
        clean_scope = self._normalize_scope(scope)
        if not clean_action:
            return {}
        state_key = self._state_key(clean_action, clean_scope)
        clean_status = str(status or "").strip().lower()
        clean_category = str(failure_category or "").strip().lower() or "unknown"
        clean_error = str(error or "").strip()
        now = _utc_now()

        with self._lock:
            state = self._states.get(state_key)
            if state is None:
                state = CircuitState(action=clean_action, scope=clean_scope, last_updated_at=_iso(now))

            if clean_status == "success":
                state.consecutive_failures = 0
                state.open_until = ""
                state.last_failure_category = ""
                state.last_error = ""
                state.last_updated_at = _iso(now)
                self._states[state_key] = state
                self._trim_locked()
                return {"action": clean_action, "state": state.to_dict(), "opened": False}

            if clean_status not in {"failed", "blocked"}:
                state.last_updated_at = _iso(now)
                self._states[state_key] = state
                self._trim_locked()
                return {"action": clean_action, "state": state.to_dict(), "opened": False}

            # Do not open breaker for deliberate policy/approval blocks.
            if clean_category not in self.RETRYABLE_CATEGORIES:
                state.last_failure_category = clean_category
                state.last_error = clean_error
                state.last_updated_at = _iso(now)
                self._states[state_key] = state
                self._trim_locked()
                return {"action": clean_action, "state": state.to_dict(), "opened": False}

            state.consecutive_failures = max(0, int(state.consecutive_failures)) + 1
            state.last_failure_category = clean_category
            state.last_error = clean_error
            state.last_updated_at = _iso(now)

            opened = False
            if state.consecutive_failures >= self.failure_threshold:
                state.opened_count = max(0, int(state.opened_count)) + 1
                backoff = min(self.max_cooldown_s, self.cooldown_s * (2 ** max(0, state.opened_count - 1)))
                state.open_until = _iso(now + timedelta(seconds=backoff))
                state.consecutive_failures = 0
                opened = True

            self._states[state_key] = state
            self._trim_locked()
            return {"action": clean_action, "state": state.to_dict(), "opened": opened}

    def snapshot(
        self,
        *,
        limit: int = 200,
        action: str = "",
        scope: str = "",
    ) -> Dict[str, object]:
        bounded = max(1, min(int(limit), 5000))
        target_action = str(action or "").strip().lower()
        target_scope = self._normalize_scope(scope)
        with self._lock:
            rows = [row.to_dict() for row in self._states.values()]
        if target_action:
            rows = [row for row in rows if str(row.get("action", "")).strip().lower() == target_action]
        if target_scope:
            rows = [row for row in rows if str(row.get("scope", "")).strip().lower() == target_scope]
        rows.sort(key=lambda item: str(item.get("last_updated_at", "")), reverse=True)
        return {"status": "success", "items": rows[:bounded], "count": min(len(rows), bounded), "total": len(rows)}

    def _trim_locked(self) -> None:
        if len(self._states) <= self.max_states:
            return
        rows = sorted(self._states.values(), key=lambda item: str(item.last_updated_at))
        overflow = len(rows) - self.max_states
        for item in rows[:overflow]:
            self._states.pop(self._state_key(item.action, item.scope), None)

    @staticmethod
    def _normalize_scope(scope: object) -> str:
        return str(scope or "").strip().lower()

    @staticmethod
    def _state_key(action: str, scope: str) -> str:
        clean_action = str(action or "").strip().lower()
        clean_scope = str(scope or "").strip().lower()
        if not clean_scope:
            return clean_action
        return f"{clean_action}::{clean_scope}"
