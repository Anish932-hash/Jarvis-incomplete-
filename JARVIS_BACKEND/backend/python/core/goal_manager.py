import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import Event, RLock
from typing import Any, Callable, Dict, List, Optional, Tuple

from .contracts import ActionResult, ExecutionPlan, GoalRecord, GoalRequest, PlanStep, utc_now_iso
from .task_state import GoalStatus, StepStatus


class GoalManager:
    TERMINAL_STATUSES = {
        GoalStatus.COMPLETED,
        GoalStatus.FAILED,
        GoalStatus.BLOCKED,
        GoalStatus.CANCELLED,
    }
    _DEFAULT_SOURCE_PRIORITIES: Dict[str, int] = {
        "desktop-ui": 0,
        "user": 0,
        "voice-session": 0,
        "desktop-voice": 0,
        "desktop-chat": 1,
        "desktop-context": 1,
        "desktop-macro": 1,
        "desktop-mission": 2,
        "desktop-schedule": 4,
        "desktop-trigger": 4,
        "desktop-context-opportunity": 4,
        "desktop-mission-auto": 5,
    }

    def __init__(
        self,
        *,
        store_path: str = "data/goals.json",
        max_records: int = 5000,
    ) -> None:
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._goals: Dict[str, GoalRecord] = {}
        self._cancel_requests: Dict[str, str] = {}
        self._queued_ids: set[str] = set()
        self._queue_order: List[str] = []
        self._terminal_events: Dict[str, Event] = {}
        self._store_path = Path(store_path)
        self._max_records = max(100, min(int(max_records), 100_000))
        self._lock = RLock()
        self._priority_dequeue_enabled = self._env_bool("JARVIS_GOAL_PRIORITY_DEQUEUE_ENABLED", True)
        self._priority_starvation_window_s = self._env_float(
            "JARVIS_GOAL_PRIORITY_STARVATION_WINDOW_S",
            45.0,
            minimum=5.0,
            maximum=3600.0,
        )
        self._default_source_priority = self._env_int(
            "JARVIS_GOAL_DEFAULT_SOURCE_PRIORITY",
            2,
            minimum=-10,
            maximum=20,
        )
        self._source_priorities = self._build_source_priorities(
            os.getenv("JARVIS_GOAL_SOURCE_PRIORITIES", ""),
        )
        self._queue_deadline_enforced = self._env_bool("JARVIS_GOAL_QUEUE_DEADLINE_ENFORCED", True)
        self._default_max_queue_wait_s = self._env_float(
            "JARVIS_GOAL_DEFAULT_MAX_QUEUE_WAIT_S",
            0.0,
            minimum=0.0,
            maximum=86400.0 * 7.0,
        )
        self._source_max_queue_wait_s = self._build_source_max_wait_s(
            os.getenv("JARVIS_GOAL_SOURCE_MAX_QUEUE_WAIT_S", ""),
        )
        self._queue_expiry_reason = str(
            os.getenv("JARVIS_GOAL_QUEUE_EXPIRY_REASON", "Goal expired in pending queue due to deadline policy."),
        ).strip() or "Goal expired in pending queue due to deadline policy."
        self._recovered_queue_count = 0
        self._recovered_running_count = 0
        self._load_locked()

    async def enqueue(self, goal: GoalRecord) -> None:
        should_queue = False
        goal_id = str(goal.goal_id or "").strip()
        if not goal_id:
            return
        with self._lock:
            metadata = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
            if not str(metadata.get("queue_enqueued_at", "")).strip():
                metadata["queue_enqueued_at"] = utc_now_iso()
            source = str(goal.request.source or "").strip().lower()
            if source and "queue_source" not in metadata:
                metadata["queue_source"] = source
            goal.request.metadata = metadata
            self._goals[goal_id] = goal
            event = self._terminal_events.get(goal_id)
            if event is None:
                event = Event()
                self._terminal_events[goal_id] = event
            if goal.status in self.TERMINAL_STATUSES:
                event.set()
            else:
                event.clear()
            if goal.status not in self.TERMINAL_STATUSES and goal_id not in self._queued_ids:
                self._queued_ids.add(goal_id)
                self._queue_order.append(goal_id)
                should_queue = True
            self._persist_locked()
        if should_queue:
            await self._queue.put(goal_id)

    async def dequeue(self, timeout_s: float | None = None) -> Optional[GoalRecord]:
        if timeout_s is None:
            while True:
                goal_id = await self._queue.get()
                goal = self._dequeue_goal_state(goal_id)
                if goal is None:
                    continue
                if goal.status == GoalStatus.CANCELLED:
                    continue
                return goal

        deadline = asyncio.get_running_loop().time() + max(0.01, float(timeout_s))
        try:
            while True:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    return None
                goal_id = await asyncio.wait_for(self._queue.get(), timeout=remaining)
                goal = self._dequeue_goal_state(goal_id)
                if goal is None:
                    continue
                if goal.status == GoalStatus.CANCELLED:
                    continue
                return goal
        except asyncio.TimeoutError:
            return None

    def get(self, goal_id: str) -> Optional[GoalRecord]:
        with self._lock:
            return self._goals.get(goal_id)

    def mark_running(self, goal: GoalRecord) -> None:
        with self._lock:
            goal.status = GoalStatus.RUNNING
            goal.started_at = goal.started_at or goal.request.created_at
            self._goals[goal.goal_id] = goal
            event = self._terminal_events.get(goal.goal_id)
            if event is None:
                event = Event()
                self._terminal_events[goal.goal_id] = event
            event.clear()
            self._persist_locked()

    def mark_completed(self, goal: GoalRecord) -> None:
        with self._lock:
            goal.status = GoalStatus.COMPLETED
            goal.completed_at = goal.completed_at or utc_now_iso()
            self._goals[goal.goal_id] = goal
            event = self._terminal_events.get(goal.goal_id)
            if event is None:
                event = Event()
                self._terminal_events[goal.goal_id] = event
            event.set()
            self._clear_cancel_request_locked(goal.goal_id)
            self._persist_locked()

    def mark_failed(self, goal: GoalRecord, reason: str) -> None:
        with self._lock:
            goal.status = GoalStatus.FAILED
            goal.failure_reason = reason
            goal.completed_at = goal.completed_at or utc_now_iso()
            self._goals[goal.goal_id] = goal
            event = self._terminal_events.get(goal.goal_id)
            if event is None:
                event = Event()
                self._terminal_events[goal.goal_id] = event
            event.set()
            self._clear_cancel_request_locked(goal.goal_id)
            self._persist_locked()

    def mark_cancelled(self, goal: GoalRecord, reason: str = "Cancelled by user request.") -> None:
        with self._lock:
            goal.status = GoalStatus.CANCELLED
            goal.failure_reason = reason
            goal.completed_at = goal.completed_at or utc_now_iso()
            self._goals[goal.goal_id] = goal
            event = self._terminal_events.get(goal.goal_id)
            if event is None:
                event = Event()
                self._terminal_events[goal.goal_id] = event
            event.set()
            self._clear_cancel_request_locked(goal.goal_id)
            self._persist_locked()

    def sync(self, goal: GoalRecord) -> None:
        with self._lock:
            self._goals[goal.goal_id] = goal
            event = self._terminal_events.get(goal.goal_id)
            if event is None:
                event = Event()
                self._terminal_events[goal.goal_id] = event
            if goal.status in self.TERMINAL_STATUSES:
                event.set()
            else:
                event.clear()
            self._persist_locked()

    def wait_for_terminal(self, goal_id: str, timeout_s: float = 10.0) -> Optional[GoalRecord]:
        clean_id = str(goal_id or "").strip()
        if not clean_id:
            return None
        timeout = max(0.01, min(float(timeout_s), 3600.0))
        with self._lock:
            goal = self._goals.get(clean_id)
            if goal is None:
                return None
            if goal.status in self.TERMINAL_STATUSES:
                return goal
            event = self._terminal_events.get(clean_id)
            if event is None:
                event = Event()
                self._terminal_events[clean_id] = event
        event.wait(timeout=timeout)
        with self._lock:
            return self._goals.get(clean_id)

    def promote(self, goal_id: str, *, temporary_priority: int = -3, reason: str = "manual") -> bool:
        return self.reprioritize(
            goal_id,
            priority=temporary_priority,
            reason=reason,
            move_front=True,
            stronger_only=True,
        )

    def reprioritize(
        self,
        goal_id: str,
        *,
        priority: int,
        reason: str = "manual",
        move_front: bool = False,
        stronger_only: bool = False,
    ) -> bool:
        clean_id = str(goal_id or "").strip()
        if not clean_id:
            return False
        with self._lock:
            goal = self._goals.get(clean_id)
            if goal is None:
                return False
            if goal.status in self.TERMINAL_STATUSES:
                return False
            if clean_id not in self._queued_ids:
                return False
            current_position = -1
            try:
                current_position = self._queue_order.index(clean_id)
            except ValueError:
                return False
            metadata = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
            existing = self._coerce_int_or_none(metadata.get("queue_priority"))
            bounded_priority = max(-20, min(int(priority), 20))
            if stronger_only and existing is not None and bounded_priority >= existing:
                return False
            metadata["queue_priority"] = bounded_priority
            metadata["queue_priority_updated_at"] = utc_now_iso()
            metadata["queue_priority_reason"] = str(reason or "manual").strip() or "manual"
            if move_front:
                try:
                    self._queue_order.pop(current_position)
                    self._queue_order.insert(0, clean_id)
                    metadata["queue_promoted_at"] = utc_now_iso()
                except Exception:
                    pass
            goal.request.metadata = metadata
            self._goals[clean_id] = goal
            self._persist_locked()
            return True

    def request_cancel(self, goal_id: str, reason: str = "Cancelled by user request.") -> Tuple[bool, str, Optional[GoalRecord]]:
        with self._lock:
            goal = self._goals.get(goal_id)
            if not goal:
                return (False, "Goal not found.", None)

            if goal.status in self.TERMINAL_STATUSES:
                return (False, f"Goal already {goal.status.value}.", goal)

            clean_reason = str(reason or "").strip() or "Cancelled by user request."
            if goal.status == GoalStatus.PENDING:
                goal.status = GoalStatus.CANCELLED
                goal.failure_reason = clean_reason
                goal.completed_at = goal.completed_at or utc_now_iso()
                self._goals[goal.goal_id] = goal
                event = self._terminal_events.get(goal.goal_id)
                if event is None:
                    event = Event()
                    self._terminal_events[goal.goal_id] = event
                event.set()
                self._clear_cancel_request_locked(goal.goal_id)
                self._persist_locked()
                return (True, "Pending goal cancelled.", goal)

            self._cancel_requests[goal_id] = clean_reason
            self._persist_locked()
            return (True, "Cancellation requested; goal will stop at the next safe checkpoint.", goal)

    def is_cancel_requested(self, goal_id: str) -> bool:
        with self._lock:
            return goal_id in self._cancel_requests

    def cancel_reason(self, goal_id: str) -> str:
        with self._lock:
            reason = self._cancel_requests.get(goal_id, "")
        return reason.strip() or "Cancelled by user request."

    def clear_cancel_request(self, goal_id: str) -> None:
        with self._lock:
            self._clear_cancel_request_locked(goal_id)
            self._persist_locked()

    def all_goals(self) -> Dict[str, GoalRecord]:
        with self._lock:
            return dict(self._goals)

    def queue_snapshot(
        self,
        *,
        limit: int = 200,
        include_terminal: bool = False,
        status: str = "",
        source: str = "",
        mission_id: str = "",
        mission_lookup: Optional[Callable[[str], str]] = None,
    ) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 5000))
        normalized_status = str(status or "").strip().lower()
        normalized_source = str(source or "").strip().lower()
        normalized_mission = str(mission_id or "").strip()

        with self._lock:
            now_ts = time.time()
            rows: List[Dict[str, Any]] = []
            queue_id_set = {str(item or "").strip() for item in self._queue_order if str(item or "").strip()}
            for queue_index, goal_id in enumerate(self._queue_order):
                clean_goal_id = str(goal_id or "").strip()
                if not clean_goal_id:
                    continue
                goal = self._goals.get(clean_goal_id)
                if goal is None:
                    continue
                row = self._queue_row_locked(
                    goal=goal,
                    queue_index=queue_index,
                    now_ts=now_ts,
                    mission_lookup=mission_lookup,
                )
                row_status = str(row.get("status", "")).strip().lower()
                if not include_terminal and row_status in {status.value for status in self.TERMINAL_STATUSES}:
                    continue
                if normalized_status and row_status != normalized_status:
                    continue
                row_source = str(row.get("source", "")).strip().lower()
                if normalized_source and row_source != normalized_source:
                    continue
                row_mission_id = str(row.get("mission_id", "")).strip()
                if normalized_mission and row_mission_id != normalized_mission:
                    continue
                rows.append(row)

            if include_terminal:
                for goal in self._goals.values():
                    clean_goal_id = str(goal.goal_id or "").strip()
                    if not clean_goal_id or clean_goal_id in queue_id_set:
                        continue
                    row = self._queue_row_locked(
                        goal=goal,
                        queue_index=-1,
                        now_ts=now_ts,
                        mission_lookup=mission_lookup,
                    )
                    row_status = str(row.get("status", "")).strip().lower()
                    if normalized_status and row_status != normalized_status:
                        continue
                    row_source = str(row.get("source", "")).strip().lower()
                    if normalized_source and row_source != normalized_source:
                        continue
                    row_mission_id = str(row.get("mission_id", "")).strip()
                    if normalized_mission and row_mission_id != normalized_mission:
                        continue
                    rows.append(row)

            rows.sort(
                key=lambda row: (
                    int(row.get("effective_priority", 99)),
                    int(row.get("queue_index", 999999)),
                    str(row.get("created_at", "")),
                    str(row.get("goal_id", "")),
                ),
            )
            selected = rows[:bounded]
            status_counts: Dict[str, int] = {}
            source_counts: Dict[str, int] = {}
            for row in selected:
                status_key = str(row.get("status", "unknown")).strip().lower() or "unknown"
                source_key = str(row.get("source", "unknown")).strip().lower() or "unknown"
                status_counts[status_key] = int(status_counts.get(status_key, 0)) + 1
                source_counts[source_key] = int(source_counts.get(source_key, 0)) + 1

            orphaned_pending = 0
            for goal in self._goals.values():
                if goal.status in self.TERMINAL_STATUSES:
                    continue
                if str(goal.goal_id or "").strip() not in queue_id_set:
                    orphaned_pending += 1

            return {
                "status": "success",
                "items": selected,
                "count": len(selected),
                "total": len(rows),
                "queue_length": len(self._queue_order),
                "orphaned_pending_count": int(orphaned_pending),
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
                "updated_at": utc_now_iso(),
            }

    def recovery_summary(self) -> Dict[str, int]:
        with self._lock:
            return {
                "requeued_count": int(self._recovered_queue_count),
                "recovered_running_count": int(self._recovered_running_count),
                "goal_count": len(self._goals),
            }

    def _dequeue_goal_state(self, goal_id: str) -> Optional[GoalRecord]:
        with self._lock:
            selected_id = self._select_goal_for_dequeue_locked(hinted_goal_id=goal_id)
            if not selected_id:
                return None
            self._queued_ids.discard(selected_id)
            try:
                self._queue_order.remove(selected_id)
            except ValueError:
                pass
            goal = self._goals.get(selected_id)
            self._persist_locked()
            return goal

    def _select_goal_for_dequeue_locked(self, *, hinted_goal_id: str) -> str:
        clean_hint = str(hinted_goal_id or "").strip()
        if not self._queue_order:
            return ""

        stale_ids: List[str] = []
        if not self._priority_dequeue_enabled:
            if clean_hint and clean_hint in self._queue_order:
                return clean_hint
            return str(self._queue_order[0] or "").strip()

        now_ts = time.time()
        now_iso = utc_now_iso()
        best_goal_id = ""
        best_score: tuple[int, str, int] | None = None
        for idx, queued_goal_id in enumerate(self._queue_order):
            clean_id = str(queued_goal_id or "").strip()
            if not clean_id:
                continue
            goal = self._goals.get(clean_id)
            if goal is None:
                stale_ids.append(clean_id)
                continue
            if goal.status in self.TERMINAL_STATUSES:
                stale_ids.append(clean_id)
                continue
            if self._queue_deadline_enforced:
                expired, expiry_reason = self._is_goal_queue_expired(goal=goal, now_ts=now_ts)
                if expired:
                    self._expire_goal_locked(goal=goal, reason=expiry_reason, completed_at=now_iso)
                    stale_ids.append(clean_id)
                    continue
            score = self._goal_dequeue_score(goal=goal, queue_index=idx, now_ts=now_ts)
            if best_score is None or score < best_score:
                best_score = score
                best_goal_id = clean_id

        if stale_ids:
            stale_set = set(stale_ids)
            self._queue_order = [row for row in self._queue_order if str(row or "").strip() not in stale_set]
            for stale_id in stale_set:
                self._queued_ids.discard(stale_id)

        if best_goal_id:
            return best_goal_id
        if clean_hint and clean_hint in self._queue_order:
            return clean_hint
        if self._queue_order:
            return str(self._queue_order[0] or "").strip()
        return ""

    def _goal_dequeue_score(self, *, goal: GoalRecord, queue_index: int, now_ts: float) -> tuple[int, str, int]:
        base_priority = self._resolve_goal_queue_priority(goal)
        created_at = str(goal.request.created_at or "").strip()
        waited_s = self._goal_waited_seconds(goal=goal, now_ts=now_ts)
        starvation_windows = int(waited_s // self._priority_starvation_window_s)
        effective_priority = max(-20, base_priority - starvation_windows)
        return (effective_priority, created_at, int(queue_index))

    def _queue_row_locked(
        self,
        *,
        goal: GoalRecord,
        queue_index: int,
        now_ts: float,
        mission_lookup: Optional[Callable[[str], str]],
    ) -> Dict[str, Any]:
        metadata = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
        mission = ""
        if callable(mission_lookup):
            try:
                mission = str(mission_lookup(goal.goal_id) or "").strip()
            except Exception:
                mission = ""
        if not mission:
            mission = str(metadata.get("__jarvis_mission_id", "")).strip()
        base_priority = self._resolve_goal_queue_priority(goal)
        waited_s = self._goal_waited_seconds(goal=goal, now_ts=now_ts)
        starvation_windows = int(waited_s // self._priority_starvation_window_s)
        effective_priority = max(-20, base_priority - starvation_windows)
        has_deadline, deadline_epoch, deadline_reason = self._resolve_goal_queue_deadline(goal=goal, now_ts=now_ts)
        remaining_s: Optional[float] = None
        if has_deadline:
            remaining_s = max(0.0, float(deadline_epoch - now_ts))
        return {
            "goal_id": goal.goal_id,
            "mission_id": mission,
            "status": goal.status.value if hasattr(goal.status, "value") else str(goal.status),
            "source": str(goal.request.source or "").strip(),
            "text": str(goal.request.text or ""),
            "queue_index": int(queue_index),
            "created_at": str(goal.request.created_at or ""),
            "queue_enqueued_at": str(metadata.get("queue_enqueued_at", "") or ""),
            "started_at": str(goal.started_at or ""),
            "completed_at": str(goal.completed_at or ""),
            "waited_s": round(max(0.0, waited_s), 3),
            "base_priority": int(base_priority),
            "effective_priority": int(effective_priority),
            "starvation_windows": int(starvation_windows),
            "queue_priority": self._coerce_int_or_none(metadata.get("queue_priority")),
            "queue_priority_reason": str(metadata.get("queue_priority_reason", "") or ""),
            "queue_promoted_at": str(metadata.get("queue_promoted_at", "") or ""),
            "queue_promoted_reason": str(metadata.get("queue_promoted_reason", "") or ""),
            "queue_deadline_enforced": bool(self._queue_deadline_enforced),
            "queue_deadline_at": (
                datetime.fromtimestamp(deadline_epoch, tz=timezone.utc).isoformat()
                if has_deadline
                else ""
            ),
            "queue_deadline_remaining_s": round(remaining_s, 3) if isinstance(remaining_s, float) else None,
            "queue_deadline_reason": deadline_reason,
            "failure_reason": str(goal.failure_reason or ""),
        }

    def _goal_waited_seconds(self, *, goal: GoalRecord, now_ts: float) -> float:
        metadata = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
        enqueued_at = str(metadata.get("queue_enqueued_at", "")).strip()
        start_epoch = self._parse_iso_epoch(enqueued_at, default=-1.0)
        if start_epoch <= 0:
            created_at = str(goal.request.created_at or "").strip()
            start_epoch = self._parse_iso_epoch(created_at, default=now_ts)
        return max(0.0, float(now_ts - start_epoch))

    def _resolve_goal_queue_deadline(self, *, goal: GoalRecord, now_ts: float) -> tuple[bool, float, str]:
        metadata = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
        raw_deadline = str(metadata.get("queue_deadline_at", "")).strip()
        if raw_deadline:
            deadline_epoch = self._parse_iso_epoch(raw_deadline, default=now_ts)
            return True, float(deadline_epoch), "metadata.queue_deadline_at"

        max_wait = self._coerce_float_or_none(metadata.get("queue_max_wait_s"))
        source_name = str(goal.request.source or "").strip().lower()
        if max_wait is None and source_name:
            for prefix, source_wait in self._source_max_queue_wait_s.items():
                if source_name == prefix or source_name.startswith(f"{prefix}:") or source_name.startswith(f"{prefix}-"):
                    max_wait = float(source_wait)
                    break
        if max_wait is None:
            max_wait = float(self._default_max_queue_wait_s)
        max_wait = max(0.0, min(float(max_wait), 86400.0 * 14.0))
        if max_wait <= 0.0:
            return False, 0.0, ""
        waited = self._goal_waited_seconds(goal=goal, now_ts=now_ts)
        deadline_epoch = float(now_ts + (max_wait - waited))
        return True, deadline_epoch, "max_wait_policy"

    def _is_goal_queue_expired(self, *, goal: GoalRecord, now_ts: float) -> tuple[bool, str]:
        has_deadline, deadline_epoch, deadline_reason = self._resolve_goal_queue_deadline(goal=goal, now_ts=now_ts)
        if not has_deadline:
            return False, ""
        if float(deadline_epoch) > float(now_ts):
            return False, ""
        return True, deadline_reason

    def _expire_goal_locked(self, *, goal: GoalRecord, reason: str, completed_at: str) -> None:
        goal.status = GoalStatus.CANCELLED
        suffix = f" [{reason}]" if str(reason or "").strip() else ""
        goal.failure_reason = f"{self._queue_expiry_reason}{suffix}".strip()
        goal.completed_at = str(completed_at or utc_now_iso())
        metadata = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
        metadata["queue_expired"] = True
        metadata["queue_expired_at"] = goal.completed_at
        metadata["queue_expired_reason"] = str(reason or "").strip()
        goal.request.metadata = metadata
        self._goals[goal.goal_id] = goal
        event = self._terminal_events.get(goal.goal_id)
        if event is None:
            event = Event()
            self._terminal_events[goal.goal_id] = event
        event.set()
        self._clear_cancel_request_locked(goal.goal_id)

    def _resolve_goal_queue_priority(self, goal: GoalRecord) -> int:
        metadata = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
        explicit = self._coerce_int_or_none(metadata.get("queue_priority"))
        if explicit is not None:
            return max(-20, min(explicit, 20))
        source = str(goal.request.source or "").strip().lower()
        if source:
            for prefix, priority in self._source_priorities.items():
                if source == prefix or source.startswith(f"{prefix}:") or source.startswith(f"{prefix}-"):
                    return priority
        return int(self._default_source_priority)

    def _build_source_priorities(self, raw: str) -> Dict[str, int]:
        priorities = dict(self._DEFAULT_SOURCE_PRIORITIES)
        text = str(raw or "").strip()
        if not text:
            return priorities
        for token in text.split(","):
            part = str(token or "").strip()
            if not part or ":" not in part:
                continue
            name, value = part.split(":", 1)
            clean_name = str(name or "").strip().lower()
            if not clean_name:
                continue
            parsed = self._coerce_int_or_none(value)
            if parsed is None:
                continue
            priorities[clean_name] = max(-20, min(parsed, 20))
        # Prefer longest prefix first when matching.
        ordered = sorted(priorities.items(), key=lambda row: len(row[0]), reverse=True)
        return {key: int(value) for key, value in ordered}

    def _build_source_max_wait_s(self, raw: str) -> Dict[str, float]:
        mapping: Dict[str, float] = {}
        text = str(raw or "").strip()
        if not text:
            return mapping
        for token in text.split(","):
            part = str(token or "").strip()
            if not part or ":" not in part:
                continue
            name, value = part.split(":", 1)
            clean_name = str(name or "").strip().lower()
            if not clean_name:
                continue
            parsed = self._coerce_float_or_none(value)
            if parsed is None:
                continue
            mapping[clean_name] = max(0.0, min(float(parsed), 86400.0 * 14.0))
        ordered = sorted(mapping.items(), key=lambda row: len(row[0]), reverse=True)
        return {key: float(value) for key, value in ordered}

    @staticmethod
    def _coerce_int_or_none(raw: Any) -> Optional[int]:
        if raw is None:
            return None
        text = str(raw).strip()
        if not text:
            return None
        try:
            return int(text)
        except Exception:
            return None

    @staticmethod
    def _coerce_float_or_none(raw: Any) -> Optional[float]:
        if raw is None:
            return None
        text = str(raw).strip()
        if not text:
            return None
        try:
            return float(text)
        except Exception:
            return None

    @staticmethod
    def _parse_iso_epoch(raw: str, *, default: float) -> float:
        clean = str(raw or "").strip()
        if not clean:
            return float(default)
        try:
            return datetime.fromisoformat(clean).astimezone(timezone.utc).timestamp()
        except Exception:
            return float(default)

    @staticmethod
    def _env_bool(name: str, default: bool) -> bool:
        raw = str(os.getenv(name, "")).strip().lower()
        if raw in {"1", "true", "yes", "on"}:
            return True
        if raw in {"0", "false", "no", "off"}:
            return False
        return bool(default)

    @staticmethod
    def _env_float(name: str, default: float, *, minimum: float, maximum: float) -> float:
        raw = str(os.getenv(name, "")).strip()
        try:
            value = float(raw) if raw else float(default)
        except Exception:
            value = float(default)
        return max(minimum, min(value, maximum))

    @staticmethod
    def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
        raw = str(os.getenv(name, "")).strip()
        try:
            value = int(raw) if raw else int(default)
        except Exception:
            value = int(default)
        return max(minimum, min(value, maximum))

    def _clear_cancel_request_locked(self, goal_id: str) -> None:
        self._cancel_requests.pop(goal_id, None)

    @staticmethod
    def _parse_goal_status(raw: Any) -> GoalStatus:
        text = str(raw or "").strip().lower()
        try:
            return GoalStatus(text)
        except Exception:
            return GoalStatus.PENDING

    @staticmethod
    def _parse_step_status(raw: Any) -> StepStatus:
        text = str(raw or "").strip().lower()
        try:
            return StepStatus(text)
        except Exception:
            return StepStatus.PENDING

    def _goal_to_dict(self, goal: GoalRecord) -> Dict[str, Any]:
        request = goal.request
        payload: Dict[str, Any] = {
            "goal_id": goal.goal_id,
            "request": {
                "text": str(request.text or ""),
                "source": str(request.source or "user"),
                "metadata": dict(request.metadata) if isinstance(request.metadata, dict) else {},
                "created_at": str(request.created_at or ""),
            },
            "status": goal.status.value if hasattr(goal.status, "value") else str(goal.status),
            "started_at": str(goal.started_at or ""),
            "completed_at": str(goal.completed_at or ""),
            "failure_reason": str(goal.failure_reason or ""),
            "results": [self._result_to_dict(item) for item in goal.results if isinstance(item, ActionResult)],
        }
        if isinstance(goal.plan, ExecutionPlan):
            payload["plan"] = self._plan_to_dict(goal.plan)
        return payload

    def _goal_from_dict(self, raw: Dict[str, Any]) -> GoalRecord:
        request_raw = raw.get("request", {}) if isinstance(raw.get("request"), dict) else {}
        request = GoalRequest(
            text=str(request_raw.get("text", "")),
            source=str(request_raw.get("source", "user") or "user"),
            metadata=dict(request_raw.get("metadata", {})) if isinstance(request_raw.get("metadata"), dict) else {},
            created_at=str(request_raw.get("created_at", "") or utc_now_iso()),
        )
        goal = GoalRecord(
            goal_id=str(raw.get("goal_id", "")),
            request=request,
            status=self._parse_goal_status(raw.get("status", GoalStatus.PENDING.value)),
        )
        goal.started_at = str(raw.get("started_at", "")).strip() or None
        goal.completed_at = str(raw.get("completed_at", "")).strip() or None
        goal.failure_reason = str(raw.get("failure_reason", "")).strip() or None
        plan_raw = raw.get("plan")
        if isinstance(plan_raw, dict):
            goal.plan = self._plan_from_dict(goal.goal_id, plan_raw)
        results_raw = raw.get("results", [])
        if isinstance(results_raw, list):
            goal.results = [self._result_from_dict(item) for item in results_raw if isinstance(item, dict)]
        return goal

    @staticmethod
    def _result_to_dict(result: ActionResult) -> Dict[str, Any]:
        return {
            "action": result.action,
            "status": result.status,
            "output": dict(result.output) if isinstance(result.output, dict) else {},
            "error": result.error,
            "evidence": dict(result.evidence) if isinstance(result.evidence, dict) else {},
            "duration_ms": int(result.duration_ms),
            "attempt": int(result.attempt),
            "completed_at": str(result.completed_at or ""),
        }

    @staticmethod
    def _result_from_dict(raw: Dict[str, Any]) -> ActionResult:
        status_text = str(raw.get("status", "failed")).strip().lower() or "failed"
        if status_text not in {"success", "failed", "blocked", "skipped"}:
            status_text = "failed"
        return ActionResult(
            action=str(raw.get("action", "")),
            status=status_text,  # type: ignore[arg-type]
            output=dict(raw.get("output", {})) if isinstance(raw.get("output"), dict) else {},
            error=(str(raw.get("error", "")).strip() or None),
            evidence=dict(raw.get("evidence", {})) if isinstance(raw.get("evidence"), dict) else {},
            duration_ms=int(raw.get("duration_ms", 0) or 0),
            attempt=max(1, int(raw.get("attempt", 1) or 1)),
            completed_at=str(raw.get("completed_at", "") or utc_now_iso()),
        )

    def _plan_to_dict(self, plan: ExecutionPlan) -> Dict[str, Any]:
        return {
            "plan_id": plan.plan_id,
            "goal_id": plan.goal_id,
            "intent": plan.intent,
            "context": dict(plan.context) if isinstance(plan.context, dict) else {},
            "created_at": str(plan.created_at or ""),
            "steps": [self._step_to_dict(step) for step in plan.steps if isinstance(step, PlanStep)],
        }

    def _plan_from_dict(self, goal_id: str, raw: Dict[str, Any]) -> ExecutionPlan:
        steps_raw = raw.get("steps", [])
        steps: List[PlanStep] = []
        if isinstance(steps_raw, list):
            for item in steps_raw:
                if isinstance(item, dict):
                    steps.append(self._step_from_dict(item))
        return ExecutionPlan(
            plan_id=str(raw.get("plan_id", "")),
            goal_id=str(raw.get("goal_id", "")).strip() or goal_id,
            intent=str(raw.get("intent", "")),
            steps=steps,
            context=dict(raw.get("context", {})) if isinstance(raw.get("context"), dict) else {},
            created_at=str(raw.get("created_at", "") or utc_now_iso()),
        )

    def _step_to_dict(self, step: PlanStep) -> Dict[str, Any]:
        return {
            "step_id": step.step_id,
            "action": step.action,
            "args": dict(step.args) if isinstance(step.args, dict) else {},
            "depends_on": list(step.depends_on) if isinstance(step.depends_on, list) else [],
            "verify": dict(step.verify) if isinstance(step.verify, dict) else {},
            "status": step.status.value if hasattr(step.status, "value") else str(step.status),
            "can_retry": bool(step.can_retry),
            "max_retries": int(step.max_retries),
            "timeout_s": int(step.timeout_s),
        }

    def _step_from_dict(self, raw: Dict[str, Any]) -> PlanStep:
        return PlanStep(
            step_id=str(raw.get("step_id", "")),
            action=str(raw.get("action", "")),
            args=dict(raw.get("args", {})) if isinstance(raw.get("args"), dict) else {},
            depends_on=[str(item) for item in raw.get("depends_on", []) if str(item).strip()] if isinstance(raw.get("depends_on"), list) else [],
            verify=dict(raw.get("verify", {})) if isinstance(raw.get("verify"), dict) else {},
            status=self._parse_step_status(raw.get("status", StepStatus.PENDING.value)),
            can_retry=bool(raw.get("can_retry", True)),
            max_retries=max(0, int(raw.get("max_retries", 2) or 2)),
            timeout_s=max(1, int(raw.get("timeout_s", 30) or 30)),
        )

    def _trim_locked(self) -> None:
        if len(self._goals) <= self._max_records:
            return
        over = len(self._goals) - self._max_records
        if over <= 0:
            return
        goals_with_keys: List[Tuple[str, str]] = []
        for goal_id, goal in self._goals.items():
            completed_key = str(goal.completed_at or "")
            created_key = str(goal.request.created_at or "")
            rank = f"{completed_key}|{created_key}|{goal_id}"
            goals_with_keys.append((goal_id, rank))
        goals_with_keys.sort(key=lambda item: item[1])
        removable_ids: List[str] = []
        for goal_id, _ in goals_with_keys:
            goal = self._goals.get(goal_id)
            if goal is None:
                continue
            if goal.status not in self.TERMINAL_STATUSES:
                continue
            removable_ids.append(goal_id)
            if len(removable_ids) >= over:
                break
        for goal_id in removable_ids:
            self._goals.pop(goal_id, None)
            self._cancel_requests.pop(goal_id, None)
            self._queued_ids.discard(goal_id)
            self._terminal_events.pop(goal_id, None)
        if removable_ids:
            self._queue_order = [goal_id for goal_id in self._queue_order if goal_id not in set(removable_ids)]

    def _load_locked(self) -> None:
        if not self._store_path.exists():
            return
        try:
            payload = json.loads(self._store_path.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(payload, dict):
            return

        goals_raw = payload.get("goals", [])
        if isinstance(goals_raw, list):
            for item in goals_raw:
                if not isinstance(item, dict):
                    continue
                goal = self._goal_from_dict(item)
                if not goal.goal_id:
                    continue
                self._goals[goal.goal_id] = goal
                event = Event()
                if goal.status in self.TERMINAL_STATUSES:
                    event.set()
                self._terminal_events[goal.goal_id] = event

        cancel_raw = payload.get("cancel_requests", {})
        if isinstance(cancel_raw, dict):
            for goal_id, reason in cancel_raw.items():
                clean_id = str(goal_id or "").strip()
                clean_reason = str(reason or "").strip()
                if clean_id and clean_reason and clean_id in self._goals:
                    self._cancel_requests[clean_id] = clean_reason

        recovered_running = 0
        for goal in self._goals.values():
            if goal.status == GoalStatus.RUNNING:
                goal.status = GoalStatus.PENDING
                goal.completed_at = None
                metadata = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
                metadata["recovered_from_status"] = GoalStatus.RUNNING.value
                metadata["recovered_at"] = utc_now_iso()
                goal.request.metadata = metadata
                recovered_running += 1
        self._recovered_running_count = recovered_running

        queue_raw = payload.get("queue", [])
        if isinstance(queue_raw, list):
            for item in queue_raw:
                goal_id = str(item or "").strip()
                goal = self._goals.get(goal_id)
                if not goal:
                    continue
                if goal.status in self.TERMINAL_STATUSES:
                    continue
                if goal_id in self._queued_ids:
                    continue
                self._queued_ids.add(goal_id)
                self._queue_order.append(goal_id)

        pending_goals = [
            goal
            for goal in self._goals.values()
            if goal.status not in self.TERMINAL_STATUSES
        ]
        pending_goals.sort(key=lambda row: str(row.request.created_at or ""))
        for goal in pending_goals:
            goal_id = str(goal.goal_id or "").strip()
            if not goal_id or goal_id in self._queued_ids:
                continue
            self._queued_ids.add(goal_id)
            self._queue_order.append(goal_id)

        for goal_id in self._queue_order:
            self._queue.put_nowait(goal_id)
        self._recovered_queue_count = len(self._queue_order)

    def _persist_locked(self) -> None:
        try:
            self._trim_locked()
            goals_serialized = [self._goal_to_dict(goal) for goal in self._goals.values()]
            payload = {
                "version": 1,
                "updated_at": utc_now_iso(),
                "goals": goals_serialized,
                "cancel_requests": dict(self._cancel_requests),
                "queue": list(self._queue_order),
            }
            self._store_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self._store_path.with_suffix(f"{self._store_path.suffix}.tmp")
            tmp_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
            tmp_path.replace(self._store_path)
        except Exception:
            return
