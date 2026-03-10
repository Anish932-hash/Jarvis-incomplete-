from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List, Optional, Tuple


SCHEDULE_METADATA_KEY = "__jarvis_schedule_id"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _parse_iso(value: str) -> Optional[datetime]:
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
class ScheduleRecord:
    schedule_id: str
    text: str
    source: str
    metadata: Dict[str, Any]
    run_at: str
    next_run_at: str
    status: str = "pending"
    max_attempts: int = 3
    retry_delay_s: int = 60
    repeat_interval_s: int = 0
    attempt_count: int = 0
    run_count: int = 0
    last_run_at: str = ""
    last_goal_id: str = ""
    last_error: str = ""
    checkpoint: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: _iso(_utc_now()))
    updated_at: str = field(default_factory=lambda: _iso(_utc_now()))

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ScheduleManager:
    TERMINAL_STATUSES = {"completed", "failed", "cancelled"}
    DISPATCHABLE_STATUSES = {"pending", "retry_wait"}

    def __init__(self, store_path: str = "data/schedules.json", max_records: int = 2000) -> None:
        self.store_path = Path(store_path)
        self.max_records = max(200, int(max_records))
        self._records: Dict[str, ScheduleRecord] = {}
        self._lock = RLock()
        self._load()

    def create(
        self,
        *,
        text: str,
        run_at: str,
        source: str = "desktop-ui",
        metadata: Dict[str, Any] | None = None,
        max_attempts: int = 3,
        retry_delay_s: int = 60,
        repeat_interval_s: int = 0,
    ) -> ScheduleRecord:
        run_dt = _parse_iso(run_at)
        if run_dt is None:
            raise ValueError("run_at must be a valid ISO-8601 datetime string.")

        safe_text = str(text or "").strip()
        if not safe_text:
            raise ValueError("text is required")

        with self._lock:
            now = _iso(_utc_now())
            record = ScheduleRecord(
                schedule_id=str(uuid.uuid4()),
                text=safe_text,
                source=str(source or "desktop-ui"),
                metadata=dict(metadata or {}),
                run_at=_iso(run_dt),
                next_run_at=_iso(run_dt),
                status="pending",
                max_attempts=max(1, min(int(max_attempts), 20)),
                retry_delay_s=max(5, min(int(retry_delay_s), 86_400)),
                repeat_interval_s=self._clamp_repeat_interval(repeat_interval_s),
                attempt_count=0,
                run_count=0,
                last_run_at="",
                checkpoint={"created_note": "Scheduled by user/API."},
                created_at=now,
                updated_at=now,
            )
            self._records[record.schedule_id] = record
            self._trim_locked()
            self._save_locked()
            return record

    def get(self, schedule_id: str) -> Optional[ScheduleRecord]:
        with self._lock:
            return self._records.get(schedule_id)

    def list(self, *, status: str | None = None, limit: int = 200) -> List[Dict[str, Any]]:
        with self._lock:
            rows = list(self._records.values())
            if status:
                rows = [item for item in rows if item.status == status]
            rows.sort(key=lambda item: item.created_at, reverse=True)
            bounded = rows[: max(1, min(int(limit), 1000))]
            return [item.to_dict() for item in bounded]

    def cancel(self, schedule_id: str) -> Tuple[bool, str, Optional[ScheduleRecord]]:
        with self._lock:
            record = self._records.get(schedule_id)
            if not record:
                return (False, "Schedule not found.", None)
            if record.status in self.TERMINAL_STATUSES:
                return (False, f"Schedule already {record.status}.", record)
            record.status = "cancelled"
            record.updated_at = _iso(_utc_now())
            record.checkpoint["cancelled_at"] = record.updated_at
            self._save_locked()
            return (True, "Schedule cancelled.", record)

    def pause(self, schedule_id: str) -> Tuple[bool, str, Optional[ScheduleRecord]]:
        with self._lock:
            record = self._records.get(schedule_id)
            if not record:
                return (False, "Schedule not found.", None)
            if record.status in self.TERMINAL_STATUSES:
                return (False, f"Cannot pause schedule in terminal state '{record.status}'.", record)
            if record.status in {"running", "dispatched"}:
                return (False, "Schedule is currently running and cannot be paused.", record)
            if record.status == "paused":
                return (True, "Schedule already paused.", record)

            now_iso = _iso(_utc_now())
            record.status = "paused"
            record.updated_at = now_iso
            record.checkpoint["paused_at"] = now_iso
            self._save_locked()
            return (True, "Schedule paused.", record)

    def resume(self, schedule_id: str) -> Tuple[bool, str, Optional[ScheduleRecord]]:
        with self._lock:
            record = self._records.get(schedule_id)
            if not record:
                return (False, "Schedule not found.", None)
            if record.status in self.TERMINAL_STATUSES:
                return (False, f"Cannot resume schedule in terminal state '{record.status}'.", record)
            if record.status != "paused":
                return (False, "Schedule is not paused.", record)

            now = _utc_now()
            now_iso = _iso(now)
            next_dt = _parse_iso(record.next_run_at)
            if next_dt is None or next_dt <= now:
                record.next_run_at = now_iso
            record.status = "pending"
            record.updated_at = now_iso
            record.checkpoint["resumed_at"] = now_iso
            self._save_locked()
            return (True, "Schedule resumed.", record)

    def run_now(self, schedule_id: str) -> Tuple[bool, str, Optional[ScheduleRecord]]:
        with self._lock:
            record = self._records.get(schedule_id)
            if not record:
                return (False, "Schedule not found.", None)
            if record.status == "cancelled":
                return (False, "Cancelled schedules cannot be run again.", record)
            if record.status in {"running", "dispatched"}:
                return (False, "Schedule is already running.", record)

            now_iso = _iso(_utc_now())
            record.status = "pending"
            record.next_run_at = now_iso
            record.attempt_count = 0
            record.updated_at = now_iso
            record.checkpoint["manual_run_now_at"] = now_iso
            self._save_locked()
            return (True, "Schedule queued to run immediately.", record)

    def pending_count(self) -> int:
        with self._lock:
            return sum(1 for item in self._records.values() if item.status in self.DISPATCHABLE_STATUSES)

    def due(self, *, now: datetime | None = None, limit: int = 50) -> List[ScheduleRecord]:
        current = now or _utc_now()
        with self._lock:
            candidates: List[ScheduleRecord] = []
            for item in self._records.values():
                if item.status not in self.DISPATCHABLE_STATUSES:
                    continue
                next_dt = _parse_iso(item.next_run_at) or _utc_now()
                if next_dt <= current:
                    candidates.append(item)
            candidates.sort(key=lambda item: item.next_run_at)
            return candidates[: max(1, min(int(limit), 500))]

    def mark_dispatched(self, schedule_id: str, goal_id: str) -> Optional[ScheduleRecord]:
        with self._lock:
            record = self._records.get(schedule_id)
            if not record:
                return None
            record.attempt_count = max(0, int(record.attempt_count)) + 1
            record.run_count = max(0, int(record.run_count)) + 1
            record.status = "dispatched"
            record.last_goal_id = str(goal_id or "")
            now_iso = _iso(_utc_now())
            record.last_run_at = now_iso
            record.updated_at = now_iso
            record.checkpoint["last_dispatched_at"] = now_iso
            record.checkpoint["attempt_count"] = record.attempt_count
            record.checkpoint["run_count"] = record.run_count
            self._save_locked()
            return record

    def mark_goal_result(self, schedule_id: str, *, goal_id: str, goal_status: str, failure_reason: str = "") -> Optional[ScheduleRecord]:
        with self._lock:
            record = self._records.get(schedule_id)
            if not record:
                return None
            if record.last_goal_id and goal_id and record.last_goal_id != goal_id:
                return record

            now = _utc_now()
            now_iso = _iso(now)
            status = str(goal_status or "").strip().lower()
            reason = str(failure_reason or "").strip()

            record.checkpoint["last_goal_status"] = status
            record.checkpoint["last_goal_id"] = goal_id
            record.updated_at = now_iso

            if status == "completed":
                record.last_error = ""
                record.checkpoint["last_completed_at"] = now_iso
                if record.repeat_interval_s > 0:
                    self._queue_next_cycle_locked(record, now=now, reason="completed")
                else:
                    record.status = "completed"
                    record.attempt_count = 0
            elif status == "cancelled":
                record.status = "cancelled"
                record.last_error = reason
                record.checkpoint["cancelled_at"] = now_iso
                record.checkpoint["last_error"] = reason
            elif status in {"failed", "blocked"}:
                record.last_error = reason
                record.checkpoint["last_failed_at"] = now_iso
                record.checkpoint["last_error"] = reason
                if record.attempt_count < record.max_attempts:
                    record.status = "retry_wait"
                    record.next_run_at = _iso(now + timedelta(seconds=max(5, int(record.retry_delay_s))))
                else:
                    if record.repeat_interval_s > 0:
                        self._queue_next_cycle_locked(record, now=now, reason="failed")
                    else:
                        record.status = "failed"
            else:
                record.status = "running"

            self._save_locked()
            return record

    def _load(self) -> None:
        with self._lock:
            if not self.store_path.exists():
                return
            try:
                payload = json.loads(self.store_path.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                return
            if not isinstance(payload, list):
                return

            now_iso = _iso(_utc_now())
            for item in payload:
                record = self._coerce_record(item)
                if not record:
                    continue
                if record.status in {"running", "dispatched"}:
                    # Resume safely after restart by putting incomplete items back to pending.
                    record.status = "pending"
                    record.next_run_at = now_iso
                    record.checkpoint["resume_note"] = "Recovered after service restart."
                    record.updated_at = now_iso
                self._records[record.schedule_id] = record
            self._trim_locked()

    def _coerce_record(self, raw: Any) -> Optional[ScheduleRecord]:
        if not isinstance(raw, dict):
            return None
        schedule_id = str(raw.get("schedule_id", "")).strip()
        text = str(raw.get("text", "")).strip()
        source = str(raw.get("source", "desktop-ui")).strip() or "desktop-ui"
        run_at = str(raw.get("run_at", "")).strip()
        next_run_at = str(raw.get("next_run_at", "")).strip() or run_at
        if not schedule_id or not text or not _parse_iso(run_at):
            return None
        if not _parse_iso(next_run_at):
            next_run_at = run_at

        metadata = raw.get("metadata", {})
        checkpoint = raw.get("checkpoint", {})
        return ScheduleRecord(
            schedule_id=schedule_id,
            text=text,
            source=source,
            metadata=metadata if isinstance(metadata, dict) else {},
            run_at=run_at,
            next_run_at=next_run_at,
            status=str(raw.get("status", "pending")),
            max_attempts=max(1, min(int(raw.get("max_attempts", 3)), 20)),
            retry_delay_s=max(5, min(int(raw.get("retry_delay_s", 60)), 86_400)),
            repeat_interval_s=self._clamp_repeat_interval(raw.get("repeat_interval_s", 0)),
            attempt_count=max(0, int(raw.get("attempt_count", 0))),
            run_count=max(0, int(raw.get("run_count", 0))),
            last_run_at=str(raw.get("last_run_at", "")),
            last_goal_id=str(raw.get("last_goal_id", "")),
            last_error=str(raw.get("last_error", "")),
            checkpoint=checkpoint if isinstance(checkpoint, dict) else {},
            created_at=str(raw.get("created_at", _iso(_utc_now()))),
            updated_at=str(raw.get("updated_at", _iso(_utc_now()))),
        )

    @staticmethod
    def _clamp_repeat_interval(value: Any) -> int:
        try:
            interval = int(value)
        except Exception:  # noqa: BLE001
            return 0
        if interval <= 0:
            return 0
        return max(5, min(interval, 2_592_000))

    @staticmethod
    def _queue_next_cycle_locked(record: ScheduleRecord, *, now: datetime, reason: str) -> None:
        next_run = _iso(now + timedelta(seconds=max(5, int(record.repeat_interval_s))))
        record.status = "pending"
        record.next_run_at = next_run
        record.attempt_count = 0
        record.checkpoint["cycle_reset_reason"] = reason
        record.checkpoint["next_cycle_at"] = next_run

    def _save_locked(self) -> None:
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        rows = [item.to_dict() for item in self._records.values()]
        rows.sort(key=lambda item: item.get("created_at", ""), reverse=True)
        self.store_path.write_text(json.dumps(rows, ensure_ascii=True, indent=2), encoding="utf-8")

    def _trim_locked(self) -> None:
        if len(self._records) <= self.max_records:
            return
        rows = sorted(self._records.values(), key=lambda item: item.created_at)
        overflow = len(rows) - self.max_records
        for item in rows[:overflow]:
            self._records.pop(item.schedule_id, None)
