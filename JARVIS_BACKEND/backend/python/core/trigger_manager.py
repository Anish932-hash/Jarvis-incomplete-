from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List, Optional, Tuple


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
class TriggerRecord:
    trigger_id: str
    text: str
    source: str
    metadata: Dict[str, Any]
    interval_s: int
    next_run_at: str
    status: str = "active"
    run_count: int = 0
    last_goal_id: str = ""
    last_fired_at: str = ""
    last_error: str = ""
    created_at: str = field(default_factory=lambda: _iso(_utc_now()))
    updated_at: str = field(default_factory=lambda: _iso(_utc_now()))

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class TriggerManager:
    TERMINAL_STATUSES = {"cancelled"}
    ACTIVE_STATUSES = {"active"}

    def __init__(self, store_path: str = "data/triggers.json", max_records: int = 2000) -> None:
        self.store_path = Path(store_path)
        self.max_records = max(200, int(max_records))
        self._records: Dict[str, TriggerRecord] = {}
        self._lock = RLock()
        self._load()

    def create(
        self,
        *,
        text: str,
        interval_s: int,
        start_at: str = "",
        source: str = "desktop-ui",
        metadata: Dict[str, Any] | None = None,
    ) -> TriggerRecord:
        clean_text = str(text or "").strip()
        if not clean_text:
            raise ValueError("text is required")

        clamped_interval = self._clamp_interval(interval_s)
        start_dt = _parse_iso(start_at)
        now = _utc_now()
        if start_dt is None:
            start_dt = now + timedelta(seconds=clamped_interval)
        elif start_dt <= now:
            start_dt = now

        with self._lock:
            now_iso = _iso(now)
            record = TriggerRecord(
                trigger_id=str(uuid.uuid4()),
                text=clean_text,
                source=str(source or "desktop-ui"),
                metadata=dict(metadata or {}),
                interval_s=clamped_interval,
                next_run_at=_iso(start_dt),
                status="active",
                run_count=0,
                created_at=now_iso,
                updated_at=now_iso,
            )
            self._records[record.trigger_id] = record
            self._trim_locked()
            self._save_locked()
            return record

    def get(self, trigger_id: str) -> Optional[TriggerRecord]:
        with self._lock:
            return self._records.get(trigger_id)

    def list(self, *, status: str | None = None, limit: int = 200) -> List[Dict[str, Any]]:
        with self._lock:
            rows = list(self._records.values())
            if status:
                rows = [item for item in rows if item.status == status]
            rows.sort(key=lambda item: item.created_at, reverse=True)
            bounded = rows[: max(1, min(int(limit), 1000))]
            return [item.to_dict() for item in bounded]

    def cancel(self, trigger_id: str) -> Tuple[bool, str, Optional[TriggerRecord]]:
        with self._lock:
            record = self._records.get(trigger_id)
            if not record:
                return (False, "Trigger not found.", None)
            if record.status in self.TERMINAL_STATUSES:
                return (False, f"Trigger already {record.status}.", record)
            record.status = "cancelled"
            record.updated_at = _iso(_utc_now())
            self._save_locked()
            return (True, "Trigger cancelled.", record)

    def pause(self, trigger_id: str) -> Tuple[bool, str, Optional[TriggerRecord]]:
        with self._lock:
            record = self._records.get(trigger_id)
            if not record:
                return (False, "Trigger not found.", None)
            if record.status in self.TERMINAL_STATUSES:
                return (False, f"Cannot pause trigger in terminal state '{record.status}'.", record)
            if record.status == "paused":
                return (True, "Trigger already paused.", record)
            record.status = "paused"
            record.updated_at = _iso(_utc_now())
            self._save_locked()
            return (True, "Trigger paused.", record)

    def resume(self, trigger_id: str) -> Tuple[bool, str, Optional[TriggerRecord]]:
        with self._lock:
            record = self._records.get(trigger_id)
            if not record:
                return (False, "Trigger not found.", None)
            if record.status in self.TERMINAL_STATUSES:
                return (False, f"Cannot resume trigger in terminal state '{record.status}'.", record)
            if record.status != "paused":
                return (False, "Trigger is not paused.", record)

            now = _utc_now()
            next_dt = _parse_iso(record.next_run_at)
            if next_dt is None or next_dt <= now:
                record.next_run_at = _iso(now + timedelta(seconds=max(5, min(record.interval_s, 60))))
            record.status = "active"
            record.updated_at = _iso(now)
            self._save_locked()
            return (True, "Trigger resumed.", record)

    def run_now(self, trigger_id: str) -> Tuple[bool, str, Optional[TriggerRecord]]:
        with self._lock:
            record = self._records.get(trigger_id)
            if not record:
                return (False, "Trigger not found.", None)
            if record.status in self.TERMINAL_STATUSES:
                return (False, "Cancelled trigger cannot run.", record)
            record.status = "active"
            record.next_run_at = _iso(_utc_now())
            record.updated_at = _iso(_utc_now())
            self._save_locked()
            return (True, "Trigger queued to run now.", record)

    def due(self, *, now: datetime | None = None, limit: int = 20) -> List[TriggerRecord]:
        current = now or _utc_now()
        with self._lock:
            rows: List[TriggerRecord] = []
            for item in self._records.values():
                if item.status not in self.ACTIVE_STATUSES:
                    continue
                next_dt = _parse_iso(item.next_run_at) or _utc_now()
                if next_dt <= current:
                    rows.append(item)
            rows.sort(key=lambda item: item.next_run_at)
            return rows[: max(1, min(int(limit), 500))]

    def mark_dispatched(self, trigger_id: str, goal_id: str) -> Optional[TriggerRecord]:
        with self._lock:
            record = self._records.get(trigger_id)
            if not record:
                return None
            now = _utc_now()
            now_iso = _iso(now)
            record.run_count = max(0, int(record.run_count)) + 1
            record.last_goal_id = str(goal_id or "")
            record.last_fired_at = now_iso
            record.last_error = ""
            record.next_run_at = _iso(now + timedelta(seconds=max(5, int(record.interval_s))))
            record.updated_at = now_iso
            self._save_locked()
            return record

    def mark_dispatch_failed(self, trigger_id: str, error: str) -> Optional[TriggerRecord]:
        with self._lock:
            record = self._records.get(trigger_id)
            if not record:
                return None
            now = _utc_now()
            record.last_error = str(error or "").strip()
            record.next_run_at = _iso(now + timedelta(seconds=max(5, int(record.interval_s))))
            record.updated_at = _iso(now)
            self._save_locked()
            return record

    def active_count(self) -> int:
        with self._lock:
            return sum(1 for item in self._records.values() if item.status in self.ACTIVE_STATUSES)

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
            for item in payload:
                record = self._coerce_record(item)
                if not record:
                    continue
                self._records[record.trigger_id] = record
            self._trim_locked()

    def _coerce_record(self, raw: Any) -> Optional[TriggerRecord]:
        if not isinstance(raw, dict):
            return None
        trigger_id = str(raw.get("trigger_id", "")).strip()
        text = str(raw.get("text", "")).strip()
        source = str(raw.get("source", "desktop-ui")).strip() or "desktop-ui"
        interval_s = self._clamp_interval(raw.get("interval_s", 300))
        next_run_at = str(raw.get("next_run_at", "")).strip()
        if not trigger_id or not text or not _parse_iso(next_run_at):
            return None

        metadata = raw.get("metadata", {})
        return TriggerRecord(
            trigger_id=trigger_id,
            text=text,
            source=source,
            metadata=metadata if isinstance(metadata, dict) else {},
            interval_s=interval_s,
            next_run_at=next_run_at,
            status=str(raw.get("status", "active")),
            run_count=max(0, int(raw.get("run_count", 0))),
            last_goal_id=str(raw.get("last_goal_id", "")),
            last_fired_at=str(raw.get("last_fired_at", "")),
            last_error=str(raw.get("last_error", "")),
            created_at=str(raw.get("created_at", _iso(_utc_now()))),
            updated_at=str(raw.get("updated_at", _iso(_utc_now()))),
        )

    @staticmethod
    def _clamp_interval(value: Any) -> int:
        try:
            interval = int(value)
        except Exception:  # noqa: BLE001
            interval = 300
        return max(5, min(interval, 86_400))

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
            self._records.pop(item.trigger_id, None)
