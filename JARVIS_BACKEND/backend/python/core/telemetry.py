from __future__ import annotations

import json
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Optional

from backend.python.utils.logger import Logger


class Telemetry:
    """
    Structured telemetry publisher with:
    - bounded in-memory event log
    - optional durable JSONL persistence
    - lightweight runtime analytics helpers
    """

    def __init__(self, *, max_events: int = 2000) -> None:
        self.log = Logger("Telemetry").get_logger()
        bounded = max(1, min(int(max_events), 100_000))
        self._events: Deque[Dict[str, Any]] = deque(maxlen=bounded)
        self._lock = threading.Lock()
        self._counter = 0

        self._persist_enabled = self._env_flag("JARVIS_TELEMETRY_PERSIST", default=False)
        self._store_path = Path(os.getenv("JARVIS_TELEMETRY_STORE", "data/telemetry.jsonl"))
        self._persist_batch_size = max(1, min(int(os.getenv("JARVIS_TELEMETRY_PERSIST_BATCH", "20")), 5000))
        self._persist_interval_s = max(0.1, min(float(os.getenv("JARVIS_TELEMETRY_PERSIST_INTERVAL_S", "2.0")), 120.0))
        self._pending_persist: list[Dict[str, Any]] = []
        self._last_persist_monotonic = time.monotonic()

    def emit(self, event: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        event_name = str(event or "").strip() or "unknown"
        normalized_payload = payload if isinstance(payload, dict) else {"value": payload}
        now = time.time()
        record: Dict[str, Any] = {
            "event_id": 0,
            "event": event_name,
            "timestamp": now,
            "created_at": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
            "payload": normalized_payload,
        }
        with self._lock:
            self._counter += 1
            record["event_id"] = self._counter
            self._events.append(record)
            if self._persist_enabled:
                self._pending_persist.append(record)

        if self._persist_enabled:
            self._maybe_flush_persist(force=False)
        self.log.info(f"[event={event_name}] {normalized_payload}")
        return record

    def list_events(
        self,
        *,
        event: Optional[str] = None,
        after_id: int = 0,
        limit: int = 200,
    ) -> Dict[str, Any]:
        bounded_limit = max(1, min(int(limit), 1000))
        normalized_event = str(event or "").strip().lower()
        normalized_after = max(0, int(after_id))

        with self._lock:
            events = list(self._events)
            latest_event_id = int(self._counter)

        if normalized_event:
            events = [item for item in events if str(item.get("event", "")).lower() == normalized_event]

        if normalized_after > 0:
            events = [item for item in events if int(item.get("event_id", 0)) > normalized_after]

        items = events[-bounded_limit:]
        return {
            "items": items,
            "count": len(items),
            "latest_event_id": latest_event_id,
        }

    def summary(
        self,
        *,
        after_id: int = 0,
        limit: int = 1000,
    ) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 50_000))
        normalized_after = max(0, int(after_id))

        with self._lock:
            rows = list(self._events)
            latest_event_id = int(self._counter)
            pending_count = len(self._pending_persist)
        if normalized_after > 0:
            rows = [item for item in rows if int(item.get("event_id", 0)) > normalized_after]
        rows = rows[-bounded:]

        by_event: Dict[str, int] = {}
        failure_events = 0
        first_ts = 0.0
        last_ts = 0.0
        for row in rows:
            event_name = str(row.get("event", "")).strip() or "unknown"
            by_event[event_name] = by_event.get(event_name, 0) + 1

            payload = row.get("payload", {})
            status_value = ""
            if isinstance(payload, dict):
                status_value = str(payload.get("status", "")).strip().lower()
            lowered_event = event_name.lower()
            if status_value in {"failed", "blocked", "error"} or lowered_event.endswith(".failed") or lowered_event.endswith(".error"):
                failure_events += 1

            timestamp = float(row.get("timestamp", 0.0) or 0.0)
            if timestamp > 0:
                if first_ts <= 0:
                    first_ts = timestamp
                last_ts = timestamp

        duration_s = max(0.0, last_ts - first_ts) if first_ts > 0 and last_ts > 0 else 0.0
        events_per_s = (float(len(rows)) / duration_s) if duration_s > 0 else 0.0

        top_events = sorted(by_event.items(), key=lambda item: (-int(item[1]), item[0]))[:20]
        return {
            "status": "success",
            "count": len(rows),
            "latest_event_id": latest_event_id,
            "failure_events": int(failure_events),
            "failure_ratio": round((float(failure_events) / max(1.0, float(len(rows)))), 6),
            "duration_s": round(duration_s, 6),
            "events_per_s": round(events_per_s, 6),
            "top_events": [{"event": name, "count": count} for name, count in top_events],
            "persist": {
                "enabled": bool(self._persist_enabled),
                "path": str(self._store_path),
                "pending": pending_count,
            },
        }

    def flush(self) -> Dict[str, Any]:
        written = self._maybe_flush_persist(force=True)
        return {
            "status": "success",
            "persist_enabled": bool(self._persist_enabled),
            "written": int(written),
            "path": str(self._store_path),
        }

    def _maybe_flush_persist(self, *, force: bool) -> int:
        if not self._persist_enabled:
            return 0

        now_mono = time.monotonic()
        with self._lock:
            if not self._pending_persist:
                return 0
            if not force:
                if len(self._pending_persist) < self._persist_batch_size and (now_mono - self._last_persist_monotonic) < self._persist_interval_s:
                    return 0
            batch = self._pending_persist
            self._pending_persist = []
            self._last_persist_monotonic = now_mono

        try:
            self._store_path.parent.mkdir(parents=True, exist_ok=True)
            with self._store_path.open("a", encoding="utf-8") as handle:
                for item in batch:
                    handle.write(json.dumps(item, ensure_ascii=True))
                    handle.write("\n")
            return len(batch)
        except Exception as exc:  # noqa: BLE001
            with self._lock:
                self._pending_persist = batch + self._pending_persist
            self.log.warning(f"Telemetry persistence flush failed: {exc}")
            return 0

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
