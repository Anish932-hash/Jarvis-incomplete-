import asyncio
from datetime import datetime, timezone
from typing import Callable, Optional

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


class TimeTools:
    """Time conversion, scheduling, timers, alarms, and async delays."""

    @staticmethod
    def now(tz: str = "UTC") -> datetime:
        if ZoneInfo is None:
            return datetime.now(timezone.utc)
        normalized = (tz or "UTC").strip()
        if normalized.upper() in {"UTC", "GMT"}:
            return datetime.now(timezone.utc)
        try:
            return datetime.now(ZoneInfo(normalized))
        except Exception:
            return datetime.now(timezone.utc)

    @staticmethod
    def convert_timezone(dt: datetime, tz: str) -> datetime:
        if ZoneInfo is None:
            return dt.astimezone(timezone.utc)
        normalized = (tz or "UTC").strip()
        if normalized.upper() in {"UTC", "GMT"}:
            return dt.astimezone(timezone.utc)
        try:
            return dt.astimezone(ZoneInfo(normalized))
        except Exception:
            return dt.astimezone(timezone.utc)

    @staticmethod
    async def schedule(callback: Callable[[], None], run_at: datetime) -> None:
        now = datetime.now(run_at.tzinfo or timezone.utc)
        delay = max(0.0, (run_at - now).total_seconds())
        await asyncio.sleep(delay)
        callback()

    @staticmethod
    async def repeat(callback: Callable[[], None], interval: int) -> None:
        while True:
            await asyncio.sleep(interval)
            callback()

    @staticmethod
    async def countdown(seconds: int, tick: Optional[Callable[[int], None]] = None) -> None:
        for i in range(seconds, 0, -1):
            if tick:
                tick(i)
            await asyncio.sleep(1)
