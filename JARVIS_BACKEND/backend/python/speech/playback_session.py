from __future__ import annotations

import threading
import time
from typing import Any, Callable, Dict, Optional


class PlaybackSessionRegistry:
    """
    Tracks active playback sessions (currently used for TTS) and provides
    provider-agnostic interruption semantics.
    """

    _lock = threading.RLock()
    _sessions: Dict[str, Dict[str, Any]] = {}
    _active_by_channel: Dict[str, str] = {}

    @classmethod
    def start(
        cls,
        *,
        channel: str,
        provider: str,
        metadata: Optional[Dict[str, Any]] = None,
        stop_callback: Optional[Callable[[], None]] = None,
    ) -> Dict[str, Any]:
        clean_channel = str(channel or "tts").strip().lower() or "tts"
        clean_provider = str(provider or "unknown").strip().lower() or "unknown"
        now = time.time()
        session_id = f"{clean_channel}-{time.time_ns()}"
        row: Dict[str, Any] = {
            "session_id": session_id,
            "channel": clean_channel,
            "provider": clean_provider,
            "status": "active",
            "started_at": now,
            "ended_at": None,
            "interrupted": False,
            "message": "",
            "metadata": dict(metadata or {}),
            "_stop_callback": stop_callback,
        }
        with cls._lock:
            cls._sessions[session_id] = row
            cls._active_by_channel[clean_channel] = session_id
        return cls._sanitize(row)

    @classmethod
    def finish(cls, session_id: str, *, status: str = "completed", message: str = "") -> Dict[str, Any]:
        clean_session = str(session_id or "").strip()
        with cls._lock:
            row = cls._sessions.get(clean_session)
            if not isinstance(row, dict):
                return {"status": "missing", "session_id": clean_session}
            if row.get("ended_at") is None:
                row["ended_at"] = time.time()
            row["status"] = str(status or "completed").strip().lower() or "completed"
            row["message"] = str(message or "").strip()
            clean_channel = str(row.get("channel", "")).strip().lower()
            if clean_channel and cls._active_by_channel.get(clean_channel) == clean_session:
                cls._active_by_channel.pop(clean_channel, None)
            return cls._sanitize(row)

    @classmethod
    def active(cls, *, channel: str = "tts") -> Dict[str, Any]:
        clean_channel = str(channel or "tts").strip().lower() or "tts"
        with cls._lock:
            session_id = cls._active_by_channel.get(clean_channel, "")
            row = cls._sessions.get(session_id) if session_id else None
            if not isinstance(row, dict):
                return {"status": "missing", "channel": clean_channel, "active": None}
            return {"status": "success", "channel": clean_channel, "active": cls._sanitize(row)}

    @classmethod
    def interrupt(
        cls,
        *,
        session_id: str = "",
        channel: str = "tts",
        reason: str = "manual",
    ) -> Dict[str, Any]:
        clean_session = str(session_id or "").strip()
        clean_channel = str(channel or "tts").strip().lower() or "tts"
        clean_reason = str(reason or "manual").strip()
        callback: Optional[Callable[[], None]] = None
        with cls._lock:
            target = clean_session
            if not target:
                target = str(cls._active_by_channel.get(clean_channel, "")).strip()
            row = cls._sessions.get(target) if target else None
            if not isinstance(row, dict):
                return {
                    "status": "success",
                    "stopped": False,
                    "reason": clean_reason,
                    "channel": clean_channel,
                    "message": "No active playback session.",
                }
            row["interrupted"] = True
            row["message"] = clean_reason
            callback = row.get("_stop_callback") if callable(row.get("_stop_callback")) else None
            target_session = str(row.get("session_id", ""))
        callback_error = ""
        if callback is not None:
            try:
                callback()
            except Exception as exc:  # noqa: BLE001
                callback_error = str(exc)
        final = cls.finish(
            target_session,
            status="interrupted" if not callback_error else "error",
            message=clean_reason if not callback_error else callback_error,
        )
        return {
            "status": "success" if not callback_error else "error",
            "stopped": not bool(callback_error),
            "reason": clean_reason,
            "channel": clean_channel,
            "session": final,
            "message": callback_error,
        }

    @classmethod
    def _sanitize(cls, row: Dict[str, Any]) -> Dict[str, Any]:
        payload = {k: v for k, v in row.items() if not str(k).startswith("_")}
        started = float(payload.get("started_at", 0.0) or 0.0)
        ended_raw = payload.get("ended_at")
        ended = float(ended_raw) if isinstance(ended_raw, (int, float)) else 0.0
        if ended > 0.0 and started > 0.0:
            payload["duration_s"] = round(max(0.0, ended - started), 3)
        else:
            payload["duration_s"] = 0.0
        return payload
