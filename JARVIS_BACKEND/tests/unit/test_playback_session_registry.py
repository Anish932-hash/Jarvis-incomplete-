from __future__ import annotations

from backend.python.speech.playback_session import PlaybackSessionRegistry


def test_playback_registry_interrupts_active_session() -> None:
    called = {"count": 0}

    def _stop_callback() -> None:
        called["count"] += 1

    session = PlaybackSessionRegistry.start(
        channel="tts",
        provider="unit-test",
        metadata={"voice": "test"},
        stop_callback=_stop_callback,
    )
    session_id = str(session.get("session_id", ""))
    assert session_id

    interrupted = PlaybackSessionRegistry.interrupt(session_id=session_id, channel="tts", reason="unit-test-stop")

    assert interrupted.get("status") == "success"
    assert interrupted.get("stopped") is True
    assert called["count"] == 1
    payload = interrupted.get("session") if isinstance(interrupted.get("session"), dict) else {}
    assert payload.get("session_id") == session_id
    assert payload.get("status") == "interrupted"


def test_playback_registry_returns_no_active_when_empty() -> None:
    result = PlaybackSessionRegistry.interrupt(session_id="non-existent-session", channel="tts", reason="none")

    assert result.get("status") == "success"
    assert result.get("stopped") is False
