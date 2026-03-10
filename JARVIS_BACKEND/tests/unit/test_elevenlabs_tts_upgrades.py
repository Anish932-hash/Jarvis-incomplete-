from __future__ import annotations

from typing import Any

from backend.python.speech.elevenlabs_tts import ElevenLabsTTS


class _FakeResponse:
    def __init__(self, *, status_code: int, chunks: list[bytes] | None = None) -> None:
        self.status_code = status_code
        self._chunks = list(chunks or [])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
        del exc_type, exc, tb
        return False

    def raise_for_status(self) -> None:
        if int(self.status_code) >= 400:
            raise RuntimeError(f"http {self.status_code}")

    def iter_content(self, chunk_size: int = 4096):  # noqa: ARG002
        for chunk in self._chunks:
            yield chunk


def test_elevenlabs_tts_retries_transient_segment_failure(monkeypatch) -> None:
    # Reset shared class state.
    ElevenLabsTTS._failure_ema = 0.0  # noqa: SLF001
    ElevenLabsTTS._cooldown_until = 0.0  # noqa: SLF001
    ElevenLabsTTS._last_error = ""  # noqa: SLF001

    calls = {"count": 0}

    def _fake_post(*args, **kwargs):  # noqa: ANN001
        del args, kwargs
        calls["count"] += 1
        if calls["count"] == 1:
            return _FakeResponse(status_code=503)
        return _FakeResponse(status_code=200, chunks=[b"ID3\x00\x00", b"\x01\x02\x03"])

    monkeypatch.setattr("backend.python.speech.elevenlabs_tts.requests.post", _fake_post)
    monkeypatch.setattr(ElevenLabsTTS, "_play", lambda self, audio_bytes, cancel_event: None)

    tts = ElevenLabsTTS(api_key="test-key", voice_id="voice-1")
    payload = tts.speak("Hello from test.")

    assert payload["status"] == "success"
    assert int(calls["count"]) >= 2
    assert payload.get("segments_spoken") == 1


def test_elevenlabs_tts_split_text_segments_handles_long_text() -> None:
    text = "First sentence is short. " + ("This one is intentionally longer to trigger splitting. " * 12)
    rows = ElevenLabsTTS._split_text_segments(text, max_chars=120)  # noqa: SLF001

    assert len(rows) >= 2
    assert all(len(segment) <= 140 for segment in rows)


def test_elevenlabs_tts_output_format_fallback_from_400(monkeypatch) -> None:
    ElevenLabsTTS._failure_ema = 0.0  # noqa: SLF001
    ElevenLabsTTS._cooldown_until = 0.0  # noqa: SLF001
    ElevenLabsTTS._last_error = ""  # noqa: SLF001

    seen_formats: list[str] = []

    def _fake_post(*args, **kwargs):  # noqa: ANN001
        del args
        payload = kwargs.get("json", {})
        seen_formats.append(str(payload.get("output_format", "")))
        if len(seen_formats) == 1:
            return _FakeResponse(status_code=400)
        return _FakeResponse(status_code=200, chunks=[b"ID3", b"\x01\x02"])

    monkeypatch.setenv("JARVIS_ELEVENLABS_OUTPUT_FORMAT", "pcm_16000")
    monkeypatch.setattr("backend.python.speech.elevenlabs_tts.requests.post", _fake_post)
    monkeypatch.setattr(ElevenLabsTTS, "_play", lambda self, audio_bytes, cancel_event: None)

    payload = ElevenLabsTTS(api_key="test-key", voice_id="voice-1").speak("Fallback check.")
    assert payload["status"] == "success"
    assert len(seen_formats) >= 2
    assert seen_formats[0] == "pcm_16000"
    assert "mp3_44100_128" in seen_formats


def test_elevenlabs_tts_diagnostics_exposes_segment_metrics() -> None:
    ElevenLabsTTS._segment_latency_ema = 0.0  # noqa: SLF001
    ElevenLabsTTS._segment_bytes_ema = 0.0  # noqa: SLF001
    ElevenLabsTTS._session_history = []  # noqa: SLF001
    ElevenLabsTTS._record_segment_metrics(status="success", latency_s=0.48, audio_bytes=4096)  # noqa: SLF001
    ElevenLabsTTS._record_session_result(  # noqa: SLF001
        session_id="sess-1",
        model_id="eleven_turbo_v2",
        status="success",
        segments=2,
        total_audio_bytes=8192,
        duration_s=1.2,
    )

    diagnostics = ElevenLabsTTS.diagnostics()
    assert diagnostics["status"] == "success"
    assert float(diagnostics.get("segment_latency_ema_s", 0.0)) > 0.0
    assert float(diagnostics.get("segment_audio_bytes_ema", 0.0)) > 0.0
    history_tail = diagnostics.get("history_tail", [])
    assert isinstance(history_tail, list)
    assert any(str(item.get("kind", "")) == "session" for item in history_tail if isinstance(item, dict))


def test_elevenlabs_tts_select_model_id_prefers_narration_for_long_text(monkeypatch) -> None:
    monkeypatch.setenv("JARVIS_ELEVENLABS_MODEL_DEFAULT", "eleven_turbo_v2")
    monkeypatch.setenv("JARVIS_ELEVENLABS_MODEL_NARRATION", "eleven_multilingual_v2")
    model = ElevenLabsTTS._select_model_id("word " * 110)  # noqa: SLF001
    assert model == "eleven_multilingual_v2"
